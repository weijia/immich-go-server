// Package server 组装单节点运行实例：本地 Store + HMAC 鉴权 ClusterApi
// （含 blob 源）+ UDP 发现，并提供周期 Tick 钩子（由 main 接入 diskid/claim/coordinator）。
package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/weijia/immich-go-server/internal/cluster"
	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/discovery"
	"github.com/weijia/immich-go-server/internal/model"
	"github.com/weijia/immich-go-server/internal/store"
	"github.com/weijia/immich-go-server/internal/worker"
)

// Config 节点运行配置。
type Config struct {
	NodeID   string
	Secret   string
	ListenAddr string // 如 "127.0.0.1:0"（0 表示随机端口）
	BlobRoot  string // 可选：作为 blob 源服务的本地目录
	DBPath    string
	MaxSkew  int64 // HMAC 时间窗，默认 300

	DiscoverAddr     string        // 节点间 UDP 发现地址（如 "239.0.0.1:9999"），空则禁用
	DiscoverInterval time.Duration // 默认 5s
	TickInterval     time.Duration // 默认 15s；每次触发 OnTick
	OnTick           func(ctx context.Context, n *Node)

	// 面向 Immich 客户端的发现与展示
	ServerName         string // 发现响应中的展示名，默认 "immich-go-server"
	ServerURL          string // 外部可达基址（如 http://192.168.1.5:8081），空则自动推导
	ClientDiscoverAddr string // 客户端 UDP 发现监听地址，默认 ":2284"，空则禁用
}

// Node 单节点运行实例。
type Node struct {
	cfg      Config
	store    *store.Store
	api      *clusterapi.Handler
	listener net.Listener
	server   *http.Server
	reg      *discovery.Registry
	bc       *discovery.Broadcaster
	lis      *discovery.Listener
	discConn discovery.PacketConn
	discDst  net.Addr

	identity         discovery.ClientIdentity
	clientResponder  *discovery.ClientResponder
}

// New 构造节点（打开 SQLite、装配 API 与发现）。
func New(cfg Config) (*Node, error) {
	if cfg.MaxSkew == 0 {
		cfg.MaxSkew = 300
	}
	if cfg.DiscoverInterval == 0 {
		cfg.DiscoverInterval = 5 * time.Second
	}
	if cfg.TickInterval == 0 {
		cfg.TickInterval = 15 * time.Second
	}
	st, err := store.NewStore(cfg.DBPath, cfg.NodeID)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	h := clusterapi.NewHandler(cfg.NodeID, cfg.Secret, cfg.MaxSkew, st)
	if cfg.BlobRoot != "" {
		h.Source = clusterapi.FileSystemBlobSource{Root: cfg.BlobRoot}
	}
	h.BlobRoot = cfg.BlobRoot // 回退：Provider.DiskRoot() 查不到时用此单根
	h.AssetStore = st         // 客户端媒体 API 后端（§media-api）
	n := &Node{cfg: cfg, store: st, api: h, reg: discovery.NewRegistry()}

	// 生成/复用服务器身份（serverId / serverToken / serverName），供客户端发现与认证引导。
	ident, err := discovery.GenerateServerIdentity(
		func(k string) (string, bool, error) { return st.GetServerConfig(k) },
		func(k, v string) error { return st.SetServerConfig(k, v) },
		cfg.ServerName,
	)
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("server identity: %w", err)
	}
	n.identity = ident
	h.ServerID = ident.ServerID
	h.ServerName = ident.ServerName
	h.ServerToken = ident.ServerToken

	if cfg.DiscoverAddr != "" {
		ua, err := net.ResolveUDPAddr("udp", cfg.DiscoverAddr)
		if err != nil {
			_ = st.Close()
			return nil, fmt.Errorf("resolve discover addr: %w", err)
		}
	uc, err := net.ListenUDP("udp", &net.UDPAddr{IP: ua.IP, Port: ua.Port})
		if err != nil {
			_ = st.Close()
			return nil, fmt.Errorf("listen udp: %w", err)
		}
		n.discConn = uc
		n.discDst = uc.LocalAddr()
		n.bc = discovery.NewBroadcaster(uc, cfg.Secret, cfg.NodeID, "")
		n.lis = discovery.NewListener(uc, cfg.Secret, cfg.MaxSkew, n.reg)
	}
	return n, nil
}

// Run 启动 HTTP 服务与（可选）发现/周期循环，阻塞直到 ctx 取消。
func (n *Node) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", n.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	n.listener = ln
	n.api.Now = func() int64 { return time.Now().Unix() }
	if n.bc != nil {
		n.bc.SetAddr(ln.Addr().String())
	}
	// 计算外部可达基址并启动面向客户端的 UDP 发现响应（§ discovery-protocol）。
	extURL := n.externalURL()
	n.api.ServerURL = extURL
	if n.cfg.ClientDiscoverAddr != "" {
		cr, cerr := discovery.NewClientResponder(n.cfg.ClientDiscoverAddr, n.identity, extURL)
		if cerr != nil {
			fmt.Printf("warn: client discovery disabled: %v\n", cerr)
		} else {
			n.clientResponder = cr
			go func() { _ = cr.Run(ctx) }()
		}
	}
	// HTTP 调试日志：设置 IMMICH_GO_DEBUG=1 记录每个请求的方法/路径/状态/耗时；
	// 设置 IMMICH_GO_DEBUG=2 额外 dump 请求体与响应体（仅用于排查“server is not reachable”等）。
	var handler http.Handler = n.api.Mux()
	if lvl := os.Getenv("IMMICH_GO_DEBUG"); lvl != "" {
		handler = httpDebugMiddleware(handler, lvl == "2")
		log.Printf("[HTTP-DEBUG] enabled (level=%s)", lvl)
	}
	n.server = &http.Server{Handler: handler}
	go func() { _ = n.server.Serve(ln) }()

	if n.discConn != nil {
		go n.loop(ctx, n.cfg.DiscoverInterval, func(context.Context) { _ = n.bc.Send(n.discDst) })
		go n.loop(ctx, n.cfg.DiscoverInterval, func(context.Context) { _, _ = n.lis.RecvOnce() })
	}
	if n.cfg.OnTick != nil {
		go n.loop(ctx, n.cfg.TickInterval, func(ctx context.Context) { n.cfg.OnTick(ctx, n) })
	}

	<-ctx.Done()
	_ = n.server.Close()
	if n.discConn != nil {
		_ = n.discConn.Close()
	}
	if n.clientResponder != nil {
		_ = n.clientResponder.Close()
	}
	return nil
}

func (n *Node) loop(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn(ctx)
		}
	}
}

// Addr 返回 HTTP 实际监听地址（Run 成功后可用）。
func (n *Node) Addr() string {
	if n.listener == nil {
		return ""
	}
	return n.listener.Addr().String()
}

// externalURL 返回客户端可达的基址（不含 /api），用于发现响应与 API 自述。
// 若显式配置了 ServerURL 则直接用；否则从监听地址推导，并把回环/未指定地址替换为
// 本机首个非回环 IPv4，确保局域网客户端能真正连上。
func (n *Node) externalURL() string {
	if n.cfg.ServerURL != "" {
		return strings.TrimRight(n.cfg.ServerURL, "/")
	}
	host := listenHost(n.cfg.ListenAddr)
	if isLoopbackOrUnspecified(host) {
		if ip := firstNonLoopbackIP(); ip != "" {
			host = ip
		}
	}
	port := listenPort(n.listener.Addr().String())
	return fmt.Sprintf("http://%s:%s", host, port)
}

func listenHost(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[:i]
	}
	return addr
}

func listenPort(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[i+1:]
	}
	return ""
}

func isLoopbackOrUnspecified(host string) bool {
	if host == "" || host == "0.0.0.0" || host == "::" || host == "localhost" {
		return true
	}
	return strings.HasPrefix(host, "127.") || strings.HasPrefix(host, "::1")
}

// firstNonLoopbackIP 返回本机一个可达 IPv4 地址，供客户端发现使用。
// 两遍扫描：第一遍优先返回 RFC1918 私有地址（局域网 Wi-Fi/以太网），并跳过
// Tailscale/CGNAT（100.64.0.0/10）等虚拟网卡——否则本机会把 tailscale0 的 100.x.x.x
// 当成服务器地址返回给客户端，而客户端（手机）往往未连接 Tailscale，导致连不上。
// 若没有任何私有地址（纯 VPN 环境），第二遍兜底返回任意非回环 IPv4。
func firstNonLoopbackIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	var fallback string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			v4 := ip.To4()
			if v4 == nil {
				continue
			}
			if fallback == "" {
				fallback = v4.String()
			}
			if isPrivateV4(v4) && !isCGNAT(v4) {
				return v4.String()
			}
		}
	}
	return fallback
}

// isPrivateV4 判断是否为 RFC1918 私有地址（局域网）。不含 Tailscale/CGNAT(100.64/10)。
func isPrivateV4(ip net.IP) bool {
	if v4 := ip.To4(); v4 != nil {
		return v4[0] == 10 ||
			(v4[0] == 172 && v4[1] >= 16 && v4[1] <= 31) ||
			(v4[0] == 192 && v4[1] == 168)
	}
	return false
}

// isCGNAT 判断是否为 100.64.0.0/10（Tailscale 等 Carrier-Grade NAT 虚拟地址）。
func isCGNAT(ip net.IP) bool {
	if v4 := ip.To4(); v4 != nil {
		return v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127
	}
	return false
}

// Store 暴露本地仓储（供 OnTick / 测试使用）。
func (n *Node) Store() *store.Store { return n.store }

// Registry 暴露发现到的对等节点（供测试/调试）。
func (n *Node) Registry() *discovery.Registry { return n.reg }

// API 暴露 ClusterApi Handler（供测试直接调用 Mux）。
func (n *Node) API() *clusterapi.Handler { return n.api }

// Federate 拉取各发现到的 peer 状态并聚合为全局视图（§9 / §10）。
// 本节点状态直接取自 Store，不经过 HTTP。返回的 GlobalView 可用于 GlobalRepository。
func (n *Node) Federate(ctx context.Context) (cluster.GlobalView, error) {
	peers := n.reg.Peers()
	cps := make([]cluster.Peer, 0, len(peers))
	for _, p := range peers {
		if p.NodeID == n.cfg.NodeID {
			continue
		}
		cps = append(cps, cluster.Peer{NodeID: p.NodeID, URL: "http://" + p.Addr})
	}
	fed := &cluster.Federation{
		SelfNodeID: n.cfg.NodeID,
		SelfState:  n.store.GetState(),
		Peers:      cps,
		Client:     cluster.NewClient(n.cfg.NodeID, n.cfg.Secret, n.cfg.MaxSkew),
	}
	gv, err := fed.Run(ctx)
	if err != nil {
		return gv, err
	}
	// 把跨节点聚合出的目录放置图持久化进本地库（§8.6 控制面）：
	// 单节点下线后其目录记录仍保留在本节点，Coordinator 可据此决策重宿主/再均衡。
	for _, d := range gv.Directories {
		if err := n.store.SaveDirectory(d); err != nil {
			return gv, err
		}
	}
	return gv, nil
}

// GlobalRepository 基于全局视图构造 coordinator.Repository：磁盘来自跨节点合并，
// 目录/资产/副本/任务下发仍走本节点 Store；SubmitTask 路由到 dst 所在节点。
func (n *Node) GlobalRepository(gv cluster.GlobalView) *cluster.GlobalRepo {
	client := cluster.NewClient(n.cfg.NodeID, n.cfg.Secret, n.cfg.MaxSkew)
	return &cluster.GlobalRepo{
		Disks:   gv.Disks,
		Local:   n.store,
		SelfID:  n.cfg.NodeID,
		PeerURL: n.peerURLFunc(),
		Client:  client,
	}
}

// peerURLFunc 由发现到的 registry 构造 nodeID→基址 解析器（仅在 peer 出现时可达）。
func (n *Node) peerURLFunc() func(nodeID string) (string, bool) {
	return func(nodeID string) (string, bool) {
		for _, p := range n.reg.Peers() {
			if p.NodeID == nodeID {
				return "http://" + p.Addr, true
			}
		}
		return "", false
	}
}

// NodeLocator 实现 worker.DiskLocator：磁盘挂载节点取自全局视图，回退本地 Store；
// 对端基址取自发现 registry。
type NodeLocator struct {
	SelfID    string
	Disks     map[string]model.Disk
	Store     *store.Store
	peerURLFn func(nodeID string) (string, bool)
}

func (l NodeLocator) DiskNode(serial string) (string, bool) {
	if d, ok := l.Disks[serial]; ok && d.MountedNodeID != "" {
		return d.MountedNodeID, true
	}
	return l.Store.GetDiskLocation(serial)
}

func (l NodeLocator) PeerURL(nodeID string) (string, bool) {
	if nodeID == l.SelfID {
		return "", false
	}
	return l.peerURLFn(nodeID)
}

// Worker 构造本节点的任务执行器：目标盘由全局视图解析，源可来自本节点或远端。
func (n *Node) Worker(gv cluster.GlobalView) *worker.Worker {
	return &worker.Worker{
		NodeID: n.cfg.NodeID,
		Secret: n.cfg.Secret,
		Repo:   n.store,
		Loc: NodeLocator{
			SelfID:    n.cfg.NodeID,
			Disks:     gv.Disks,
			Store:     n.store,
			peerURLFn: n.peerURLFunc(),
		},
		Client: cluster.NewClient(n.cfg.NodeID, n.cfg.Secret, n.cfg.MaxSkew),
	}
}

// Close 释放资源（停止 Run 后调用）。
func (n *Node) Close() error {
	if n.clientResponder != nil {
		_ = n.clientResponder.Close()
	}
	return n.store.Close()
}

// statusRecorder 包装 ResponseWriter 以便记录响应状态码与字节数。
type statusRecorder struct {
	http.ResponseWriter
	status int
	size   int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.size += n
	return n, err
}

// Hijack 转发到底层 ResponseWriter，使 websocket（engine.io）升级可用。
// 否则 gorilla/websocket 的 Upgrade 会因 ResponseWriter 不可 Hijack 而返回 500。
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := r.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, errors.New("statusRecorder: underlying ResponseWriter is not a Hijacker")
}

// httpDebugMiddleware 记录每个 HTTP 请求的 方法/路径/查询/状态/耗时；
// dumpBody=true 时额外把请求体与响应体（截断到 4KB）打印出来，便于排查连通性问题。
func httpDebugMiddleware(next http.Handler, dumpBody bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqBody []byte
		if dumpBody && r.Body != nil {
			reqBody, _ = io.ReadAll(io.LimitReader(r.Body, 64<<10))
			r.Body = io.NopCloser(bytes.NewReader(reqBody)) // 还原，供下游 handler 读取
		}

		rec := &statusRecorder{ResponseWriter: w, status: 0}
		var respBuf *bytes.Buffer
		if dumpBody {
			respBuf = &bytes.Buffer{}
			rec.ResponseWriter = &teeResponseWriter{ResponseWriter: w, buf: respBuf}
		}
		start := time.Now()
		next.ServeHTTP(rec, r)
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		elapsed := time.Since(start)

		q := r.URL.RawQuery
		if q != "" {
			q = "?" + q
		}
		log.Printf("[HTTP] %s %s%s -> %d (%dB, %s) from %s",
			r.Method, r.URL.Path, q, rec.status, rec.size, elapsed, r.RemoteAddr)

		if dumpBody {
			if len(reqBody) > 0 {
				log.Printf("[HTTP-BODY] >>> %s", truncate(reqBody, 4096))
			}
			if respBuf != nil && respBuf.Len() > 0 {
				log.Printf("[HTTP-BODY] <<< %s", truncate(respBuf.Bytes(), 4096))
			}
		}
	})
}

func truncate(b []byte, max int) string {
	s := string(b)
	if len(s) > max {
		return s[:max] + "...(truncated)"
	}
	return s
}

// teeResponseWriter 在把响应写回客户端的同时，把字节复制进 buf（用于调试 dump）。
type teeResponseWriter struct {
	http.ResponseWriter
	buf *bytes.Buffer
}

func (t *teeResponseWriter) Write(b []byte) (int, error) {
	t.buf.Write(b)
	return t.ResponseWriter.Write(b)
}

// Hijack 转发到底层 ResponseWriter，使 websocket（engine.io）升级可用。
func (t *teeResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := t.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, errors.New("teeResponseWriter: underlying ResponseWriter is not a Hijacker")
}
