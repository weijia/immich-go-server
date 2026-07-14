// Package server 组装单节点运行实例：本地 Store + HMAC 鉴权 ClusterApi
// （含 blob 源）+ UDP 发现，并提供周期 Tick 钩子（由 main 接入 diskid/claim/coordinator）。
package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/discovery"
	"github.com/weijia/immich-go-server/internal/store"
)

// Config 节点运行配置。
type Config struct {
	NodeID   string
	Secret   string
	ListenAddr string // 如 "127.0.0.1:0"（0 表示随机端口）
	BlobRoot  string // 可选：作为 blob 源服务的本地目录
	DBPath    string
	MaxSkew  int64 // HMAC 时间窗，默认 300

	DiscoverAddr     string        // UDP 发现地址（如 "239.0.0.1:9999"），空则禁用
	DiscoverInterval time.Duration // 默认 5s
	TickInterval     time.Duration // 默认 15s；每次触发 OnTick
	OnTick           func(ctx context.Context, n *Node)
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
	n := &Node{cfg: cfg, store: st, api: h, reg: discovery.NewRegistry()}

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
	n.server = &http.Server{Handler: n.api.Mux()}
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

// Store 暴露本地仓储（供 OnTick / 测试使用）。
func (n *Node) Store() *store.Store { return n.store }

// Registry 暴露发现到的对等节点（供测试/调试）。
func (n *Node) Registry() *discovery.Registry { return n.reg }

// API 暴露 ClusterApi Handler（供测试直接调用 Mux）。
func (n *Node) API() *clusterapi.Handler { return n.api }

// Close 释放资源（停止 Run 后调用）。
func (n *Node) Close() error { return n.store.Close() }
