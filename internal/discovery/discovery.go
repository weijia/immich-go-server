// Package discovery 实现集群节点发现（§9.3）：通过 UDP beacon 广播节点可达地址，
// beacon 携带 HMAC 签名与时间戳，接收端校验后写入对等节点注册表。
package discovery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/weijia/immich-go-server/internal/crypto"
)

// PacketConn 是 net.PacketConn 的最小子集，便于注入测试。
type PacketConn interface {
	ReadFrom(p []byte) (n int, addr net.Addr, err error)
	WriteTo(p []byte, addr net.Addr) (n int, err error)
	Close() error
}

// Beacon 是节点发现广播体；Sig 由 SignBeacon 计算（§9.3 / §9.5 同源 HMAC）。
type Beacon struct {
	NodeID    string `json:"nodeId"`
	Addr      string `json:"addr"` // 本节点可被访问的 host:port
	Timestamp int64  `json:"ts"`
	Nonce     string `json:"nonce"`
	Sig       string `json:"sig"`
}

// SignBeacon 计算 beacon 签名：HMAC(secret, nodeId|addr|ts|nonce)。
func SignBeacon(secret string, b Beacon) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(b.NodeID))
	mac.Write([]byte(b.Addr))
	mac.Write([]byte(strconv.FormatInt(b.Timestamp, 10)))
	mac.Write([]byte(b.Nonce))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyBeacon 校验 beacon：时间窗 + 签名（§9.5）。nonce 重放由调用方注册表去重。
func VerifyBeacon(secret string, b Beacon, now, maxSkew int64) bool {
	if now-b.Timestamp > maxSkew || b.Timestamp-now > maxSkew {
		return false
	}
	return hmac.Equal([]byte(SignBeacon(secret, b)), []byte(b.Sig))
}

// Peer 注册表中的一条对等节点记录。
type Peer struct {
	NodeID   string
	Addr     string
	LastSeen int64
}

// Registry 维护已知对等节点，线程安全。
type Registry struct {
	mu    sync.Mutex
	peers map[string]Peer // nodeID -> Peer
}

func NewRegistry() *Registry { return &Registry{peers: map[string]Peer{}} }

// Upsert 写入/更新一个节点；返回是否发生了变更（新的或地址变化）。
func (r *Registry) Upsert(nodeID, addr string, now int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	old, exists := r.peers[nodeID]
	if exists && old.Addr == addr {
		old.LastSeen = now
		r.peers[nodeID] = old
		return false
	}
	r.peers[nodeID] = Peer{NodeID: nodeID, Addr: addr, LastSeen: now}
	return true
}

// Prune 移除 LastSeen 早于 cutoff 的节点，返回移除数量。
func (r *Registry) Prune(cutoff int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for id, p := range r.peers {
		if p.LastSeen < cutoff {
			delete(r.peers, id)
			n++
		}
	}
	return n
}

// Peers 返回当前所有已知节点快照。
func (r *Registry) Peers() []Peer {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Peer, 0, len(r.peers))
	for _, p := range r.peers {
		out = append(out, p)
	}
	return out
}

// Broadcaster 周期性发送带签名的 beacon（§9.3）。
type Broadcaster struct {
	conn     PacketConn
	secret   string
	nodeID   string
	addr     string
	now      func() int64
}

func NewBroadcaster(conn PacketConn, secret, nodeID, addr string) *Broadcaster {
	return &Broadcaster{conn: conn, secret: secret, nodeID: nodeID, addr: addr, now: func() int64 { return time.Now().Unix() }}
}

// Build 构造一个已签名、带新鲜 nonce 的 beacon。
func (b *Broadcaster) Build() (Beacon, error) {
	nonce, err := crypto.GenerateNonce()
	if err != nil {
		return Beacon{}, err
	}
	beacon := Beacon{
		NodeID:    b.nodeID,
		Addr:      b.addr,
		Timestamp: b.now(),
		Nonce:     hex.EncodeToString(nonce),
	}
	beacon.Sig = SignBeacon(b.secret, beacon)
	return beacon, nil
}

// Send 广播一次 beacon（封送到目标地址）。
func (b *Broadcaster) Send(dst net.Addr) error {
	beacon, err := b.Build()
	if err != nil {
		return err
	}
	data, err := json.Marshal(beacon)
	if err != nil {
		return err
	}
	_, err = b.conn.WriteTo(data, dst)
	return err
}

// Listener 接收并校验 beacon，写入注册表。
type Listener struct {
	conn     PacketConn
	secret   string
	maxSkew  int64
	reg      *Registry
	now      func() int64
	mu       sync.Mutex
	seen     map[string]bool // 近窗口 nonce 去重，防止重放
	seenCap  int
}

func NewListener(conn PacketConn, secret string, maxSkew int64, reg *Registry) *Listener {
	return &Listener{
		conn:    conn,
		secret:  secret,
		maxSkew: maxSkew,
		reg:     reg,
		now:     func() int64 { return time.Now().Unix() },
		seen:    map[string]bool{},
		seenCap: 1024,
	}
}

// HandlePacket 解析并校验一个 beacon 包；合法则写入注册表。返回是否接受。
func (l *Listener) HandlePacket(data []byte) (bool, error) {
	var b Beacon
	if err := json.Unmarshal(data, &b); err != nil {
		return false, err
	}
	now := l.now()
	if !VerifyBeacon(l.secret, b, now, l.maxSkew) {
		return false, nil
	}
	l.mu.Lock()
	if l.seen[b.Nonce] {
		l.mu.Unlock()
		return false, nil // 重放
	}
	l.seen[b.Nonce] = true
	if len(l.seen) > l.seenCap {
		l.seen = map[string]bool{}
	}
	l.mu.Unlock()

	l.reg.Upsert(b.NodeID, b.Addr, now)
	return true, nil
}

// RecvOnce 从连接读取一个包并交给 HandlePacket（用于测试与 Run 循环）。
func (l *Listener) RecvOnce() (bool, error) {
	buf := make([]byte, 1500)
	n, _, err := l.conn.ReadFrom(buf)
	if err != nil {
		return false, err
	}
	return l.HandlePacket(buf[:n])
}
