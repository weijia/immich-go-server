package discovery

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// 面向 Immich 客户端的 UDP 发现响应（见 immich-android-server/docs/discovery-protocol.md）。
// 客户端广播 DISCOVER_IMMICH_SERVER，本响应器回 IMMICH_SERVER_RESPONSE:<JSON>。
const (
	ClientDiscoveryPort     = 2284
	ClientBroadcastAddress  = "255.255.255.255"
	DiscoverRequestV1       = "DISCOVER_IMMICH_SERVER"
	DiscoverRequestPrefixV3 = "DISCOVER_IMMICH_SERVER:"
	DiscoverResponsePrefix  = "IMMICH_SERVER_RESPONSE:"
)

// ClientIdentity 是发现响应所需的服务器身份。
type ClientIdentity struct {
	ServerID    string // UUID v4
	ServerName  string // 展示名
	ServerToken string // v3 HMAC 密钥（256-bit hex）
	Version     string // 如 "3.0.0"
}

// ClientResponder 监听 UDP（默认 2284），对客户端发现请求回 IMMICH_SERVER_RESPONSE。
type ClientResponder struct {
	identity  ClientIdentity
	serverURL string // 外部可达基址，须以 /api 结尾（由 ensureAPISuffix 保证）
	conn      *net.UDPConn
	now       func() int64
}

// NewClientResponder 在 addr（如 ":2284" 或 "0.0.0.0:2284"）上监听客户端发现请求。
func NewClientResponder(addr string, id ClientIdentity, serverURL string) (*ClientResponder, error) {
	uaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("resolve client discover addr: %w", err)
	}
	conn, err := net.ListenUDP("udp", uaddr)
	if err != nil {
		return nil, fmt.Errorf("listen client discover: %w", err)
	}
	return &ClientResponder{
		identity:  id,
		serverURL: ensureAPISuffix(serverURL),
		conn:      conn,
		now:       func() int64 { return time.Now().Unix() },
	}, nil
}

// Addr 返回实际监听地址（用于测试/调试）。
func (r *ClientResponder) Addr() net.Addr { return r.conn.LocalAddr() }

// Run 阻塞处理发现请求，直到 ctx 取消或 Close 触发读取错误。
func (r *ClientResponder) Run(ctx context.Context) error {
	buf := make([]byte, 1500)
	for {
		n, raddr, err := r.conn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		if resp := r.handle(buf[:n]); resp != "" {
			_, _ = r.conn.WriteTo([]byte(resp), raddr)
		}
	}
}

// Close 释放监听。
func (r *ClientResponder) Close() error { return r.conn.Close() }

// handle 解析请求并构造响应；未知请求返回空串（忽略）。
func (r *ClientResponder) handle(data []byte) string {
	req := strings.TrimSpace(string(data))
	switch {
	case req == DiscoverRequestV1:
		// v1.0 明文请求：回 v2 风格响应（含 serverId），与 android 端 createResponse 行为一致。
		return r.buildResponse(map[string]any{
			"serverId":   r.identity.ServerID,
			"serverName": r.identity.ServerName,
			"serverUrl":  r.serverURL,
			"version":    r.identity.Version,
			"timestamp":  r.now(),
		})
	case strings.HasPrefix(req, DiscoverRequestPrefixV3):
		parts := strings.SplitN(strings.TrimPrefix(req, DiscoverRequestPrefixV3), ":", 2)
		if len(parts) < 2 || parts[1] == "" {
			return ""
		}
		nonce := parts[1]
		ts := r.now()
		sig := signV3(r.identity.ServerToken, r.identity.ServerID, r.serverURL, ts, nonce)
		return r.buildResponse(map[string]any{
			"serverId":       r.identity.ServerID,
			"serverName":     r.identity.ServerName,
			"serverUrl":      r.serverURL,
			"version":        r.identity.Version,
			"timestamp":      ts,
			"challengeNonce": nonce,
			"signature":      sig,
		})
	default:
		return ""
	}
}

func (r *ClientResponder) buildResponse(m map[string]any) string {
	b, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return DiscoverResponsePrefix + string(b)
}

// signV3 计算 v3 响应签名：HMAC-SHA256(serverToken, serverId|serverUrl|timestamp|challengeNonce)。
func signV3(token, serverID, serverURL string, ts int64, nonce string) string {
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte(serverID))
	mac.Write([]byte("|"))
	mac.Write([]byte(serverURL))
	mac.Write([]byte("|"))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("|"))
	mac.Write([]byte(nonce))
	return hex.EncodeToString(mac.Sum(nil))
}

func ensureAPISuffix(url string) string {
	if strings.HasSuffix(url, "/api") {
		return url
	}
	return strings.TrimRight(url, "/") + "/api"
}

// GenerateServerIdentity 生成并持久化服务器身份（serverId UUID + serverToken）。
// 若 store 中已有则复用，保证重启后身份稳定（v2/v3 客户端据此重连）。
func GenerateServerIdentity(get func(k string) (string, bool, error), set func(k, v string) error, name string) (ClientIdentity, error) {
	id, ok, err := get("serverId")
	if err != nil {
		return ClientIdentity{}, err
	}
	if !ok {
		id = uuidV4()
		if err := set("serverId", id); err != nil {
			return ClientIdentity{}, err
		}
	}
	token, ok, err := get("serverToken")
	if err != nil {
		return ClientIdentity{}, err
	}
	if !ok {
		token = randomHex(32)
		if err := set("serverToken", token); err != nil {
			return ClientIdentity{}, err
		}
	}
	sname, ok, err := get("serverName")
	if err != nil {
		return ClientIdentity{}, err
	}
	if !ok {
		sname = name
		if sname == "" {
			sname = "immich-go-server"
		}
		if err := set("serverName", sname); err != nil {
			return ClientIdentity{}, err
		}
	}
	return ClientIdentity{ServerID: id, ServerName: sname, ServerToken: token, Version: "3.0.0"}, nil
}

func uuidV4() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
