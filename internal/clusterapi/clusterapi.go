package clusterapi

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/weijia/immich-go-server/internal/crypto"
)

// StatePayload 对应 §9.1 集群状态拉取响应；Signature 由 SignPayload 计算（§9.5）。
type StatePayload struct {
	NodeID   string       `json:"nodeId"`
	Disks    []DiskState  `json:"disks"`
	Signature string      `json:"signature,omitempty"`
}

// DiskState 状态 payload 中的单盘视图。
type DiskState struct {
	DiskSerial    string `json:"diskSerial"`
	Tier          string `json:"tier"`
	FreeBytes     int64  `json:"freeBytes"`
	MountedNodeID string `json:"mountedNodeId"`
}

// Task 集群任务（§9.2 / §16.1），由 Coordinator 下发。
type Task struct {
	TaskID  string `json:"taskId"`
	Type    string `json:"type"` // MIGRATION | REPLICA
	DirKey  string `json:"dirKey,omitempty"`
	AssetID string `json:"assetId,omitempty"`
	SrcDisk string `json:"srcDisk,omitempty"`
	DstDisk string `json:"dstDisk,omitempty"`
}

// StateProvider 集群 API 的后端数据来源；实现可插拔（内存 / SQLite）。
type StateProvider interface {
	GetState() StatePayload
	GetDiskLocation(diskSerial string) (string, bool) // 返回 mountedNodeID
	RegisterReplica(assetID, diskSerial, checksum string) error
	SubmitTask(task Task) error
}

// Handler 持有节点身份、共享密钥与后端 provider，注册带 HMAC 鉴权的路由。
type Handler struct {
	NodeID   string
	Secret   string
	MaxSkew  int64
	Now      func() int64
	Provider StateProvider

	mu   sync.Mutex
	seen map[string]bool
}

// NewHandler 构造带鉴权的集群 API Handler。
func NewHandler(nodeID, secret string, maxSkew int64, p StateProvider) *Handler {
	return &Handler{
		NodeID:   nodeID,
		Secret:   secret,
		MaxSkew:  maxSkew,
		Now:      func() int64 { return timeNow() },
		Provider: p,
		seen:     map[string]bool{},
	}
}

// SignHeaders 为出向集群请求生成 HMAC 鉴权头（§9.5），供客户端与测试复用。
func SignHeaders(nodeID, secret, method, path string, body []byte, now int64) (http.Header, error) {
	nonce, err := crypto.GenerateNonce()
	if err != nil {
		return nil, err
	}
	nonceHex := hex.EncodeToString(nonce)
	sig := crypto.SignRequest(secret, method, path, now, []byte(nonceHex), body)
	h := http.Header{}
	h.Set("X-Cluster-NodeId", nodeID)
	h.Set("X-Cluster-Timestamp", strconv.FormatInt(now, 10))
	h.Set("X-Cluster-Nonce", nonceHex)
	h.Set("X-Cluster-Sig", sig)
	return h, nil
}

// Mux 返回带鉴权中间件的路由表。
func (h *Handler) Mux() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("/api/cluster/state", h.auth(h.handleState))
	m.HandleFunc("/api/cluster/disk/", h.auth(h.handleDiskLocation))
	m.HandleFunc("/api/cluster/replica/register", h.auth(h.handleRegisterReplica))
	m.HandleFunc("/api/cluster/task", h.auth(h.handleSubmitTask))
	return m
}

// auth HMAC 鉴权中间件：四道关（时间窗 → Nonce 防重放 → 重算常数时间比较），详见 §9.5。
func (h *Handler) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))

		ts, err := strconv.ParseInt(r.Header.Get("X-Cluster-Timestamp"), 10, 64)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		nonce := r.Header.Get("X-Cluster-Nonce")
		sig := r.Header.Get("X-Cluster-Sig")

		h.mu.Lock()
		ok := crypto.VerifyRequest(h.Secret, r.Method, r.URL.Path, ts,
			[]byte(nonce), body, sig, h.Now(), h.MaxSkew, h.seen)
		h.mu.Unlock()
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (h *Handler) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p := h.Provider.GetState()
	// 计算 payload 自带签名（§9.5）：对不含 signature 的 body 做 SignPayload
	raw, _ := json.Marshal(p)
	ts := h.Now()
	p.Signature = crypto.SignPayload(h.Secret, p.NodeID, ts, raw)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p)
}

func (h *Handler) handleDiskLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	serial := lastPathSeg(r.URL.Path, "/api/cluster/disk/")
	nodeID, ok := h.Provider.GetDiskLocation(serial)
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "disk not found"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"diskSerial": serial, "mountedNodeId": nodeID})
}

func (h *Handler) handleRegisterReplica(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		AssetID    string `json:"assetId"`
		DiskSerial string `json:"diskSerial"`
		Checksum   string `json:"checksum"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := h.Provider.RegisterReplica(req.AssetID, req.DiskSerial, req.Checksum); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var task Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := h.Provider.SubmitTask(task); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// timeNow 返回当前 epoch 秒（可被测试通过 Handler.Now 覆盖）。
func timeNow() int64 {
	return time.Now().Unix()
}

// lastPathSeg 取 "/api/cluster/disk/<serial>" 末段作为 diskSerial。
func lastPathSeg(path, prefix string) string {
	if len(path) <= len(prefix) {
		return ""
	}
	return path[len(prefix):]
}
