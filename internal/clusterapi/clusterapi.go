package clusterapi

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/weijia/immich-go-server/internal/crypto"
	"github.com/weijia/immich-go-server/internal/model"
)

// StatePayload 对应 §9.1 集群状态拉取响应；Signature 由 SignPayload 计算（§9.5）。
// SignedAt 为签名时刻的 epoch 秒，客户端据此重建签名并校验（§9.5 防篡改）。
type StatePayload struct {
	NodeID    string      `json:"nodeId"`
	Disks     []DiskState `json:"disks"`
	SignedAt  int64       `json:"signedAt,omitempty"`
	Signature string      `json:"signature,omitempty"`
}

// DiskState 状态 payload 中的单盘视图。
type DiskState struct {
	DiskSerial    string `json:"diskSerial"`
	Tier          string `json:"tier"`
	FreeBytes     int64  `json:"freeBytes"`
	MountedNodeID string `json:"mountedNodeId"`
	OnlineSeconds int64  `json:"onlineSeconds,omitempty"`
}

// Task 集群任务（§9.2 / §16.1），由 Coordinator 下发。
type Task struct {
	TaskID  string `json:"taskId"`
	Type    string `json:"type"` // MIGRATION | REPLICA
	DirKey  string `json:"dirKey,omitempty"`
	AssetID string `json:"assetId,omitempty"`
	SrcDisk string `json:"srcDisk,omitempty"`
	DstDisk string `json:"dstDisk,omitempty"`
	Status  string `json:"status,omitempty"` // QUEUED | RUNNING | DONE | FAILED
}

// DirectoryDTO 目录聚合元数据的线上传输结构（§9.x 目录跨节点重宿主）：
// 目标节点在本地无目录记录时从源节点拉取，领养为权威记录。
type DirectoryDTO struct {
	DirKey      string  `json:"dirKey"`
	NodeID      string  `json:"nodeId"`
	DiskSerial  string  `json:"diskSerial"`
	Tier        string  `json:"tier"`
	Temperature float64 `json:"temperature"`
	TotalBytes  int64   `json:"totalBytes"`
	AccessScore float64 `json:"accessScore"`
}

// ToModel 转为领域模型。
func (d DirectoryDTO) ToModel() model.Directory {
	return model.Directory{
		DirKey:      d.DirKey,
		NodeID:      d.NodeID,
		DiskSerial:  d.DiskSerial,
		Tier:        model.Tier(d.Tier),
		Temperature: d.Temperature,
		TotalBytes:  d.TotalBytes,
		AccessScore: d.AccessScore,
	}
}

// DirectoryFromModel 领域模型转为传输结构。
func DirectoryFromModel(d model.Directory) DirectoryDTO {
	return DirectoryDTO{
		DirKey:      d.DirKey,
		NodeID:      d.NodeID,
		DiskSerial:  d.DiskSerial,
		Tier:        string(d.Tier),
		Temperature: d.Temperature,
		TotalBytes:  d.TotalBytes,
		AccessScore: d.AccessScore,
	}
}

// StateProvider 集群 API 的后端数据来源；实现可插拔（内存 / SQLite）。
type StateProvider interface {
	GetState() StatePayload
	GetDiskLocation(diskSerial string) (string, bool) // 返回 mountedNodeID
	RegisterReplica(assetID, diskSerial, checksum string) error
	SubmitTask(task Task) error
	GetDirectory(dirKey string) (model.Directory, bool, error) // 目录重宿主：读本地目录元数据
	RelinquishDirectory(dirKey string) error                   // 目录重宿主：删除本地陈旧目录记录
	DeleteReplica(assetID, diskSerial string) error            // 真实源盘释放：删某 asset 在某盘上的副本记录
	ListAssetsByDir(dirKey string) ([]model.Asset, error)      // 真实源盘释放：列出目录资产以删字节
}

// BlobSource 提供 blob 字节流的本地来源（执行迁移时其他节点按需拉取，§9.1）。
// 仅持有数据的节点需要设置；无数据的节点可将 Handler.Source 置 nil。
type BlobSource interface {
	// StatBlob 返回 blob 总字节数与校验和（如无可填空串），不存在时 ok=false。
	StatBlob(assetID string) (size int64, checksum string, ok bool)
	// OpenBlob 从 offset 起返回只读字节流（用于 Range 续传）。
	OpenBlob(assetID string, offset int64) (io.ReadCloser, error)
}

// Handler 持有节点身份、共享密钥与后端 provider，注册带 HMAC 鉴权的路由。
type Handler struct {
	NodeID   string
	Secret   string
	MaxSkew  int64
	Now      func() int64
	Provider StateProvider
	Source   BlobSource // 可选：提供 blob 拉取（§9.1）
	BlobBase string     // 可选：本节点 blob 扁平根目录，用于源盘释放时删除物理字节

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
	m.HandleFunc("/api/cluster/directory/rehost", h.auth(h.handleRehostDirectory))
	m.HandleFunc("/api/cluster/directory/release", h.auth(h.handleReleaseSource))
	m.HandleFunc("/api/cluster/directory/", h.auth(h.handleGetDirectory))
	m.HandleFunc("/api/cluster/blob/", h.auth(h.handleBlob))
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
	// 计算 payload 自带签名（§9.5）：对不含 signature 的 body 做 SignPayload。
	// SignedAt 一并写入 body，供客户端重建签名（§9.5 防篡改）。
	p.SignedAt = h.Now()
	raw, _ := json.Marshal(p) // 此时 Signature 为空（omitempty），raw 不含 signature
	p.Signature = crypto.SignPayload(h.Secret, p.NodeID, p.SignedAt, raw)

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

// handleGetDirectory 经 HMAC 鉴权 GET 拉取本节点某目录的元数据（§9.x 目录重宿主）；
// 目标节点在本地无目录记录时从源节点拉取。404 表示本节点无此目录。
func (h *Handler) handleGetDirectory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dirKey := lastPathSeg(r.URL.Path, "/api/cluster/directory/")
	dir, ok, err := h.Provider.GetDirectory(dirKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "directory not found"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(DirectoryFromModel(dir))
}

// handleRehostDirectory 经 HMAC 鉴权 POST 处理目录重宿主的 relinquish 阶段（§9.x）：
// 仅当本节点正是被要求放弃的源节点时才删除本地陈旧的目录聚合记录（数据已迁走），
// 幂等且对称——同一请求发给两端，只有源节点会执行删除。
func (h *Handler) handleRehostDirectory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		DirKey         string `json:"dirKey"`
		RelinquishNode string `json:"relinquishNode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.DirKey == "" || req.RelinquishNode == "" {
		http.Error(w, "dirKey and relinquishNode required", http.StatusBadRequest)
		return
	}
	if req.RelinquishNode == h.NodeID {
		if err := h.Provider.RelinquishDirectory(req.DirKey); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

// handleReleaseSource 经 HMAC 鉴权 POST 处理“真实源盘释放”（§9.x）：
// 仅当本节点正是被要求释放的源节点时才执行——
//  1. 删除该目录下所有资产在 SrcDisk 上的副本记录；
//  2. 仅当 DstDisk 不在本节点（GetDiskLocation 未知或挂载他节点）时才删除物理字节，
//     因为同节点盘间迁移共享同一 BlobBase/<assetID> 文件，删字节会误伤目标副本。
// 幂等且对称——同一请求发给两端，只有源节点会执行。
func (h *Handler) handleReleaseSource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		DirKey      string `json:"dirKey"`
		SrcDisk     string `json:"srcDisk"`
		DstDisk     string `json:"dstDisk"`
		ReleaseNode string `json:"releaseNode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.DirKey == "" || req.SrcDisk == "" || req.DstDisk == "" || req.ReleaseNode == "" {
		http.Error(w, "dirKey, srcDisk, dstDisk, releaseNode required", http.StatusBadRequest)
		return
	}
	if req.ReleaseNode == h.NodeID {
		assets, err := h.Provider.ListAssetsByDir(req.DirKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, a := range assets {
			if err := h.Provider.DeleteReplica(a.AssetID, req.SrcDisk); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		if node, ok := h.Provider.GetDiskLocation(req.DstDisk); !ok || node != h.NodeID {
			for _, a := range assets {
				_ = os.Remove(filepath.Join(h.BlobBase, a.AssetID))
			}
		}
	}
	w.WriteHeader(http.StatusOK)
}

// handleBlob 提供 blob 字节流拉取（§9.1），支持 Range 续传（206）。
func (h *Handler) handleBlob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.Source == nil {
		http.Error(w, "blob source not configured", http.StatusNotImplemented)
		return
	}
	assetID := lastPathSeg(r.URL.Path, "/api/cluster/blob/")
	size, checksum, ok := h.Source.StatBlob(assetID)
	if !ok {
		http.Error(w, "blob not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	if checksum != "" {
		w.Header().Set("X-Blob-Checksum", checksum)
	}

	rng := r.Header.Get("Range")
	if rng == "" {
		rc, err := h.Source.OpenBlob(assetID, 0)
		if err != nil {
			http.Error(w, "open failed", http.StatusInternalServerError)
			return
		}
		defer rc.Close()
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		io.Copy(w, rc)
		return
	}

	start, end, ok := parseByteRange(rng, size)
	if !ok {
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(size, 10))
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	rc, err := h.Source.OpenBlob(assetID, start)
	if err != nil {
		http.Error(w, "open failed", http.StatusInternalServerError)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = io.CopyN(w, rc, end-start+1)
}

// parseByteRange 解析单个 "bytes=start-end" / "bytes=start-" / "bytes=-suffix" 范围（§9.1）。
// 返回 [start, end] 闭区间；越界或非法返回 ok=false。
func parseByteRange(header string, size int64) (start, end int64, ok bool) {
	const pfx = "bytes="
	if len(header) <= len(pfx) || header[:len(pfx)] != pfx {
		return 0, 0, false
	}
	spec := header[len(pfx):]
	dashIdx := -1
	for i := 0; i < len(spec); i++ {
		if spec[i] == '-' {
			dashIdx = i
			break
		}
	}
	if dashIdx < 0 {
		return 0, 0, false
	}
	left := spec[:dashIdx]
	right := spec[dashIdx+1:]

	if left == "" && right == "" {
		return 0, 0, false
	}
	if left == "" {
		// 后缀形式 bytes=-N
		n, err := strconv.ParseInt(right, 10, 64)
		if err != nil || n <= 0 || n > size {
			return 0, 0, false
		}
		return size - n, size - 1, true
	}
	s, err := strconv.ParseInt(left, 10, 64)
	if err != nil || s < 0 || s >= size {
		return 0, 0, false
	}
	start = s
	if right == "" {
		return start, size - 1, true
	}
	e, err := strconv.ParseInt(right, 10, 64)
	if err != nil || e < start || e >= size {
		return 0, 0, false
	}
	return start, e, true
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
