package clusterapi

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/weijia/immich-go-server/internal/crypto"
	"github.com/weijia/immich-go-server/internal/ingest"
	"github.com/weijia/immich-go-server/internal/model"
)

// StatePayload 对应 §9.1 集群状态拉取响应；Signature 由 SignPayload 计算（§9.5）。
// SignedAt 为签名时刻的 epoch 秒，客户端据此重建签名并校验（§9.5 防篡改）。
type StatePayload struct {
	NodeID     string          `json:"nodeId"`
	Node       *NodeInfo       `json:"node,omitempty"`
	Disks      []DiskState     `json:"disks"`
	Directories []DirectoryDTO `json:"directories"`
	SignedAt   int64           `json:"signedAt,omitempty"`
	Signature  string          `json:"signature,omitempty"`
}

// NodeInfo 节点元信息，供 WebUI 看板展示（§dashboard）；不参与集群 HMAC 签名校验。
type NodeInfo struct {
	NodeID     string `json:"nodeId"`
	ServerName string `json:"serverName"`
	ServerURL  string `json:"serverUrl"`
	Version    string `json:"version"`
	StartedAt  int64  `json:"startedAt"`
	UptimeSec  int64  `json:"uptimeSec"`
}

// DiskState 状态 payload 中的单盘视图。
type DiskState struct {
	DiskSerial    string `json:"diskSerial"`
	Label         string `json:"label,omitempty"`
	Tier          string `json:"tier"`
	FreeBytes     int64  `json:"freeBytes"`
	CapacityBytes int64  `json:"capacityBytes,omitempty"`
	MountedNodeID string `json:"mountedNodeId"`
	OnlineSeconds int64  `json:"onlineSeconds,omitempty"`
	BlobRoot      string `json:"blobRoot,omitempty"`
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

// DirectoryDTO 目录聚合元数据的线上传输结构（§8.6 控制面放置图）：
// 随 /state 上报，对端拉取后按 lastEvalAt 做 LWW 合并。
type DirectoryDTO struct {
	DirKey      string  `json:"dirKey"`
	NodeID      string  `json:"nodeId"`
	DiskSerial  string  `json:"diskSerial"`
	Tier        string  `json:"tier"`
	Temperature float64 `json:"temperature"`
	TotalBytes  int64   `json:"totalBytes"`
	AccessScore float64 `json:"accessScore"`
	LastEvalAt  int64   `json:"lastEvalAt,omitempty"`
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
		LastEvalAt:  d.LastEvalAt,
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
		LastEvalAt:  d.LastEvalAt,
	}
}

// StateProvider 集群 API 的后端数据来源；实现可插拔（内存 / SQLite）。
type StateProvider interface {
	GetState() StatePayload
	GetDiskLocation(diskSerial string) (string, bool) // 返回 mountedNodeID
	DiskRoot(diskSerial string) (string, bool)          // 返回磁盘物理仓库根（blob_root），§仓库即真相
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

// AssetBackend 客户端媒体 API 的后端数据来源（§media-api）。*Store 实现该接口。
type AssetBackend interface {
	// GetAsset 读取单个资产（物理定位用 dir_key / checksum）。
	GetAsset(assetID string) (model.Asset, bool, error)
	// GetAssetMeta 读取资产 API 元信息；不存在返回 ok=false。
	GetAssetMeta(assetID string) (model.AssetMeta, bool, error)
	// ListAssets 返回全部资产（时间线列表）。
	ListAssets() ([]model.Asset, error)
	// SaveUploadedAsset 记录一次上传产物（asset + 副本 + 目录 + 元信息）。
	SaveUploadedAsset(a model.Asset, diskSerial string, sizeBytes int64) error
	// SaveAssetMeta 写入/覆盖资产 API 元信息。
	SaveAssetMeta(m model.AssetMeta) error
	// SaveDeviceAsset 记录 (deviceId, deviceAssetId) -> assetId 映射（去重）。
	SaveDeviceAsset(deviceID, deviceAssetID, assetID string) error
	// LookupDeviceAssets 返回该设备下已存在的 {deviceAssetId: assetId}。
	LookupDeviceAssets(deviceID string, deviceAssetIDs []string) (map[string]string, error)
	// DeleteAsset 删除资产全部元数据，返回被删资产（用于清理物理字节）。
	DeleteAsset(assetID string) (model.Asset, bool, error)
	// ListMountedDisks 返回本节点已认领、非可疑磁盘（按 free_bytes 降序）。
	ListMountedDisks(nodeID string) ([]model.Disk, error)
	// ListReplicas 返回某资产的所有副本（删除时定位物理仓库根）。
	ListReplicas(assetID string) ([]model.Replica, error)
	// DiskRoot 返回磁盘物理仓库根（blob_root），未知返回 ok=false。
	DiskRoot(diskSerial string) (string, bool)
}

// AssetResponse 客户端资产响应（兼容 Immich AssetResponse 字段子集）。
type AssetResponse struct {
	ID             string `json:"id"`
	OwnerID        string `json:"ownerId"`
	DeviceAssetID  string `json:"deviceAssetId"`
	DeviceID       string `json:"deviceId"`
	Type           string `json:"type"`
	OriginalPath   string `json:"originalPath"`
	Thumbhash      string `json:"thumbhash,omitempty"`
	FileCreatedAt  string `json:"fileCreatedAt"`
	FileModifiedAt string `json:"fileModifiedAt"`
	CreatedAt      string `json:"createdAt"`
	UpdatedAt      string `json:"updatedAt"`
	IsFavorite     bool   `json:"isFavorite"`
	IsArchived     bool   `json:"isArchived"`
	IsTrashed      bool   `json:"isTrashed"`
	OriginalFileName string `json:"originalFileName"`
	MimeType       string `json:"mimeType"`
	FileSize       int64  `json:"fileSize"`
	Width          int    `json:"width,omitempty"`
	Height         int    `json:"height,omitempty"`
	Duration       string `json:"duration,omitempty"`
	Checksum       string `json:"checksum,omitempty"`
	LivePhotoVideoID string `json:"livePhotoVideoId,omitempty"`
}

// AssetUploadResponse 上传响应（兼容 Immich）。
type AssetUploadResponse struct {
	ID        string `json:"id"`
	Duplicate bool   `json:"duplicate"`
}

// BulkUploadCheckRequest / Response 客户端上传前批量查重。
type BulkUploadCheckRequest struct {
	DeviceAssetIDs []string `json:"deviceAssetIds"`
	DeviceID       string   `json:"deviceId"`
}
type BulkUploadCheckResponse struct {
	Results []AssetExistence `json:"results"`
}
type AssetExistence struct {
	ID            string `json:"id"`
	DeviceAssetID string `json:"deviceAssetId"`
	Exists        bool   `json:"exists"`
}

// Handler 持有节点身份、共享密钥与后端 provider，注册带 HMAC 鉴权的路由。
type Handler struct {
	NodeID   string
	Secret   string
	MaxSkew  int64
	Now      func() int64
	Provider StateProvider
	Source   BlobSource // 可选：提供 blob 拉取（§9.1），无 disk/dir 查询参数时作为回退
	BlobRoot string     // 回退：单根 blob 目录，仅在 Provider.DiskRoot() 查不到时使用

	// Immich 客户端 API（发现/认证引导）所需身份；ServerURL 由 server 在 Run 时填充。
	ServerID    string
	ServerName  string
	ServerToken string
	ServerURL   string // 外部可达基址（不含 /api，运行时填充）

	// AssetStore 客户端媒体 API 后端（§media-api）；为 nil 时资产路由返回 501。
	AssetStore AssetBackend

	// DashboardToken 供 WebUI（浏览器端看板）拉取节点信息的访问令牌。
	// 空则代表不启用 token 校验（不推荐）。
	DashboardToken string

	// Version / StartedAt 供 WebUI 看板展示节点元信息（§dashboard）。
	Version   string
	StartedAt int64

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

	// ---- Immich 客户端 API（发现/认证引导），不经集群 HMAC 鉴权 ----
	// 客户端 openapi 生成的 pingServer() 请求 /api/server/ping，
	// 这里同时注册新路径与遗留的 /api/server-info/ping（与 immich-android-server 保持一致）。
	m.HandleFunc("/api/server/ping", h.handlePing)
	m.HandleFunc("/api/server-info/ping", h.handlePing)
	m.HandleFunc("/api/server-info", h.handleServerInfo)
	m.HandleFunc("/api/server/version", h.handleServerVersion)
	m.HandleFunc("/api/server/features", h.handleServerFeatures)
	m.HandleFunc("/api/server/config", h.handleServerConfig)
	m.HandleFunc("/api/auth/login", h.handleLogin)
	m.HandleFunc("/api/auth/token-exchange", h.handleTokenExchange)
	m.HandleFunc("/api/auth/validateToken", h.handleValidateToken)
	m.HandleFunc("/api/users/me/preferences", h.handleGetMyPreferences)
	m.HandleFunc("/api/users/me", h.handleGetMe)

	// ---- Immich 客户端同步 API（登录后全量同步）----
	m.HandleFunc("/api/sync/ack", h.handleSyncAck)
	m.HandleFunc("/api/sync/stream", h.handleSyncStream)

	// ---- Immich 实时通道：socket.io (engine.io v4, websocket transport) ----
	// 客户端登录后通过 socket_io_client 连接，用于接收资产上传就绪等实时事件。
	// 这里实现最小 engine.io v4 握手，使客户端显示"已连接"并停止 404 噪声；
	// 暂不主动推送业务事件（不影响登录与相册浏览）。
	m.HandleFunc("/api/socket.io/", h.handleSocketIO)

	// ---- WebUI 看板信息端点（浏览器端跨域拉取，token 鉴权）----
	// 与 /api/cluster/state 返回相同节点状态，但用更轻量的 DashboardToken（启动生成、
	// 打印在日志）而非集群 HMAC 签名，便于看板从浏览器直接跨域读取。
	m.HandleFunc("/api/dashboard/state", h.dashboardAuth(h.handleState))

	// ---- Immich 客户端媒体 API（资产上传/列表/下载/删除），不经集群 HMAC 鉴权 ----
	m.HandleFunc("/api/assets/bulk-upload-check", h.handleBulkUploadCheck)
	m.HandleFunc("/api/assets", h.handleAssets) // GET 列表 / POST 上传
	m.HandleFunc("/api/assets/", h.handleAssetItem) // {id} / {id}/original / {id}/thumbnail
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

// dashboardAuth 是 WebUI 看板端点的轻量鉴权：用启动时生成/配置的 DashboardToken
// 做常量时间比较，避免与集群 HMAC（CLUSTER_SECRET）耦合。token 可从以下任一位置获取：
//   - Authorization: Bearer <token>
//   - x-api-key: <token>
//   - 查询参数 ?token=<token>
//
// 若未配置 DashboardToken（空），则放行（不推荐，仅供本地调试）。
func (h *Handler) dashboardAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.DashboardToken == "" {
			next(w, r)
			return
		}
		tok := dashboardTokenFromRequest(r)
		if tok == "" || subtle.ConstantTimeCompare([]byte(tok), []byte(h.DashboardToken)) != 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid dashboard token"})
			return
		}
		next(w, r)
	}
}

// dashboardTokenFromRequest 从请求中提取看板令牌。
func dashboardTokenFromRequest(r *http.Request) string {
	if ah := r.Header.Get("Authorization"); strings.HasPrefix(ah, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(ah, "Bearer "))
	}
	if k := r.Header.Get("x-api-key"); k != "" {
		return k
	}
	return r.URL.Query().Get("token")
}

// ---- Immich 客户端 API（发现/认证引导） ----

// handlePing 实现 Immich /api/server-info/ping（客户端连通性探测）。
func (h *Handler) handlePing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"res": "pong"})
}

// handleServerInfo 实现 Immich /api/server-info（版本探测）。
func (h *Handler) handleServerInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"version": "3.0.0",
		"build":   "immich-go-server",
		"isNew":   false,
	})
}

// handleServerVersion 实现 Immich /api/server/version：登录表单初始化时读取，
// 用于版本兼容性判定。必须与移动端 app 主版本一致（app 为 3.0.0），否则登录表单
// 报 "Your app major version is not compatible with the server!"。见 docs/requirements.md。
func (h *Handler) handleServerVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"major":      3,
		"minor":      0,
		"patch":      0,
		"prerelease": 0,
	})
}

// handleServerFeatures 实现 Immich /api/server/features：登录表单据此决定显示
// 密码登录 / OAuth 入口。passwordLogin 必须为 true，否则表单不显示密码框。
func (h *Handler) handleServerFeatures(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"configFile":          true,
		"duplicateDetection":  false,
		"email":               false,
		"facialRecognition":   false,
		"importFaces":         false,
		"map":                 true,
		"oauth":               false,
		"oauthAutoLaunch":     false,
		"ocr":                 false,
		"passwordLogin":       true,
		"realtimeTranscoding": false,
		"reverseGeocoding":    false,
		"search":              true,
		"sidecar":             false,
		"smartSearch":         false,
		"trash":               true,
	})
}

// handleServerConfig 实现 Immich /api/server/config：登录表单读取 OAuth 按钮文本等。
func (h *Handler) handleServerConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"externalDomain":   "",
		"isInitialized":    true,
		"isOnboarded":      true,
		"loginPageMessage": "",
		"maintenanceMode":  false,
		"mapDarkStyleUrl":  "https://tiles.immich.cloud/v1/style/dark.json",
		"mapLightStyleUrl": "https://tiles.immich.cloud/v1/style/light.json",
		"minFaces":         3,
		"oauthButtonText":  "",
		"publicUsers":      false,
		"trashDays":        30,
		"userDeleteDelay":  0,
	})
}

// handleLogin 实现 Immich /api/auth/login 的最小引导：返回 access token。
func (h *Handler) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"accessToken":        randomAccessToken(),
		"userId":             h.ServerID,
		"userEmail":          "admin@immich.local",
		"name":               h.ServerName,
		"isAdmin":            true,
		"isOnboarded":        true,
		"profileImagePath":   "",
		"shouldChangePassword": false,
	})
}

// handleValidateToken 实现 Immich /api/auth/validateToken：AuthGuard 在后台周期性
// 调用以确认 token 仍有效。返回 200 + {authStatus:true} 即可，避免后台抛 404 错误。
func (h *Handler) handleValidateToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"authStatus": true})
}

// handleTokenExchange 实现 v3 发现引导：返回 serverToken + serverId，
// 供客户端缓存后用于后续发现响应的签名校验（文档中为 HTTPS 首连交换）。
func (h *Handler) handleTokenExchange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"serverId":    h.ServerID,
		"serverToken": h.ServerToken,
		"expiresAt":   nil,
	})
}

// handleGetMe 实现 Immich /api/users/me：返回当前（admin）用户，
// 字段对齐 openapi UserAdminResponseDto 的全部必填项，供客户端 saveAuthInfo
// 在登录后解析/恢复会话（否则会触发客户端回退逻辑）。
func (h *Handler) handleGetMe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	now := time.Now().UTC().Format(time.RFC3339)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":                   h.ServerID,
		"email":                "admin@immich.local",
		"name":                 h.ServerName,
		"isAdmin":              true,
		"avatarColor":          "primary",
		"createdAt":            now,
		"updatedAt":            now,
		"profileChangedAt":     now,
		"deletedAt":            nil,
		"license":              nil,
		"oauthId":              "",
		"profileImagePath":     "",
		"quotaSizeInBytes":     nil,
		"quotaUsageInBytes":    nil,
		"shouldChangePassword": false,
		"status":               "active",
		"storageLabel":         nil,
	})
}

// handleGetMyPreferences 实现 Immich /api/users/me/preferences：返回默认用户偏好，
// 字段对齐 openapi UserPreferencesResponseDto 的全部必填嵌套对象。
// getMyUser() 会以 (.wait) 并行请求本接口与 /users/me，任一失败即导致会话建立失败。
func (h *Handler) handleGetMyPreferences(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"albums": map[string]any{
			"defaultAssetOrder": "asc",
		},
		"cast": map[string]any{
			"gCastEnabled": false,
		},
		"download": map[string]any{
			"archiveSize":          -1,
			"includeEmbeddedVideos": false,
		},
		"emailNotifications": map[string]any{
			"albumInvite": false,
			"albumUpdate": false,
			"enabled":     false,
		},
		"folders": map[string]any{
			"enabled":    false,
			"sidebarWeb": false,
		},
		"memories": map[string]any{
			"duration": 0,
			"enabled":  false,
		},
		"people": map[string]any{
			"enabled":    false,
			"sidebarWeb": false,
		},
		"purchase": map[string]any{
			"hideBuyButtonUntil": "",
			"showSupportBadge":   false,
		},
		"ratings": map[string]any{
			"enabled": false,
		},
		"sharedLinks": map[string]any{
			"enabled":    false,
			"sidebarWeb": false,
		},
		"tags": map[string]any{
			"enabled":    false,
			"sidebarWeb": false,
		},
	})
}

// ---- Immich 客户端同步 API（登录后全量同步）----

// handleSyncAck 处理 /api/sync/ack 的三种方法：
//   - DELETE：pre-sync 迁移任务里删除指定类型的回执（忽略 body，返回 204）
//   - POST：客户端分批确认已处理的同步事件（忽略 body，返回 204）
//   - GET：拉取当前会话的回执列表（返回空数组）
func (h *Handler) handleSyncAck(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete, http.MethodPost:
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]any{})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// syncRequestToResponse 将客户端在 /api/sync/stream 请求的同步类型（SyncRequestType）
// 映射到响应里使用的实体类型（SyncEntityType）。客户端按响应类型反序列化数据。
var syncRequestToResponse = map[string]string{
	"AuthUsersV1":         "AuthUserV1",
	"UsersV1":             "UserV1",
	"AssetsV1":            "AssetV1",
	"AssetsV2":            "AssetV2",
	"AssetExifsV1":        "AssetExifV1",
	"AssetEditsV1":        "AssetEditV1",
	"AssetMetadataV1":     "AssetMetadataV1",
	"AssetOcrV1":          "AssetOcrV1",
	"PartnersV1":          "PartnerV1",
	"PartnerAssetsV1":     "PartnerAssetV1",
	"PartnerAssetsV2":     "PartnerAssetV2",
	"PartnerAssetExifsV1": "PartnerAssetExifV1",
	"PartnerStacksV1":     "PartnerStackV1",
	"AlbumsV1":            "AlbumV1",
	"AlbumsV2":            "AlbumV2",
	"AlbumUsersV1":        "AlbumUserV1",
	"AlbumToAssetsV1":     "AlbumToAssetV1",
	"AlbumAssetsV1":       "AlbumAssetCreateV1",
	"AlbumAssetsV2":       "AlbumAssetCreateV2",
	"AlbumAssetExifsV1":   "AlbumAssetExifCreateV1",
	"MemoriesV1":          "MemoryV1",
	"MemoryToAssetsV1":    "MemoryToAssetV1",
	"StacksV1":            "StackV1",
	"PeopleV1":            "PersonV1",
	"AssetFacesV1":        "AssetFaceV1",
	"AssetFacesV2":        "AssetFaceV2",
	"UserMetadataV1":      "UserMetadataV1",
}

// handleSyncStream 实现 Immich /api/sync/stream：登录后客户端据此做一次全量同步。
// 客户端 POST 一个 SyncStreamDto{types:[...]}，服务器以 JSON-lines（每行一个 JSON 对象）
// 流式返回每个类型的实体数据，最后以 SyncCompleteV1 标记同步结束。
// 本最小实现对每个请求类型返回空数据（当前用户类型填充本节点身份），足以让客户端完成同步并进入首页。
func (h *Handler) handleSyncStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Types []string `json:"types"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &req)

	flusher, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "application/jsonlines+json")
	w.WriteHeader(http.StatusOK)

	now := time.Now().UTC().Format(time.RFC3339)
	writeLine := func(obj map[string]any) {
		b, _ := json.Marshal(obj)
		b = append(b, '\n')
		_, _ = w.Write(b)
		if flusher != nil {
			flusher.Flush()
		}
	}

	for _, t := range req.Types {
		respType, ok := syncRequestToResponse[t]
		if !ok {
			continue
		}
		data := []any{}
		switch respType {
		case "AuthUserV1":
			data = []any{h.syncAuthUser(now)}
		case "UserV1":
			data = []any{h.syncUser(now)}
		}
		writeLine(map[string]any{
			"type": respType,
			"data": data,
			"ack":  randomAccessToken(),
		})
	}
	// 同步完成标记：客户端据此结束同步流。
	writeLine(map[string]any{"type": "SyncCompleteV1", "data": []any{}})
}

// syncAuthUser 拼出与 SyncAuthUserV1 必填字段对齐的当前（admin）用户。
func (h *Handler) syncAuthUser(now string) map[string]any {
	return map[string]any{
		"id":                h.ServerID,
		"email":             "admin@immich.local",
		"name":              h.ServerName,
		"isAdmin":           true,
		"hasProfileImage":   false,
		"oauthId":           "",
		"pinCode":           nil,
		"deletedAt":         nil,
		"profileChangedAt":  now,
		"quotaSizeInBytes":  nil,
		"quotaUsageInBytes": 0,
		"storageLabel":      nil,
		"avatarColor":       "primary",
	}
}

// syncUser 拼出与 SyncUserV1 必填字段对齐的当前（admin）用户。
func (h *Handler) syncUser(now string) map[string]any {
	return map[string]any{
		"id":               h.ServerID,
		"email":            "admin@immich.local",
		"name":             h.ServerName,
		"hasProfileImage":  false,
		"deletedAt":        nil,
		"profileChangedAt": now,
		"avatarColor":      "primary",
	}
}

// ---- Immich 实时通道：socket.io (engine.io v4) ----

var socketIOUpgrader = websocket.Upgrader{
	// 移动端跨域直连本节点，放行所有 Origin。
	CheckOrigin: func(r *http.Request) bool { return true },
}

// handleSocketIO 实现最小 engine.io v4（仅 websocket transport）。流程：
//  1. 升级为 websocket 后立即下发 open 包：0{...sid/pingInterval...}
//  2. 收到客户端 "40"（namespace "/" connect）回应 "40"（connect ack），客户端即认为已连接
//  3. 双向心跳：客户端 "2"(ping) → 回 "3"(pong)；服务端周期性发 "2" 保活
//
// 不主动推送业务事件（无实时资产更新），但足以让客户端建立连接、消除 404 报错。
func (h *Handler) handleSocketIO(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	conn, err := socketIOUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	sid := randomAccessToken()
	open := fmt.Sprintf(
		`0{"sid":%q,"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}`,
		sid,
	)
	_ = conn.WriteMessage(websocket.TextMessage, []byte(open))

	// 周期性服务端心跳，防止客户端因长时间无消息而断线重连。
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ping.C:
				_ = conn.WriteMessage(websocket.TextMessage, []byte("2"))
			}
		}
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		s := string(msg)
		if len(s) == 0 {
			continue
		}
		switch s[0] {
		case '2': // 客户端 ping
			_ = conn.WriteMessage(websocket.TextMessage, []byte("3"))
		case '4': // 消息帧：客户端 "40" 为 namespace "/" 连接请求
			if s == "40" {
				_ = conn.WriteMessage(websocket.TextMessage, []byte("40"))
			}
		}
	}
	close(done)
}

// ---- Immich 客户端媒体 API（§media-api） ----

func (h *Handler) assetUnavailable(w http.ResponseWriter) {
	http.Error(w, "asset api not configured", http.StatusNotImplemented)
}

func (h *Handler) handleBulkUploadCheck(w http.ResponseWriter, r *http.Request) {
	if h.AssetStore == nil {
		h.assetUnavailable(w)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req BulkUploadCheckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	exists := map[string]string{}
	if req.DeviceID != "" && len(req.DeviceAssetIDs) > 0 {
		m, err := h.AssetStore.LookupDeviceAssets(req.DeviceID, req.DeviceAssetIDs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		exists = m
	}
	resp := BulkUploadCheckResponse{Results: make([]AssetExistence, 0, len(req.DeviceAssetIDs))}
	for _, daid := range req.DeviceAssetIDs {
		if id, ok := exists[daid]; ok {
			resp.Results = append(resp.Results, AssetExistence{ID: id, DeviceAssetID: daid, Exists: true})
		} else {
			resp.Results = append(resp.Results, AssetExistence{ID: "", DeviceAssetID: daid, Exists: false})
		}
	}
	writeJSON(w, resp)
}

// handleAssets GET 列表 / POST 上传。
func (h *Handler) handleAssets(w http.ResponseWriter, r *http.Request) {
	if h.AssetStore == nil {
		h.assetUnavailable(w)
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.listAssets(w, r)
	case http.MethodPost:
		h.uploadAsset(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) listAssets(w http.ResponseWriter, r *http.Request) {
	assets, err := h.AssetStore.ListAssets()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]AssetResponse, 0, len(assets))
	for _, a := range assets {
		out = append(out, h.buildAssetResponse(a))
	}
	writeJSON(w, out)
}

func (h *Handler) uploadAsset(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	fh := r.MultipartForm.File["assetData"]
	if len(fh) == 0 {
		http.Error(w, "missing assetData", http.StatusBadRequest)
		return
	}
	deviceAssetID := formStr(r, "deviceAssetId")
	deviceID := formStr(r, "deviceId")
	fileCreatedAt := formStr(r, "fileCreatedAt")
	fileModifiedAt := formStr(r, "fileModifiedAt")
	isFavorite := formStr(r, "isFavorite") == "true"
	duration := formStr(r, "duration")

	// 去重：相同 (deviceId, deviceAssetId) 已存在则直接返回既有 id。
	if deviceID != "" && deviceAssetID != "" {
		if m, err := h.AssetStore.LookupDeviceAssets(deviceID, []string{deviceAssetID}); err == nil {
			if id, ok := m[deviceAssetID]; ok {
				writeJSONStatus(w, AssetUploadResponse{ID: id, Duplicate: true}, http.StatusOK)
				return
			}
		}
	}

	f := fh[0]
	src, err := f.Open()
	if err != nil {
		http.Error(w, "open upload: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer src.Close()
	data, err := io.ReadAll(src)
	if err != nil {
		http.Error(w, "read upload: "+err.Error(), http.StatusInternalServerError)
		return
	}
	sum := sha256.Sum256(data)
	assetID := hex.EncodeToString(sum[:])
	size := int64(len(data))

	// 选盘：本节点 free_bytes 最大的已认领盘；无则回退单根 BlobRoot。
	diskSerial := ""
	blobRoot := h.BlobRoot
	if disks, err := h.AssetStore.ListMountedDisks(h.NodeID); err == nil && len(disks) > 0 {
		diskSerial = disks[0].DiskSerial
		if root, ok := h.AssetStore.DiskRoot(diskSerial); ok {
			blobRoot = root
		}
	}
	if blobRoot == "" {
		http.Error(w, "no storage configured (set BLOB_ROOT or DISK_DIRS)", http.StatusInternalServerError)
		return
	}

	// dir_key 由拍摄/修改时间推导，失败回退当前时间。
	t := time.Now()
	if s := fileCreatedAt; s != "" {
		if pt, err := time.Parse(time.RFC3339, s); err == nil {
			t = pt
		} else if pt, err := time.Parse(time.RFC3339, fileModifiedAt); err == nil {
			t = pt
		}
	}
	dirKey := t.Format("2006/01")

	// 写物理字节 + sidecar（仓库即真相）。
	destDir := filepath.Join(blobRoot, filepath.FromSlash(dirKey))
	dest := filepath.Join(destDir, assetID)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		http.Error(w, "mkdir: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.WriteFile(dest, data, 0o644); err != nil {
		http.Error(w, "write: "+err.Error(), http.StatusInternalServerError)
		return
	}
	ext := strings.ToLower(filepath.Ext(f.Filename))
	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	kind := "other"
	assetType := "IMAGE"
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		kind, assetType = "photo", "IMAGE"
	case strings.HasPrefix(mimeType, "video/"):
		kind, assetType = "video", "VIDEO"
	}
	if err := ingest.FlushMeta(blobRoot, dirKey, []ingest.MetaAsset{{
		AssetID:      assetID,
		Checksum:     assetID,
		SizeBytes:    size,
		OriginalPath: f.Filename,
		CapturedAt:   t.Format(time.RFC3339),
		Kind:         kind,
	}}); err != nil {
		http.Error(w, "meta: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 写元数据。
	asset := model.Asset{AssetID: assetID, SizeBytes: size, Checksum: assetID, DirKey: dirKey, OriginalPath: f.Filename}
	if err := h.AssetStore.SaveUploadedAsset(asset, diskSerial, size); err != nil {
		http.Error(w, "store: "+err.Error(), http.StatusInternalServerError)
		return
	}
	meta := model.AssetMeta{
		AssetID:          assetID,
		DeviceAssetID:    deviceAssetID,
		DeviceID:         deviceID,
		FileCreatedAt:    fileCreatedAt,
		FileModifiedAt:   fileModifiedAt,
		IsFavorite:       isFavorite,
		Duration:         duration,
		Type:             assetType,
		MimeType:         mimeType,
		OriginalFileName: f.Filename,
	}
	if err := h.AssetStore.SaveAssetMeta(meta); err != nil {
		http.Error(w, "store meta: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if deviceID != "" && deviceAssetID != "" {
		_ = h.AssetStore.SaveDeviceAsset(deviceID, deviceAssetID, assetID)
	}

	writeJSONStatus(w, AssetUploadResponse{ID: assetID, Duplicate: false}, http.StatusCreated)
}

// handleAssetItem 处理 /api/assets/{id} 及其子资源（/original /thumbnail）与 DELETE。
func (h *Handler) handleAssetItem(w http.ResponseWriter, r *http.Request) {
	if h.AssetStore == nil {
		h.assetUnavailable(w)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api/assets/")
	parts := strings.SplitN(rest, "/", 2)
	id := parts[0]
	if id == "" {
		http.Error(w, "missing asset id", http.StatusBadRequest)
		return
	}
	sub := ""
	if len(parts) > 1 {
		sub = parts[1]
	}

	switch r.Method {
	case http.MethodDelete:
		h.deleteAsset(w, r, id)
		return
	case http.MethodGet:
		// ok
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	switch sub {
	case "":
		a, ok, err := h.AssetStore.GetAsset(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "asset not found", http.StatusNotFound)
			return
		}
		writeJSON(w, h.buildAssetResponse(a))
	case "original", "thumbnail":
		h.serveAssetBytes(w, r, id, sub)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (h *Handler) deleteAsset(w http.ResponseWriter, r *http.Request, id string) {
	a, ok, err := h.AssetStore.GetAsset(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "asset not found", http.StatusNotFound)
		return
	}
	// 删除元数据前先定位物理仓库根（删除副本记录会丢失磁盘归属）。
	blobRoot := h.BlobRoot
	if reps, err := h.AssetStore.ListReplicas(id); err == nil && len(reps) > 0 {
		if root, ok := h.AssetStore.DiskRoot(reps[0].DiskSerial); ok {
			blobRoot = root
		}
	}
	if _, _, err := h.AssetStore.DeleteAsset(id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if blobRoot != "" {
		_ = os.Remove(filepath.Join(blobRoot, filepath.FromSlash(a.DirKey), a.AssetID))
		_ = ingest.RemoveAssetFromMeta(blobRoot, a.DirKey, a.AssetID)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) serveAssetBytes(w http.ResponseWriter, r *http.Request, id, sub string) {
	a, ok, err := h.AssetStore.GetAsset(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "asset not found", http.StatusNotFound)
		return
	}
	blobRoot := h.BlobRoot
	if reps, err := h.AssetStore.ListReplicas(id); err == nil && len(reps) > 0 {
		if root, ok := h.AssetStore.DiskRoot(reps[0].DiskSerial); ok {
			blobRoot = root
		}
	}
	if blobRoot == "" {
		http.Error(w, "no storage configured", http.StatusInternalServerError)
		return
	}
	phys := filepath.Join(blobRoot, filepath.FromSlash(a.DirKey), a.AssetID)
	f, err := os.Open(phys)
	if err != nil {
		http.Error(w, "asset not found", http.StatusNotFound)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		http.Error(w, "stat failed", http.StatusInternalServerError)
		return
	}
	ext := strings.ToLower(filepath.Ext(a.OriginalPath))
	if ext == "" {
		ext = strings.ToLower(filepath.Ext(a.AssetID))
	}
	ct := mime.TypeByExtension(ext)
	if ct == "" {
		ct = "application/octet-stream"
	}
	if sub == "thumbnail" && !strings.HasPrefix(ct, "image/") {
		ct = "image/jpeg"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	w.Header().Set("Accept-Ranges", "bytes")
	io.Copy(w, f)
}

// buildAssetResponse 由资产 + 元信息拼出客户端响应。
func (h *Handler) buildAssetResponse(a model.Asset) AssetResponse {
	meta, _, _ := h.AssetStore.GetAssetMeta(a.AssetID)
	now := time.Now().UTC().Format(time.RFC3339)
	resp := AssetResponse{
		ID:               a.AssetID,
		OwnerID:          h.ServerID,
		DeviceAssetID:    meta.DeviceAssetID,
		DeviceID:         meta.DeviceID,
		Type:             meta.Type,
		OriginalPath:     a.OriginalPath,
		FileCreatedAt:    meta.FileCreatedAt,
		FileModifiedAt:   meta.FileModifiedAt,
		CreatedAt:        now,
		UpdatedAt:        now,
		IsFavorite:       meta.IsFavorite,
		IsArchived:       false,
		IsTrashed:        false,
		OriginalFileName: meta.OriginalFileName,
		MimeType:         meta.MimeType,
		FileSize:         a.SizeBytes,
		Width:            meta.Width,
		Height:           meta.Height,
		Duration:         meta.Duration,
		Checksum:         a.Checksum,
	}
	if resp.Type == "" {
		resp.Type = "IMAGE"
	}
	return resp
}

func formStr(r *http.Request, key string) string {
	if r.MultipartForm == nil {
		return ""
	}
	vs := r.MultipartForm.Value[key]
	if len(vs) == 0 {
		return ""
	}
	return vs[0]
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONStatus(w http.ResponseWriter, v any, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func randomAccessToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

func (h *Handler) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p := h.Provider.GetState()
	// 节点元信息：供 WebUI 看板展示（§dashboard），不参与集群 HMAC 签名。
	p.Node = &NodeInfo{
		NodeID:     h.NodeID,
		ServerName: h.ServerName,
		ServerURL:  h.ServerURL,
		Version:    h.Version,
		StartedAt:  h.StartedAt,
		UptimeSec:  h.Now() - h.StartedAt,
	}
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
//     因为同节点盘间迁移可能共享同一仓库根下的文件，删字节会误伤目标副本。
// 幂等且对称——同一请求发给两端，只有源节点会执行。
func (h *Handler) handleReleaseSource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		DirKey        string   `json:"dirKey"`
		SrcDisk       string   `json:"srcDisk"`
		DstDisk       string   `json:"dstDisk"`
		ReleaseNode   string   `json:"releaseNode"`
		ReleaseAssets []string `json:"releaseAssets,omitempty"` // 可选：仅释放这些资产；空=释放目录下全部
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
		// 仅保留调用方标记为可安全释放的资产（门禁在 worker 侧决策）。
		allow := map[string]bool{}
		if len(req.ReleaseAssets) == 0 {
			for _, a := range assets { // 空列表=释放全部（向后兼容直接调用）
				allow[a.AssetID] = true
			}
		} else {
			for _, id := range req.ReleaseAssets {
				allow[id] = true
			}
		}
		for _, a := range assets {
			if !allow[a.AssetID] {
				continue // 释放后会低于 MinReplicas，保留源副本
			}
			if err := h.Provider.DeleteReplica(a.AssetID, req.SrcDisk); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		if node, ok := h.Provider.GetDiskLocation(req.DstDisk); !ok || node != h.NodeID {
			root, _ := h.Provider.DiskRoot(req.SrcDisk)
			if root == "" {
				root = h.BlobRoot // 回退：Provider 未记录 blob_root 时使用单根
			}
			for _, a := range assets {
				if !allow[a.AssetID] {
					continue
				}
				_ = os.Remove(filepath.Join(root, req.DirKey, a.AssetID))
			}
		}
	}
	w.WriteHeader(http.StatusOK)
}

// handleBlob 提供 blob 字节流拉取（§9.1），支持 Range 续传（206）。
// 支持两种定位方式：
//   - 带 ?disk=<diskSerial>&dir=<dirKey>：按每磁盘仓库（blob_root/<dirKey>/<assetID>）定位；
//   - 无查询参数：回退到 h.Source（单根 FileSystemBlobSource，向后兼容）。
func (h *Handler) handleBlob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	assetID := lastPathSeg(r.URL.Path, "/api/cluster/blob/")
	disk := r.URL.Query().Get("disk")
	dirKey := r.URL.Query().Get("dir")

	var size int64
	var checksum string
	var src io.ReadCloser
	var err error

	if disk != "" && dirKey != "" {
		// 每磁盘仓库路径：blobRoot/<dirKey>/<assetID>
		root, ok := h.Provider.DiskRoot(disk)
		if !ok && h.BlobRoot != "" {
			root = h.BlobRoot
		}
		if root == "" {
			http.Error(w, "disk blob root not found", http.StatusNotFound)
			return
		}
		// 避免目录穿越：dirKey 与 assetID 均不得含 ".."
		if strings.Contains(dirKey, "..") || strings.Contains(assetID, "..") {
			http.Error(w, "invalid path", http.StatusBadRequest)
			return
		}
		phys := filepath.Join(root, dirKey, assetID)
		fi, serr := os.Stat(phys)
		if serr != nil {
			http.Error(w, "blob not found", http.StatusNotFound)
			return
		}
		size = fi.Size()
		src, err = os.Open(phys)
		if err != nil {
			http.Error(w, "open failed", http.StatusInternalServerError)
			return
		}
	} else {
		if h.Source == nil {
			http.Error(w, "blob source not configured", http.StatusNotImplemented)
			return
		}
		var ok bool
		size, checksum, ok = h.Source.StatBlob(assetID)
		if !ok {
			http.Error(w, "blob not found", http.StatusNotFound)
			return
		}
		src, err = h.Source.OpenBlob(assetID, 0)
		if err != nil {
			http.Error(w, "open failed", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	if checksum != "" {
		w.Header().Set("X-Blob-Checksum", checksum)
	}

	rng := r.Header.Get("Range")
	if rng == "" {
		defer src.Close()
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		io.Copy(w, src)
		return
	}

	_ = src.Close()
	start, end, ok := parseByteRange(rng, size)
	if !ok {
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(size, 10))
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	// 重新打开（按 per-disk 或 Source），并定位到 start 偏移
	var rc io.ReadCloser
	if disk != "" && dirKey != "" {
		root, _ := h.Provider.DiskRoot(disk)
		if root == "" {
			root = h.BlobRoot
		}
		f, serr := os.Open(filepath.Join(root, dirKey, assetID))
		if serr != nil {
			http.Error(w, "open failed", http.StatusInternalServerError)
			return
		}
		if start > 0 {
			if _, serr := f.Seek(start, io.SeekStart); serr != nil {
				f.Close()
				http.Error(w, "seek failed", http.StatusInternalServerError)
				return
			}
		}
		rc = f
	} else {
		var serr error
		rc, serr = h.Source.OpenBlob(assetID, start)
		if serr != nil {
			http.Error(w, "open failed", http.StatusInternalServerError)
			return
		}
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
