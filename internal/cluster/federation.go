package cluster

import (
	"bytes"
	"context"
	"crypto/hmac"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/coordinator"
	"github.com/weijia/immich-go-server/internal/crypto"
	"github.com/weijia/immich-go-server/internal/model"
)

// Client 是带 HMAC 鉴权的集群 API 客户端（§9.5），用于主动拉取 peer 状态。
type Client struct {
	SelfNodeID  string
	Secret      string
	MaxSkew     int64
	Now         func() int64
	HTTPClient  *http.Client
}

// NewClient 构造集群客户端。
func NewClient(selfNodeID, secret string, maxSkew int64) *Client {
	return &Client{
		SelfNodeID: selfNodeID,
		Secret:     secret,
		MaxSkew:    maxSkew,
		Now:        func() int64 { return time.Now().Unix() },
		HTTPClient: http.DefaultClient,
	}
}

// FetchState 经 HMAC 鉴权 GET 拉取对端 /state，并校验 payload 签名与时间新鲜度（§9.5）。
func (c *Client) FetchState(ctx context.Context, baseURL string) (clusterapi.StatePayload, error) {
	now := c.Now()
	hdr, err := clusterapi.SignHeaders(c.SelfNodeID, c.Secret, http.MethodGet, "/api/cluster/state", nil, now)
	if err != nil {
		return clusterapi.StatePayload{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(baseURL, "/api/cluster/state"), nil)
	if err != nil {
		return clusterapi.StatePayload{}, err
	}
	req.Header = hdr
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return clusterapi.StatePayload{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return clusterapi.StatePayload{}, fmt.Errorf("fetch state: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return clusterapi.StatePayload{}, err
	}
	var sp clusterapi.StatePayload
	if err := json.Unmarshal(body, &sp); err != nil {
		return clusterapi.StatePayload{}, err
	}
	if !VerifyStatePayload(c.Secret, sp, now, c.MaxSkew) {
		return clusterapi.StatePayload{}, fmt.Errorf("fetch state: signature verification failed")
	}
	return sp, nil
}

// FetchDiskLocation 经 HMAC 鉴权 GET 拉取磁盘所在挂载节点（§9.4）；404 返回 ok=false。
func (c *Client) FetchDiskLocation(ctx context.Context, baseURL, serial string) (string, bool, error) {
	path := "/api/cluster/disk/" + serial
	now := c.Now()
	hdr, err := clusterapi.SignHeaders(c.SelfNodeID, c.Secret, http.MethodGet, path, nil, now)
	if err != nil {
		return "", false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(baseURL, path), nil)
	if err != nil {
		return "", false, err
	}
	req.Header = hdr
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", false, fmt.Errorf("fetch disk location: status %d", resp.StatusCode)
	}
	var m struct {
		MountedNodeID string `json:"mountedNodeId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return "", false, err
	}
	return m.MountedNodeID, true, nil
}

// VerifyStatePayload 校验状态 payload 自带签名及新鲜度（§9.5）。
// 重建签名时剥离 Signature 字段（omitempty）后对相同 body 重算并做常数时间比较。
func VerifyStatePayload(secret string, sp clusterapi.StatePayload, now, maxSkew int64) bool {
	if sp.Signature == "" || sp.SignedAt == 0 {
		return false
	}
	if now < sp.SignedAt-maxSkew || now > sp.SignedAt+maxSkew {
		return false
	}
	tmp := sp
	tmp.Signature = ""
	raw, err := json.Marshal(tmp)
	if err != nil {
		return false
	}
	expected := crypto.SignPayload(secret, sp.NodeID, sp.SignedAt, raw)
	return hmacEqual(expected, sp.Signature)
}

// Peer 描述一个对端节点及其可达基址。
type Peer struct {
	NodeID string
	URL    string
}

// GlobalView 跨节点聚合后的全局视图（§9 / §10）：
// 所有磁盘按 diskSerial 合并；并选出协调者节点。
type GlobalView struct {
	SelfNodeID  string
	Nodes       []string
	Disks       map[string]model.Disk
	Coordinator string
}

// Federation 聚合本节点与一组 peer 的状态为全局视图。
type Federation struct {
	SelfNodeID string
	SelfState  clusterapi.StatePayload
	Peers      []Peer
	Client     *Client
}

// Run 拉取所有 peer 状态、校验、聚合为 GlobalView。
func (f *Federation) Run(ctx context.Context) (GlobalView, error) {
	nodes := []model.Node{nodeFromState(f.SelfNodeID, f.SelfState)}
	for _, p := range f.Peers {
		sp, err := f.Client.FetchState(ctx, p.URL)
		if err != nil {
			return GlobalView{}, fmt.Errorf("peer %s: %w", p.NodeID, err)
		}
		nodes = append(nodes, nodeFromState(sp.NodeID, sp))
	}
	merged := AggregateDiskStats(nodes)
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.NodeID)
	}
	sort.Strings(ids)
	return GlobalView{
		SelfNodeID:  f.SelfNodeID,
		Nodes:       ids,
		Disks:       merged,
		Coordinator: ElectCoordinator(nodes),
	}, nil
}

// nodeFromState 把单节点状态转为 model.Node，并以其磁盘在线秒数之和作为节点可靠度评分。
func nodeFromState(nodeID string, sp clusterapi.StatePayload) model.Node {
	disks := make([]model.Disk, 0, len(sp.Disks))
	var score float64
	for _, d := range sp.Disks {
		disks = append(disks, model.Disk{
			DiskSerial:    d.DiskSerial,
			Tier:          model.Tier(d.Tier),
			FreeBytes:     d.FreeBytes,
			MountedNodeID: d.MountedNodeID,
			OnlineSeconds: d.OnlineSeconds,
		})
		score += float64(d.OnlineSeconds)
	}
	return model.Node{NodeID: nodeID, OnlineScore: score, Disks: disks}
}

// GlobalRepo 把全局聚合视图适配为 coordinator.Repository（§6 / §9.2）：
// 磁盘来自跨节点合并视图；目录/资产/副本/任务下发仍走本节点本地存储。
// SubmitTask 会把任务下发到 dstDisk 实际挂载的节点（本节点则本地落库），
// 由该节点的 worker 以"拉模型"执行（源可来自任意节点，§6.5 / §9.1）。
type GlobalRepo struct {
	Disks   map[string]model.Disk
	Local   coordinator.Repository
	SelfID  string
	PeerURL func(nodeID string) (string, bool) // 节点 ID → 基址（http://host:port）
	Client  *Client
}

// ListDisks 返回聚合后的全局磁盘视图（跨节点）。
func (g *GlobalRepo) ListDisks() ([]model.Disk, error) {
	out := make([]model.Disk, 0, len(g.Disks))
	for _, d := range g.Disks {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DiskSerial < out[j].DiskSerial })
	return out, nil
}

func (g *GlobalRepo) ListDirectories() ([]model.Directory, error) { return g.Local.ListDirectories() }
func (g *GlobalRepo) ListAssets() ([]model.Asset, error)          { return g.Local.ListAssets() }
func (g *GlobalRepo) ListReplicas(a string) ([]model.Replica, error) {
	return g.Local.ListReplicas(a)
}
func (g *GlobalRepo) ReplicaCount(a string) int { return g.Local.ReplicaCount(a) }

// SubmitTask 路由任务到 dstDisk 所在节点（§9.2）：本节点则本地落库，对端则 HMAC POST。
func (g *GlobalRepo) SubmitTask(t clusterapi.Task) error {
	owner := ""
	if d, ok := g.Disks[t.DstDisk]; ok {
		owner = d.MountedNodeID
	}
	if owner == "" || owner == g.SelfID {
		return g.Local.SubmitTask(t)
	}
	if g.PeerURL == nil || g.Client == nil {
		return fmt.Errorf("no route to dst owner %s for disk %s", owner, t.DstDisk)
	}
	url, ok := g.PeerURL(owner)
	if !ok {
		return fmt.Errorf("unknown peer %s for disk %s", owner, t.DstDisk)
	}
	return g.Client.SubmitTask(context.Background(), url, t)
}

// SubmitTask 经 HMAC 鉴权 POST 一条任务到对端节点（§9.2）。
func (c *Client) SubmitTask(ctx context.Context, baseURL string, task clusterapi.Task) error {
	path := "/api/cluster/task"
	body, err := json.Marshal(task)
	if err != nil {
		return err
	}
	now := c.Now()
	hdr, err := clusterapi.SignHeaders(c.SelfNodeID, c.Secret, http.MethodPost, path, body, now)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(baseURL, path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header = hdr
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("submit task: status %d", resp.StatusCode)
	}
	return nil
}

// GetDirectory 经 HMAC 鉴权 GET 拉取对端某目录元数据（§9.x 目录跨节点重宿主）：
// 目标节点在本地无目录记录时从源节点拉取权威视图。404 返回 ok=false。
func (c *Client) GetDirectory(ctx context.Context, baseURL, dirKey string) (model.Directory, bool, error) {
	path := "/api/cluster/directory/" + dirKey
	now := c.Now()
	hdr, err := clusterapi.SignHeaders(c.SelfNodeID, c.Secret, http.MethodGet, path, nil, now)
	if err != nil {
		return model.Directory{}, false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(baseURL, path), nil)
	if err != nil {
		return model.Directory{}, false, err
	}
	req.Header = hdr
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return model.Directory{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return model.Directory{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return model.Directory{}, false, fmt.Errorf("get directory: status %d", resp.StatusCode)
	}
	var dto clusterapi.DirectoryDTO
	if err := json.NewDecoder(resp.Body).Decode(&dto); err != nil {
		return model.Directory{}, false, err
	}
	return dto.ToModel(), true, nil
}

// RehostDirectory 经 HMAC 鉴权 POST 通知对端 relinquish 某目录记录（§9.x 目录跨节点重宿主）：
// 源节点收到后会删除其本地陈旧的目录聚合视图（数据已迁走）。
func (c *Client) RehostDirectory(ctx context.Context, baseURL, dirKey, relinquishNode string) error {
	path := "/api/cluster/directory/rehost"
	body, err := json.Marshal(struct {
		DirKey         string `json:"dirKey"`
		RelinquishNode string `json:"relinquishNode"`
	}{DirKey: dirKey, RelinquishNode: relinquishNode})
	if err != nil {
		return err
	}
	now := c.Now()
	hdr, err := clusterapi.SignHeaders(c.SelfNodeID, c.Secret, http.MethodPost, path, body, now)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(baseURL, path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header = hdr
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rehost directory: status %d", resp.StatusCode)
	}
	return nil
}

// ReleaseSource 经 HMAC 鉴权 POST 通知对端释放源盘（§9.x 真实源盘释放）：
// 源节点收到后删除其本地源副本记录，并（若 DstDisk 不在该节点）删除物理字节。
// releaseAssets 为空表示释放目录下全部资产；否则仅释放列表中的资产（门禁在调用方决策）。
func (c *Client) ReleaseSource(ctx context.Context, baseURL, dirKey, srcDisk, dstDisk, releaseNode string, releaseAssets []string) error {
	path := "/api/cluster/directory/release"
	body, err := json.Marshal(struct {
		DirKey        string   `json:"dirKey"`
		SrcDisk       string   `json:"srcDisk"`
		DstDisk       string   `json:"dstDisk"`
		ReleaseNode   string   `json:"releaseNode"`
		ReleaseAssets []string `json:"releaseAssets,omitempty"`
	}{DirKey: dirKey, SrcDisk: srcDisk, DstDisk: dstDisk, ReleaseNode: releaseNode, ReleaseAssets: releaseAssets})
	if err != nil {
		return err
	}
	now := c.Now()
	hdr, err := clusterapi.SignHeaders(c.SelfNodeID, c.Secret, http.MethodPost, path, body, now)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(baseURL, path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header = hdr
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("release source: status %d", resp.StatusCode)
	}
	return nil
}

// joinURL 拼接基址与路径，处理基址尾斜杠。
func joinURL(base, path string) string {
	u, err := url.Parse(base)
	if err != nil {
		return base + path
	}
	u.Path = u.Path + path
	return u.String()
}

// hmacEqual 常数时间比较两十六进制签名串。
func hmacEqual(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
}
