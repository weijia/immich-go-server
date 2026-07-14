package cluster

import (
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
type GlobalRepo struct {
	Disks map[string]model.Disk
	Local coordinator.Repository
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
func (g *GlobalRepo) SubmitTask(t clusterapi.Task) error {
	return g.Local.SubmitTask(t)
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
