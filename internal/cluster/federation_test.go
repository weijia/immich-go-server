package cluster

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/coordinator"
	"github.com/weijia/immich-go-server/internal/crypto"
	"github.com/weijia/immich-go-server/internal/model"
)

const fedSecret = "shared-cluster-secret"

// ---- fake 实现 ----

// fakeProvider 实现 clusterapi.StateProvider，供 httptest 起真实 /state 端点。
type fakeProvider struct {
	state clusterapi.StatePayload
}

func (f *fakeProvider) GetState() clusterapi.StatePayload { return f.state }
func (f *fakeProvider) GetDiskLocation(serial string) (string, bool) {
	return "", false
}
func (f *fakeProvider) RegisterReplica(assetID, diskSerial, checksum string) error {
	return nil
}
func (f *fakeProvider) SubmitTask(task clusterapi.Task) error { return nil }

// fakeRepo 内存 coordinator.Repository，供 GlobalRepo 的本地部分与跨节点调度测试。
type fakeRepo struct {
	dirs     []model.Directory
	disks    []model.Disk
	assets   []model.Asset
	replicas map[string][]model.Replica
	counts   map[string]int
	tasks    []clusterapi.Task
}

func (f *fakeRepo) ListDirectories() ([]model.Directory, error) { return f.dirs, nil }
func (f *fakeRepo) ListDisks() ([]model.Disk, error)            { return f.disks, nil }
func (f *fakeRepo) ListAssets() ([]model.Asset, error)          { return f.assets, nil }
func (f *fakeRepo) ListReplicas(id string) ([]model.Replica, error) {
	return f.replicas[id], nil
}
func (f *fakeRepo) ReplicaCount(id string) int { return f.counts[id] }
func (f *fakeRepo) SubmitTask(t clusterapi.Task) error {
	for _, x := range f.tasks {
		if x.TaskID == t.TaskID {
			return nil
		}
	}
	f.tasks = append(f.tasks, t)
	return nil
}

func hotDisk(serial, node string) model.Disk {
	return model.Disk{DiskSerial: serial, Tier: model.TierHot, CapacityBytes: 100 << 30, FreeBytes: 90 << 30, MountedNodeID: node, OnlineSeconds: 900}
}
func coldDisk(serial, node string) model.Disk {
	return model.Disk{DiskSerial: serial, Tier: model.TierCold, CapacityBytes: 100 << 30, FreeBytes: 90 << 30, MountedNodeID: node, OnlineSeconds: 100}
}

// ---- 测试 ----

func TestVerifyStatePayload(t *testing.T) {
	sp := clusterapi.StatePayload{NodeID: "B", Disks: []clusterapi.DiskState{{DiskSerial: "DB", Tier: "HOT", OnlineSeconds: 500}}}
	sp.SignedAt = 1000
	raw, _ := json.Marshal(sp)
	sp.Signature = crypto.SignPayload(fedSecret, sp.NodeID, sp.SignedAt, raw)

	if !VerifyStatePayload(fedSecret, sp, 1000, 60) {
		t.Error("valid signature should verify")
	}
	if VerifyStatePayload("wrong-secret", sp, 1000, 60) {
		t.Error("wrong secret should fail")
	}
	// 时间新鲜度：超出 skew
	if VerifyStatePayload(fedSecret, sp, 1000+100, 60) {
		t.Error("stale payload should fail")
	}
}

func TestFederationRun(t *testing.T) {
	// 两个 peer 节点，各起真实 HMAC 鉴权 HTTP 服务
	pB := &fakeProvider{state: clusterapi.StatePayload{NodeID: "B", Disks: []clusterapi.DiskState{
		{DiskSerial: "DB", Tier: "HOT", FreeBytes: 90 << 30, MountedNodeID: "B", OnlineSeconds: 500},
	}}}
	pC := &fakeProvider{state: clusterapi.StatePayload{NodeID: "C", Disks: []clusterapi.DiskState{
		{DiskSerial: "DC", Tier: "COLD", FreeBytes: 90 << 30, MountedNodeID: "C", OnlineSeconds: 200},
	}}}
	srvB := httptest.NewServer(clusterapi.NewHandler("B", fedSecret, 60, pB).Mux())
	srvC := httptest.NewServer(clusterapi.NewHandler("C", fedSecret, 60, pC).Mux())
	defer srvB.Close()
	defer srvC.Close()

	client := NewClient("A", fedSecret, 60)
	fed := &Federation{
		SelfNodeID: "A",
		SelfState: clusterapi.StatePayload{NodeID: "A", Disks: []clusterapi.DiskState{
			{DiskSerial: "DA", Tier: "WARM", FreeBytes: 50 << 30, MountedNodeID: "A", OnlineSeconds: 300},
		}},
		Peers:  []Peer{{NodeID: "B", URL: srvB.URL}, {NodeID: "C", URL: srvC.URL}},
		Client: client,
	}

	// 单独验证 Client.FetchState 走真实 HTTP + 签名校验
	spB, err := client.FetchState(context.Background(), srvB.URL)
	if err != nil {
		t.Fatalf("FetchState B: %v", err)
	}
	if spB.NodeID != "B" || len(spB.Disks) != 1 || spB.Disks[0].DiskSerial != "DB" {
		t.Fatalf("unexpected fetched state: %+v", spB)
	}
	// 错误密钥应被拒绝
	bad := NewClient("A", "evil", 60)
	if _, err := bad.FetchState(context.Background(), srvB.URL); err == nil {
		t.Error("FetchState with wrong secret should fail")
	}

	// 聚合全局视图
	gv, err := fed.Run(context.Background())
	if err != nil {
		t.Fatalf("Federation.Run: %v", err)
	}
	if len(gv.Disks) != 3 {
		t.Fatalf("expected 3 merged disks, got %d: %v", len(gv.Disks), gv.Disks)
	}
	for _, s := range []string{"DA", "DB", "DC"} {
		if _, ok := gv.Disks[s]; !ok {
			t.Errorf("missing disk %s in global view", s)
		}
	}
	// 协调者应为在线秒最高的 B(500)
	if gv.Coordinator != "B" {
		t.Errorf("expected coordinator B, got %s", gv.Coordinator)
	}
}

func TestGlobalRepoCrossNodeCoordinator(t *testing.T) {
	// 聚合视图含本节点 DA(WARM) 与远端 DB(HOT) / DC(COLD)
	disks := map[string]model.Disk{
		"DA": {DiskSerial: "DA", Tier: model.TierWarm, CapacityBytes: 100 << 30, FreeBytes: 50 << 30, MountedNodeID: "A", OnlineSeconds: 300},
		"DB": hotDisk("DB", "B"),  // 远端 HOT 盘
		"DC": coldDisk("DC", "C"), // 远端 COLD 盘
	}
	// 本地仅有目录元数据：一个本应处于 HOT 的目录目前落在 DA(WARM)
	local := &fakeRepo{
		dirs: []model.Directory{{DirKey: "2026-06", DiskSerial: "DA", Tier: model.TierWarm, Temperature: 0.9, TotalBytes: 5 << 30}},
	}
	gr := &GlobalRepo{Disks: disks, Local: local, SelfID: "B"}
	c := coordinator.New(gr, config.Default())
	n, err := c.RunBalancingCycle()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 migration task, got %d (%+v)", n, local.tasks)
	}
	task := local.tasks[0]
	if task.Type != "MIGRATION" {
		t.Fatalf("expected MIGRATION, got %s", task.Type)
	}
	if task.SrcDisk != "DA" {
		t.Errorf("src should be DA, got %s", task.SrcDisk)
	}
	// 关键：目标盘 DB 是远端节点 B 的盘 → 证明调度已跨节点
	if task.DstDisk != "DB" {
		t.Errorf("expected cross-node target DB, got %s", task.DstDisk)
	}
	if gr.Disks[task.DstDisk].MountedNodeID != "B" {
		t.Errorf("target disk should be mounted on remote node B")
	}
}

// 确保编译期 fakeProvider 满足 StateProvider。
var _ clusterapi.StateProvider = (*fakeProvider)(nil)
var _ coordinator.Repository = (*fakeRepo)(nil)
var _ coordinator.Repository = (*GlobalRepo)(nil)
