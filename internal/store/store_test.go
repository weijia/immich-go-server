package store

import (
	"path/filepath"
	"testing"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/model"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := NewStore(path, "node-A")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSaveGetDisk(t *testing.T) {
	s := newTestStore(t)
	d := model.Disk{
		DiskSerial: "SSD-A", Label: "ssd", CapacityBytes: 100 << 30, FreeBytes: 60 << 30,
		Tier: model.TierHot, MountedNodeID: "node-A", OnlineSeconds: 900, FirstSeenAt: 1, LastSeenAt: 2,
	}
	if err := s.SaveDisk(d); err != nil {
		t.Fatalf("SaveDisk: %v", err)
	}
	got, ok, err := s.GetDisk("SSD-A")
	if err != nil || !ok {
		t.Fatalf("GetDisk: ok=%v err=%v", ok, err)
	}
	if got.DiskSerial != d.DiskSerial || got.Tier != model.TierHot || got.FreeBytes != d.FreeBytes {
		t.Errorf("roundtrip mismatch: %+v", got)
	}
	// 不存在
	if _, ok, _ := s.GetDisk("NOPE"); ok {
		t.Error("expected not found")
	}
}

func TestDiskUpsertAndUpdateFree(t *testing.T) {
	s := newTestStore(t)
	d := model.Disk{DiskSerial: "SSD-A", CapacityBytes: 100 << 30, FreeBytes: 60 << 30, Tier: model.TierHot}
	if err := s.SaveDisk(d); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateFree("SSD-A", 10<<30); err != nil {
		t.Fatal(err)
	}
	got, _, _ := s.GetDisk("SSD-A")
	if got.FreeBytes != 10<<30 {
		t.Errorf("free not updated: %d", got.FreeBytes)
	}
}

func TestClaimOrTouchDisk(t *testing.T) {
	s := newTestStore(t)
	// 起始：无主、在线时长为 0
	if err := s.SaveDisk(model.Disk{DiskSerial: "D1", Tier: model.TierHot, LastSeenAt: 1000}); err != nil {
		t.Fatal(err)
	}
	grace := int64(3600)

	// 节点 A 认领：应成功，挂载到 A，在线时长从 now-lastSeen 累加 (5000-1000=4000)
	d, err := s.ClaimOrTouchDisk("D1", "node-A", 5000, grace)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if d.MountedNodeID != "node-A" {
		t.Errorf("expected mounted by node-A, got %s", d.MountedNodeID)
	}
	if d.OnlineSeconds != 4000 {
		t.Errorf("expected online 4000, got %d", d.OnlineSeconds)
	}

	// 续占：再次 touch，now=5600 → 累加 600 → 4600
	d2, err := s.ClaimOrTouchDisk("D1", "node-A", 5600, grace)
	if err != nil {
		t.Fatalf("touch: %v", err)
	}
	if d2.OnlineSeconds != 4600 {
		t.Errorf("expected online 4600, got %d", d2.OnlineSeconds)
	}

	// 另一在线节点 B（lastSeen 很新）尝试认领 → 应失败
	if err := s.SaveDisk(model.Disk{DiskSerial: "D2", Tier: model.TierHot, MountedNodeID: "node-B", LastSeenAt: 5600}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimOrTouchDisk("D2", "node-A", 5700, grace); err == nil {
		t.Error("claim by other online node should fail")
	}

	// 节点 B 已离线超 grace：A 可重新认领（§11.3 重新认领）
	if err := s.SaveDisk(model.Disk{DiskSerial: "D3", Tier: model.TierHot, MountedNodeID: "node-B", LastSeenAt: 1000}); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := s.ClaimOrTouchDisk("D3", "node-A", 6000, grace)
	if err != nil {
		t.Fatalf("reclaim offline disk: %v", err)
	}
	if reclaimed.MountedNodeID != "node-A" {
		t.Errorf("expected reclaimed by node-A, got %s", reclaimed.MountedNodeID)
	}
}

func TestListDisks(t *testing.T) {
	s := newTestStore(t)
	_ = s.SaveDisk(model.Disk{DiskSerial: "A", Tier: model.TierHot})
	_ = s.SaveDisk(model.Disk{DiskSerial: "B", Tier: model.TierCold})
	disks, err := s.ListDisks()
	if err != nil {
		t.Fatal(err)
	}
	if len(disks) != 2 {
		t.Fatalf("expected 2 disks, got %d", len(disks))
	}
}

func TestReplicaAndHealthyCount(t *testing.T) {
	s := newTestStore(t)
	// 两块盘：A 健康，B 可疑
	_ = s.SaveDisk(model.Disk{DiskSerial: "A", Tier: model.TierHot})
	_ = s.SaveDisk(model.Disk{DiskSerial: "B", Tier: model.TierCold, Suspect: true})
	_ = s.RegisterReplica("asset-1", "A", "c1")
	_ = s.RegisterReplica("asset-1", "B", "c1")

	reps, err := s.ListReplicas("asset-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(reps) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(reps))
	}
	// 健康副本只应数到 A（B 可疑）
	n, err := s.CountHealthyReplicas("asset-1")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 healthy replica, got %d", n)
	}
}

func TestRegisterReplicaStatus(t *testing.T) {
	s := newTestStore(t)
	_ = s.SaveDisk(model.Disk{DiskSerial: "A", Tier: model.TierHot})
	if err := s.RegisterReplica("a1", "A", "c1"); err != nil {
		t.Fatal(err)
	}
	reps, _ := s.ListReplicas("a1")
	if len(reps) != 1 || reps[0].Status != "PENDING" {
		t.Errorf("replica should be PENDING: %+v", reps)
	}
}

func TestDirectoryRoundTrip(t *testing.T) {
	s := newTestStore(t)
	dir := model.Directory{DirKey: "2026-06", NodeID: "node-A", DiskSerial: "A", Tier: model.TierWarm, Temperature: 0.5, TotalBytes: 123, AccessScore: 0.7}
	if err := s.SaveDirectory(dir); err != nil {
		t.Fatal(err)
	}
	dirs, err := s.ListDirectories()
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 1 || dirs[0].DirKey != "2026-06" || dirs[0].Tier != model.TierWarm {
		t.Errorf("directory mismatch: %+v", dirs)
	}
}

// TestSaveDirectoryLWW 验证目录放置图的 LWW upsert（§8.6 控制面）：
// 仅当传入 last_eval_at 严格更大时才覆盖既有记录；较旧或相等时间戳的记录
// 不得回退/冲掉已持有的较新放置。这正是节点下线后聚合持久化时，对端发来的
// 陈旧副本不会破坏本地正确放置的基础保证。
func TestSaveDirectoryLWW(t *testing.T) {
	s := newTestStore(t)
	const key = "2026/06"

	// 1) 较旧(100)先、较新(200)后 → 较新覆盖较旧
	if err := s.SaveDirectory(model.Directory{DirKey: key, NodeID: "B", DiskSerial: "DB-old", LastEvalAt: 100}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveDirectory(model.Directory{DirKey: key, NodeID: "B", DiskSerial: "DB-new", LastEvalAt: 200}); err != nil {
		t.Fatal(err)
	}
	d, ok, err := s.GetDirectory(key)
	if err != nil || !ok {
		t.Fatalf("GetDirectory: ok=%v err=%v", ok, err)
	}
	if d.DiskSerial != "DB-new" {
		t.Fatalf("newer ts should win, got %+v", d)
	}

	// 2) 较旧(150)再来 → 不得回退覆盖较新(200)
	if err := s.SaveDirectory(model.Directory{DirKey: key, NodeID: "B", DiskSerial: "DB-stale", LastEvalAt: 150}); err != nil {
		t.Fatal(err)
	}
	d2, _, _ := s.GetDirectory(key)
	if d2.DiskSerial != "DB-new" {
		t.Fatalf("stale (150) must not overwrite newer (200): got %+v", d2)
	}

	// 3) 相等(200)再来 → 严格 > 不满足，仍保留较新内容
	if err := s.SaveDirectory(model.Directory{DirKey: key, NodeID: "B", DiskSerial: "DB-eq", LastEvalAt: 200}); err != nil {
		t.Fatal(err)
	}
	d3, _, _ := s.GetDirectory(key)
	if d3.DiskSerial != "DB-new" {
		t.Fatalf("equal ts (200) must not overwrite (strict >): got %+v", d3)
	}

	// 4) 严格更大(300)再来 → 覆盖
	if err := s.SaveDirectory(model.Directory{DirKey: key, NodeID: "B", DiskSerial: "DB-fresh", LastEvalAt: 300}); err != nil {
		t.Fatal(err)
	}
	d4, _, _ := s.GetDirectory(key)
	if d4.DiskSerial != "DB-fresh" {
		t.Fatalf("strictly larger ts should overwrite, got %+v", d4)
	}
}

func TestSubmitTaskIdempotent(t *testing.T) {
	s := newTestStore(t)
	task := clusterapi.Task{TaskID: "t1", Type: "MIGRATION", DirKey: "2026-06", SrcDisk: "A", DstDisk: "B"}
	if err := s.SubmitTask(task); err != nil {
		t.Fatal(err)
	}
	if err := s.SubmitTask(task); err != nil { // 重复应被忽略
		t.Fatal(err)
	}
	// 无报错即视为幂等成功（INSERT OR IGNORE）
}

func TestGetStateImplementsProvider(t *testing.T) {
	s := newTestStore(t)
	_ = s.SaveDisk(model.Disk{DiskSerial: "A", Tier: model.TierHot, FreeBytes: 1 << 30, MountedNodeID: "node-A"})
	p := s.GetState()
	if p.NodeID != "node-A" {
		t.Errorf("wrong node id: %s", p.NodeID)
	}
	if len(p.Disks) != 1 || p.Disks[0].DiskSerial != "A" {
		t.Errorf("state disks wrong: %+v", p.Disks)
	}
	if loc, ok := s.GetDiskLocation("A"); !ok || loc != "node-A" {
		t.Errorf("location wrong: %q %v", loc, ok)
	}
	if _, ok := s.GetDiskLocation("ZZZ"); ok {
		t.Error("unknown disk should not be found")
	}
}
