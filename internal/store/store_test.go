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
