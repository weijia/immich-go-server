package coordinator

import (
	"path/filepath"
	"testing"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/model"
	"github.com/weijia/immich-go-server/internal/store"
)

// fakeRepo 内存 Repository，用于纯逻辑单测。
type fakeRepo struct {
	dirs    []model.Directory
	disks   []model.Disk
	assets  []model.Asset
	replicas map[string][]model.Replica
	counts  map[string]int
	tasks   []clusterapi.Task
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
			return nil // 幂等去重
		}
	}
	f.tasks = append(f.tasks, t)
	return nil
}

func hotDisk(serial, node string) model.Disk {
	return model.Disk{DiskSerial: serial, Tier: model.TierHot, CapacityBytes: 100 << 30, FreeBytes: 60 << 30, MountedNodeID: node, OnlineSeconds: 900}
}
func coldDisk(serial, node string) model.Disk {
	return model.Disk{DiskSerial: serial, Tier: model.TierCold, CapacityBytes: 100 << 30, FreeBytes: 90 << 30, MountedNodeID: node, OnlineSeconds: 100}
}

func TestCycleEmitsMigration(t *testing.T) {
	cfg := config.Default()
	// 目录温度高(0.9)→应处 HOT，但当前在 COLD 盘，应迁移到 HOT 盘
	repo := &fakeRepo{
		disks: []model.Disk{coldDisk("SRC", "A"), hotDisk("DST", "B")},
		dirs:  []model.Directory{{DirKey: "2026-06", DiskSerial: "SRC", Tier: model.TierCold, Temperature: 0.9, TotalBytes: 10 << 30}},
	}
	c := New(repo, cfg)
	n, err := c.RunBalancingCycle()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 task, got %d", n)
	}
	task := repo.tasks[0]
	if task.Type != "MIGRATION" || task.SrcDisk != "SRC" || task.DstDisk != "DST" {
		t.Errorf("wrong migration task: %+v", task)
	}
}

func TestCycleEmitsReplica(t *testing.T) {
	cfg := config.Default()
	repo := &fakeRepo{
		disks:  []model.Disk{coldDisk("SRC", "A"), hotDisk("DST", "B")},
		assets: []model.Asset{{AssetID: "a1", SizeBytes: 5 << 30, DirKey: "2026-06"}},
		replicas: map[string][]model.Replica{
			"a1": {{AssetID: "a1", DiskSerial: "SRC", NodeID: "A"}},
		},
		counts: map[string]int{"a1": 1}, // < MinReplicas
	}
	c := New(repo, cfg)
	n, err := c.RunBalancingCycle()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 task, got %d", n)
	}
	task := repo.tasks[0]
	if task.Type != "REPLICA" || task.AssetID != "a1" || task.DstDisk != "DST" {
		t.Errorf("wrong replica task: %+v", task)
	}
}

func TestCycleNoTasksWhenHealthy(t *testing.T) {
	cfg := config.Default()
	repo := &fakeRepo{
		disks:  []model.Disk{hotDisk("DST", "B")},
		dirs:   []model.Directory{{DirKey: "2026-06", DiskSerial: "DST", Tier: model.TierHot, Temperature: 0.9, TotalBytes: 1 << 30}},
		assets: []model.Asset{{AssetID: "a1", SizeBytes: 1 << 30}},
		counts: map[string]int{"a1": 2}, // 已达标
	}
	c := New(repo, cfg)
	n, err := c.RunBalancingCycle()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 tasks, got %d (%+v)", n, repo.tasks)
	}
}

func TestCycleIdempotent(t *testing.T) {
	cfg := config.Default()
	repo := &fakeRepo{
		disks:  []model.Disk{coldDisk("SRC", "A"), hotDisk("DST", "B")},
		assets: []model.Asset{{AssetID: "a1", SizeBytes: 5 << 30}},
		counts: map[string]int{"a1": 1},
	}
	c := New(repo, cfg)
	first, _ := c.RunBalancingCycle()
	second, _ := c.RunBalancingCycle()
	if first != second || len(repo.tasks) != first {
		t.Errorf("cycle not idempotent: first=%d second=%d stored=%d", first, second, len(repo.tasks))
	}
}

// TestCycleAgainstStore 用真实 SQLite 验证 Store 满足 Repository 且端到端产出任务。
func TestCycleAgainstStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	st, err := store.NewStore(path, "node-A")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	_ = st.SaveDisk(coldDisk("SRC", "A"))
	_ = st.SaveDisk(hotDisk("DST", "B"))
	_ = st.SaveDirectory(model.Directory{DirKey: "2026-06", DiskSerial: "SRC", Tier: model.TierCold, Temperature: 0.9, TotalBytes: 10 << 30})
	_ = st.SaveAsset(model.Asset{AssetID: "a1", SizeBytes: 5 << 30, DirKey: "2026-06"})
	_ = st.AddReplica(model.Replica{ReplicaID: "a1@SRC", AssetID: "a1", DiskSerial: "SRC", NodeID: "A", Status: "HEALTHY"})

	c := New(st, config.Default())
	if _, err := c.RunBalancingCycle(); err != nil {
		t.Fatal(err)
	}
	tasks, err := st.ListTasks()
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks (migration+replica), got %d: %+v", len(tasks), tasks)
	}
	// 再次运行应幂等：仍是 2 条
	if _, err := c.RunBalancingCycle(); err != nil {
		t.Fatal(err)
	}
	tasks2, _ := st.ListTasks()
	if len(tasks2) != 2 {
		t.Fatalf("expected idempotent 2 tasks, got %d", len(tasks2))
	}
}

// 编译期断言 Store 满足 Repository。
var _ Repository = (*store.Store)(nil)
