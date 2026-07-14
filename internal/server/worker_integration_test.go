package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/coordinator"
	"github.com/weijia/immich-go-server/internal/model"
)

// newNodeWith 创建带指定 ID / blob 根目录的测试节点。
func newNodeWith(t *testing.T, id string) *Node {
	t.Helper()
	dir := t.TempDir()
	db := filepath.Join(dir, "s.db")
	blob := filepath.Join(dir, "blob")
	if err := os.Mkdir(blob, 0o755); err != nil {
		t.Fatal(err)
	}
	n, err := New(Config{
		NodeID:    id,
		Secret:    "sec",
		ListenAddr: "127.0.0.1:0",
		BlobRoot:  blob,
		DBPath:    db,
	})
	if err != nil {
		t.Fatalf("New %s: %v", id, err)
	}
	return n
}

// TestNodeWorkerCrossNode 端到端验证：
//  1. 协调者 A 产出跨节点 MIGRATION 任务（dir d1 从 DA 迁到远端 DB），经 GlobalRepo 路由到 B；
//  2. 节点 B 的 worker 以“拉模型”从 A 经 HMAC blob 端点拉取字节写入本地，登记副本并更新目录盘。
func TestNodeWorkerCrossNode(t *testing.T) {
	a := newNodeWith(t, "A")
	b := newNodeWith(t, "B")
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()
	go func() { _ = b.Run(ctx) }()
	addrA := waitAddr(t, a)
	addrB := waitAddr(t, b)

	// --- 节点 A（协调者候选人）：持有源盘 DA 与目录 d1 的资产/副本 ---
	if err := a.Store().SaveDisk(model.Disk{DiskSerial: "DA", Tier: model.TierWarm, MountedNodeID: "A", OnlineSeconds: 900, FreeBytes: 50 << 30}); err != nil {
		t.Fatal(err)
	}
	if err := a.Store().SaveDirectory(model.Directory{DirKey: "d1", NodeID: "A", DiskSerial: "DA", Tier: model.TierWarm, Temperature: 0.9, TotalBytes: 100}); err != nil {
		t.Fatal(err)
	}
	for id, content := range map[string]string{"a1": "hello-from-A-a1", "a2": "hello-from-A-a2"} {
		if err := a.Store().SaveAsset(model.Asset{AssetID: id, SizeBytes: int64(len(content)), Checksum: "c-" + id, DirKey: "d1"}); err != nil {
			t.Fatal(err)
		}
		if err := a.Store().AddReplica(model.Replica{ReplicaID: id + "@DA", AssetID: id, DiskSerial: "DA", NodeID: "A", Checksum: "c-" + id, Status: "HEALTHY"}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(a.cfg.BlobRoot, id), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// --- 节点 B：持有目标盘 DB；预置目录/资产元数据（联邦下副本目录会同步） ---
	if err := b.Store().SaveDisk(model.Disk{DiskSerial: "DB", Tier: model.TierHot, MountedNodeID: "B", OnlineSeconds: 100, FreeBytes: 90 << 30}); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().SaveDirectory(model.Directory{DirKey: "d1", NodeID: "A", DiskSerial: "DA", Tier: model.TierWarm, Temperature: 0.9, TotalBytes: 100}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a1", "a2"} {
		if err := b.Store().SaveAsset(model.Asset{AssetID: id, SizeBytes: 12, Checksum: "c-" + id, DirKey: "d1"}); err != nil {
			t.Fatal(err)
		}
	}

	// 互相注入发现地址：A 需知道 B（路由任务），B 需知道 A（拉取源字节）。
	a.Registry().Upsert("B", addrB, time.Now().Unix())
	b.Registry().Upsert("A", addrA, time.Now().Unix())

	// A 聚合并作为协调者下发任务（MinReplicas=1 避免额外 REPLICA 任务干扰）
	gvA, err := a.Federate(ctx)
	if err != nil {
		t.Fatalf("A.Federate: %v", err)
	}
	repoA := a.GlobalRepository(gvA)
	cfg := config.Default()
	cfg.MinReplicas = 1
	cond := coordinator.New(repoA, cfg)
	if emitted, err := cond.RunBalancingCycle(); err != nil {
		t.Fatalf("balance: %v", err)
	} else if emitted != 1 {
		t.Fatalf("expected 1 migration task, got %d", emitted)
	}

	// B 拉取全局视图并执行落在本地的任务（dst=DB 在 B）
	gvB, err := b.Federate(ctx)
	if err != nil {
		t.Fatalf("B.Federate: %v", err)
	}
	if err := b.Worker(gvB).RunOnce(ctx); err != nil {
		t.Fatalf("B.Worker: %v", err)
	}

	// 断言 1：字节已跨节点搬到 B 的 BlobRoot，且内容一致
	for id, want := range map[string]string{"a1": "hello-from-A-a1", "a2": "hello-from-A-a2"} {
		got, err := os.ReadFile(filepath.Join(b.cfg.BlobRoot, id))
		if err != nil {
			t.Fatalf("B missing blob %s: %v", id, err)
		}
		if string(got) != want {
			t.Errorf("blob %s content mismatch: got %q want %q", id, got, want)
		}
	}
	// 断言 2：B 已登记目标副本（HEALTHY）
	for _, id := range []string{"a1", "a2"} {
		reps, _ := b.Store().ListReplicas(id)
		found := false
		for _, r := range reps {
			if r.DiskSerial == "DB" && r.Status == "HEALTHY" {
				found = true
			}
		}
		if !found {
			t.Errorf("B missing HEALTHY replica of %s on DB: %+v", id, reps)
		}
	}
	// 断言 3：B 的目录 d1 归属盘已更新为 DB
	dir, ok, _ := b.Store().GetDirectory("d1")
	if !ok {
		t.Fatal("B directory d1 missing")
	}
	if dir.DiskSerial != "DB" {
		t.Errorf("B directory d1 disk = %s, want DB", dir.DiskSerial)
	}
	// 断言 4：任务在 B 已标记为 DONE
	tasks, _ := b.Store().ListTasks()
	if len(tasks) != 1 || tasks[0].Status != "DONE" {
		t.Fatalf("B task status unexpected: %+v", tasks)
	}
}

// TestNodeWorkerReplica 单独验证跨节点 REPLICA：资产 a3 在 A 有健康副本，
// B 经任务补一份到本地 DB（从 A 拉取字节）。
func TestNodeWorkerReplica(t *testing.T) {
	a := newNodeWith(t, "A")
	b := newNodeWith(t, "B")
	defer a.Close()
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()
	go func() { _ = b.Run(ctx) }()
	addrA := waitAddr(t, a)

	// A 持有源副本 + 字节
	if err := a.Store().SaveDisk(model.Disk{DiskSerial: "DA", Tier: model.TierWarm, MountedNodeID: "A", OnlineSeconds: 900}); err != nil {
		t.Fatal(err)
	}
	if err := a.Store().SaveAsset(model.Asset{AssetID: "a3", SizeBytes: 11, Checksum: "c-a3", DirKey: "d2"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Store().AddReplica(model.Replica{ReplicaID: "a3@DA", AssetID: "a3", DiskSerial: "DA", NodeID: "A", Checksum: "c-a3", Status: "HEALTHY"}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.cfg.BlobRoot, "a3"), []byte("replica-src-A"), 0o644); err != nil {
		t.Fatal(err)
	}

	// B 持有目标盘 DB，并预置资产元数据 + 源副本目录（联邦下副本目录同步）
	if err := b.Store().SaveDisk(model.Disk{DiskSerial: "DB", Tier: model.TierHot, MountedNodeID: "B", OnlineSeconds: 100}); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().SaveAsset(model.Asset{AssetID: "a3", SizeBytes: 11, Checksum: "c-a3", DirKey: "d2"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Store().AddReplica(model.Replica{ReplicaID: "a3@DA", AssetID: "a3", DiskSerial: "DA", NodeID: "A", Checksum: "c-a3", Status: "HEALTHY"}); err != nil {
		t.Fatal(err)
	}

	b.Registry().Upsert("A", addrA, time.Now().Unix())

	// 直接把 REPLICA 任务注入 B 的本地库（模拟路由到目标盘所在节点）
	if err := b.Store().SubmitTask(clusterapi.Task{TaskID: "rep-a3", Type: "REPLICA", AssetID: "a3", DstDisk: "DB"}); err != nil {
		t.Fatal(err)
	}

	gvB, err := b.Federate(ctx)
	if err != nil {
		t.Fatalf("B.Federate: %v", err)
	}
	if err := b.Worker(gvB).RunOnce(ctx); err != nil {
		t.Fatalf("B.Worker: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(b.cfg.BlobRoot, "a3"))
	if err != nil {
		t.Fatalf("B missing replica blob a3: %v", err)
	}
	if string(got) != "replica-src-A" {
		t.Errorf("replica content = %q, want replica-src-A", got)
	}
	reps, _ := b.Store().ListReplicas("a3")
	found := false
	for _, r := range reps {
		if r.DiskSerial == "DB" && r.Status == "HEALTHY" {
			found = true
		}
	}
	if !found {
		t.Errorf("B missing HEALTHY replica a3 on DB: %+v", reps)
	}
}
