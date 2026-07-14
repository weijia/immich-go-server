package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/weijia/immich-go-server/internal/cluster"
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

	// --- 节点 B：持有目标盘 DB；预置资产元数据（联邦下副本目录会同步）但不预置目录记录，
	//     以诚实验证“目录跨节点重宿主”——B 需从 A 拉取目录元数据并领养，再让 A 放弃旧记录。 ---
	if err := b.Store().SaveDisk(model.Disk{DiskSerial: "DB", Tier: model.TierHot, MountedNodeID: "B", OnlineSeconds: 100, FreeBytes: 90 << 30}); err != nil {
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
	// 断言 3：B 已领养目录 d1 为权威记录（node_id=B、归属盘=DB）
	dir, ok, _ := b.Store().GetDirectory("d1")
	if !ok {
		t.Fatal("B directory d1 missing after rehost")
	}
	if dir.NodeID != "B" {
		t.Errorf("B directory d1 nodeId = %s, want B", dir.NodeID)
	}
	if dir.DiskSerial != "DB" {
		t.Errorf("B directory d1 disk = %s, want DB", dir.DiskSerial)
	}
	// 断言 4：源节点 A 已放弃陈旧目录记录（跨节点重宿主）
	if _, ok, _ := a.Store().GetDirectory("d1"); ok {
		t.Errorf("A still has stale directory d1 after rehost")
	}
	// 断言 4b（真实源盘释放）：A 上 DA 盘的源副本记录已删除
	for _, id := range []string{"a1", "a2"} {
		reps, _ := a.Store().ListReplicas(id)
		for _, r := range reps {
			if r.DiskSerial == "DA" {
				t.Errorf("A still has source replica %s@DA after release", id)
			}
		}
	}
	// 断言 4c（真实源盘释放）：A 上 DA 盘的物理字节已删除
	for _, id := range []string{"a1", "a2"} {
		if _, err := os.Stat(filepath.Join(a.cfg.BlobRoot, id)); !os.IsNotExist(err) {
			t.Errorf("A blob %s should be released (deleted), stat err=%v", id, err)
		}
	}
	// 断言 5：任务在 B 已标记为 DONE
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

// TestNodeDirectoryRehostAPI 直接验证目录重宿主的两个集群端点：
//  1. GET  /api/cluster/directory/<dirKey> 能拉取目录元数据，对缺失目录返回 404（ok=false）；
//  2. POST /api/cluster/directory/rehost 让源节点放弃其陈旧目录记录（数据已迁走）。
func TestNodeDirectoryRehostAPI(t *testing.T) {
	a := newNodeWith(t, "A")
	defer a.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()
	addrA := waitAddr(t, a)

	if err := a.Store().SaveDirectory(model.Directory{DirKey: "d9", NodeID: "A", DiskSerial: "DA", Tier: model.TierWarm, Temperature: 0.5, TotalBytes: 42}); err != nil {
		t.Fatal(err)
	}

	client := cluster.NewClient("B", "sec", 300)

	// 拉取存在的目录
	got, ok, err := client.GetDirectory(ctx, "http://"+addrA, "d9")
	if err != nil {
		t.Fatalf("GetDirectory: %v", err)
	}
	if !ok {
		t.Fatal("expected d9 on A")
	}
	if got.DiskSerial != "DA" || got.TotalBytes != 42 {
		t.Errorf("unexpected directory: %+v", got)
	}

	// 拉取缺失目录返回 404 / ok=false
	if _, ok, err := client.GetDirectory(ctx, "http://"+addrA, "missing"); err != nil {
		t.Fatalf("GetDirectory missing: %v", err)
	} else if ok {
		t.Error("expected missing directory to be not found")
	}

	// 重宿主：通知 A 放弃 d9
	if err := client.RehostDirectory(ctx, "http://"+addrA, "d9", "A"); err != nil {
		t.Fatalf("RehostDirectory: %v", err)
	}
	if _, ok, _ := a.Store().GetDirectory("d9"); ok {
		t.Error("A should have relinquished directory d9")
	}

	// 非源节点收到同一请求应忽略（幂等对称）：用 B 当源节点，A 不应受影响（已无记录）
	if err := client.RehostDirectory(ctx, "http://"+addrA, "d9", "B"); err != nil {
		t.Fatalf("RehostDirectory (no-op): %v", err)
	}
}
