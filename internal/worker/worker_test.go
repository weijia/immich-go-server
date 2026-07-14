package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/model"
	"github.com/weijia/immich-go-server/internal/store"
)

// localLocator 测试用：所有磁盘都在本节点（NodeID="X"），无远端。
type localLocator struct{}

func (localLocator) DiskNode(serial string) (string, bool) { return "X", true }
func (localLocator) PeerURL(nodeID string) (string, bool)  { return "", false }

// TestWorkerLocalMigration 同节点迁移：源/目标盘均在本节点，字节已在 BlobBase，
// worker 仅更新目录归属盘并登记目标副本（拉模型下本节点无需字节搬运）。
func TestWorkerLocalMigration(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "s.db")
	blob := filepath.Join(dir, "blob")
	if err := os.Mkdir(blob, 0o755); err != nil {
		t.Fatal(err)
	}
	st, err := store.NewStore(db, "X")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if err := st.SaveDisk(model.Disk{DiskSerial: "DA", Tier: model.TierWarm, MountedNodeID: "X"}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveDisk(model.Disk{DiskSerial: "DB", Tier: model.TierHot, MountedNodeID: "X"}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveDirectory(model.Directory{DirKey: "d1", NodeID: "X", DiskSerial: "DA", Tier: model.TierWarm}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveAsset(model.Asset{AssetID: "a1", SizeBytes: 3, Checksum: "c1", DirKey: "d1"}); err != nil {
		t.Fatal(err)
	}
	if err := st.AddReplica(model.Replica{ReplicaID: "a1@DA", AssetID: "a1", DiskSerial: "DA", NodeID: "X", Checksum: "c1", Status: "HEALTHY"}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blob, "a1"), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := st.SubmitTask(clusterapi.Task{TaskID: "mig-d1", Type: "MIGRATION", DirKey: "d1", SrcDisk: "DA", DstDisk: "DB"}); err != nil {
		t.Fatal(err)
	}

	w := &Worker{
		NodeID:   "X",
		Secret:   "sec",
		Repo:     st,
		BlobBase: blob,
		Loc:      localLocator{},
		Client:   nil, // 本节点路径不会用到远端客户端
		Cfg:      config.Default(),
	}
	if err := w.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	dir2, ok, _ := st.GetDirectory("d1")
	if !ok || dir2.DiskSerial != "DB" {
		t.Fatalf("directory disk not updated: ok=%v disk=%q", ok, dir2.DiskSerial)
	}
	reps, _ := st.ListReplicas("a1")
	found := false
	for _, r := range reps {
		if r.DiskSerial == "DB" && r.Status == "HEALTHY" {
			found = true
		}
	}
	if !found {
		t.Errorf("missing HEALTHY replica on DB: %+v", reps)
	}
	tasks, _ := st.ListTasks()
	if len(tasks) != 1 || tasks[0].Status != "DONE" {
		t.Fatalf("task status unexpected: %+v", tasks)
	}
}
