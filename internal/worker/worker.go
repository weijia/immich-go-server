// Package worker 消费 Store 中的集群任务并执行（§6.5 / §7.2 / §9）。
//
// 拓扑采用“拉模型”：Coordinator 把任务下发给 dstDisk 实际挂载的节点
// （见 cluster.GlobalRepo.SubmitTask），因此每个节点只执行目标盘在本地的任务。
// 源字节可来自本节点（同一 BlobBase 已存在）或经 HMAC 鉴权的远端 blob 端点拉取
// （migrationexec.RemoteSource）。目标字节落在本节点 BlobBase/<assetID>。
package worker

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/weijia/immich-go-server/internal/cluster"
	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/migration"
	"github.com/weijia/immich-go-server/internal/migrationexec"
	"github.com/weijia/immich-go-server/internal/model"
)

// Repo 是 worker 执行任务所需的数据访问，由 *store.Store 满足。
type Repo interface {
	ListPendingTasks(status string) ([]clusterapi.Task, error)
	UpdateTaskStatus(taskID, status string) error
	GetDirectory(dirKey string) (model.Directory, bool, error)
	ListAssetsByDir(dirKey string) ([]model.Asset, error)
	GetAsset(assetID string) (model.Asset, bool, error)
	ListReplicas(assetID string) ([]model.Replica, error)
	AddReplica(r model.Replica) error
	UpdateDirectoryDisk(dirKey, diskSerial string) error
	GetDiskLocation(serial string) (string, bool)
}

// DiskLocator 解析磁盘挂载节点与对端基址，供跨节点拉取字节。
type DiskLocator interface {
	// DiskNode 返回磁盘当前挂载节点；ok=false 表示未知。
	DiskNode(serial string) (nodeID string, ok bool)
	// PeerURL 返回节点基址（http://host:port）；ok=false 表示本节点/未知。
	PeerURL(nodeID string) (string, bool)
}

// Worker 执行本节点待办任务（目标盘在本地）。
type Worker struct {
	NodeID   string
	Secret   string
	Repo     Repo
	BlobBase string // 本节点 blob 扁平根目录：BlobBase/<assetID>
	Loc      DiskLocator
	Client   *cluster.Client
	Cfg      config.Config
}

// RunOnce 认领全部 QUEUED 任务并依次执行，更新状态为 RUNNING→DONE/FAILED。
// 单任务失败不影响其余任务（幂等：重复执行安全）。
func (w *Worker) RunOnce(ctx context.Context) error {
	tasks, err := w.Repo.ListPendingTasks("QUEUED")
	if err != nil {
		return err
	}
	for _, t := range tasks {
		_ = w.Repo.UpdateTaskStatus(t.TaskID, "RUNNING")
		if err := w.execute(ctx, t); err != nil {
			_ = w.Repo.UpdateTaskStatus(t.TaskID, "FAILED")
		} else {
			_ = w.Repo.UpdateTaskStatus(t.TaskID, "DONE")
		}
	}
	return nil
}

func (w *Worker) execute(ctx context.Context, t clusterapi.Task) error {
	switch t.Type {
	case "MIGRATION":
		return w.runMigration(ctx, t)
	case "REPLICA":
		return w.runReplica(ctx, t)
	default:
		return fmt.Errorf("unknown task type %q", t.Type)
	}
}

// runMigration 把目录下所有资产从 SrcDisk 搬到本节点 DstDisk。
// 源在本节点则无需字节搬运（仅更新元数据）；源在远端则经 RemoteSource 拉取。
func (w *Worker) runMigration(ctx context.Context, t clusterapi.Task) error {
	if _, ok, err := w.Repo.GetDirectory(t.DirKey); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("directory %s not found", t.DirKey)
	}
	assets, err := w.Repo.ListAssetsByDir(t.DirKey)
	if err != nil {
		return err
	}
	if len(assets) == 0 {
		// 空目录：直接更新归属盘
		return w.Repo.UpdateDirectoryDisk(t.DirKey, t.DstDisk)
	}
	if err := w.copyAssets(t.SrcDisk, t.DstDisk, assets); err != nil {
		return err
	}
	// 登记目标副本（HEALTHY）并更新目录归属盘
	for _, a := range assets {
		if err := w.Repo.AddReplica(model.Replica{
			ReplicaID:  a.AssetID + "@" + t.DstDisk,
			AssetID:    a.AssetID,
			DiskSerial: t.DstDisk,
			NodeID:     w.NodeID,
			Checksum:   a.Checksum,
			Status:     "HEALTHY",
		}); err != nil {
			return err
		}
	}
	return w.Repo.UpdateDirectoryDisk(t.DirKey, t.DstDisk)
}

// runReplica 为某资产在本地 DstDisk 补一份副本；源取一份健康副本所在盘（本节点或远端）。
func (w *Worker) runReplica(ctx context.Context, t clusterapi.Task) error {
	a, ok, err := w.Repo.GetAsset(t.AssetID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("asset %s not found", t.AssetID)
	}
	reps, err := w.Repo.ListReplicas(t.AssetID)
	if err != nil {
		return err
	}
	srcDisk := ""
	for _, r := range reps {
		if r.Status == "HEALTHY" {
			srcDisk = r.DiskSerial
			break
		}
	}
	if srcDisk == "" {
		return fmt.Errorf("no healthy replica for asset %s", t.AssetID)
	}
	if err := w.copyAssets(srcDisk, t.DstDisk, []model.Asset{a}); err != nil {
		return err
	}
	return w.Repo.AddReplica(model.Replica{
		ReplicaID:  a.AssetID + "@" + t.DstDisk,
		AssetID:    a.AssetID,
		DiskSerial: t.DstDisk,
		NodeID:     w.NodeID,
		Checksum:   a.Checksum,
		Status:     "HEALTHY",
	})
}

// copyAssets 把资产字节搬到本节点 DstDisk 的 BlobBase。src 为远端时经 RemoteSource 拉取；
// src 为本节点（同一 BlobBase 已有字节）则跳过字节搬运。
func (w *Worker) copyAssets(srcDisk, dstDisk string, assets []model.Asset) error {
	dstBS := migrationexec.NewOSBlobStore("", w.BlobBase) // 仅作为目标：写 BlobBase/<assetID>

	srcNode, _ := w.Loc.DiskNode(srcDisk)
	if srcNode != "" && srcNode != w.NodeID {
		url, ok := w.Loc.PeerURL(srcNode)
		if !ok {
			return fmt.Errorf("no peer url for src node %s", srcNode)
		}
		src := &migrationexec.RemoteSource{
			BaseURL: url,
			NodeID:  w.NodeID,
			Secret:  w.Secret,
			Client:  w.Client.HTTPClient,
			Now:     w.Client.Now,
		}
		bs := migrationexec.NewRemoteBlobStore(src, dstBS)
		exec := migrationexec.NewExecutor(bs, w.cfg())
		m, err := exec.Start(srcDisk, dstDisk, assets)
		if err != nil {
			return err
		}
		st, err := exec.Run(&m, assets)
		if err != nil {
			return err
		}
		if st != migration.StateVerified {
			return fmt.Errorf("copy %s->%s not verified: state=%s", srcDisk, dstDisk, st)
		}
		return nil
	}
	// 源在本节点：字节已存在于 BlobBase/<assetID>，无需搬运（同节点盘间迁移=仅元数据）。
	return nil
}

func (w *Worker) cfg() config.Config {
	if w.Cfg.MinReplicas == 0 {
		return config.Default()
	}
	return w.Cfg
}

// BlobPath 返回某资产在本节点的物理路径（BlobBase/<assetID>）。
func BlobPath(blobBase, assetID string) string {
	return filepath.Join(blobBase, assetID)
}
