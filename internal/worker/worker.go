// Package worker 消费 Store 中的集群任务并执行（§6.5 / §7.2 / §9）。
//
// 拓扑采用"拉模型"：Coordinator 把任务下发给 dstDisk 实际挂载的节点
// （见 cluster.GlobalRepo.SubmitTask），因此每个节点只执行目标盘在本地的任务。
// 源字节可来自本节点（同一或不同磁盘 blob 根）或经 HMAC 鉴权的远端 blob 端点拉取
// （migrationexec.RemoteSource）。
//
// 物理路径模型（§仓库即真相）：assets 位于 <disk.BlobRoot>/<dirKey>/<assetID>。
// 每磁盘一个仓库，摄入时已按 dirKey 分桶存放；worker 按 disk 解析 blob_root、
// 再拼接 dirKey/assetID 定位物理字节。
package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
	DeleteReplica(assetID, diskSerial string) error // 真实源盘释放：删源副本记录
	SaveDirectory(dir model.Directory) error       // 目录重宿主：领养权威记录
	GetDiskLocation(serial string) (string, bool)
	DiskRoot(serial string) (string, bool) // 返回磁盘物理仓库根 (§仓库即真相)
}

// DiskLocator 解析磁盘挂载节点与对端基址，供跨节点拉取字节。
type DiskLocator interface {
	// DiskNode 返回磁盘当前挂载节点；ok=false 表示未知。
	DiskNode(serial string) (nodeID string, ok bool)
	// PeerURL 返回节点基址（http://host:port）；ok=false 表示本节点/未知。
	PeerURL(nodeID string) (string, bool)
}

// Worker 执行本节点待办任务（目标盘在本地）。
// 物理路径通过 Repo.DiskRoot(serial) 按磁盘解析 blob_root，而非单一 BlobBase。
type Worker struct {
	NodeID string
	Secret string
	Repo   Repo
	Loc    DiskLocator
	Client *cluster.Client
	Cfg    config.Config
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
// 完成后把目录重宿主到本节点（领养权威记录 + 通知源节点放弃旧记录）。
func (w *Worker) runMigration(ctx context.Context, t clusterapi.Task) error {
	// 源盘挂载节点（用于跨节点拉取字节与重宿主通知）。
	srcNode, _ := w.Loc.DiskNode(t.SrcDisk)

	// 取权威目录元数据：优先本地；本地缺失（目录已在源节点）则从源节点拉取。
	dir, ok, err := w.Repo.GetDirectory(t.DirKey)
	if err != nil {
		return err
	}
	if !ok {
		if srcNode != "" && srcNode != w.NodeID {
			url, ok := w.Loc.PeerURL(srcNode)
			if !ok {
				return fmt.Errorf("no peer url for src node %s", srcNode)
			}
			dir, ok, err = w.Client.GetDirectory(ctx, url, t.DirKey)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("directory %s not found on source node %s", t.DirKey, srcNode)
			}
		} else {
			return fmt.Errorf("directory %s not found", t.DirKey)
		}
	}

	assets, err := w.Repo.ListAssetsByDir(t.DirKey)
	if err != nil {
		return err
	}
	if len(assets) > 0 {
		if err := w.copyAssets(t.SrcDisk, t.DstDisk, assets); err != nil {
			return err
		}
		// 登记目标副本（HEALTHY）—— 在释放源之前确保目标已就绪
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
	}
	// 释放源盘：删除源副本记录与（跨节点时）物理字节
	if err := w.releaseSource(ctx, t, assets, srcNode); err != nil {
		return err
	}
	return w.rehost(ctx, t, dir, srcNode)
}

// rehost 把目录的权威记录重宿主到本节点（目标盘所在节点）：
//  1. 本地领养：把目录记录的 node_id/disk_serial 更新为本节点与 DstDisk，保留其余元数据；
//  2. 若源节点非本节点，通知其放弃（删除）陈旧的源目录记录。
func (w *Worker) rehost(ctx context.Context, t clusterapi.Task, dir model.Directory, srcNode string) error {
	dir.NodeID = w.NodeID
	dir.DiskSerial = t.DstDisk
	dir.LastEvalAt = time.Now().Unix() // 本地权威写入：刷新评估时刻（epoch 秒），LWW 胜出（§8.6 控制面）
	if err := w.Repo.SaveDirectory(dir); err != nil {
		return err
	}
	if srcNode != "" && srcNode != w.NodeID {
		url, ok := w.Loc.PeerURL(srcNode)
		if !ok {
			return fmt.Errorf("no peer url for src node %s", srcNode)
		}
		if err := w.Client.RehostDirectory(ctx, url, t.DirKey, srcNode); err != nil {
			return err
		}
	}
	return nil
}

// releaseSource 迁移完成后释放源盘（§9.x 真实源盘释放）：
//  1. 仅释放"删源后仍满足 MinReplicas"的资产（safeToRelease）；其余保留源副本；
//  2. 删除源物理字节——按 per-disk 仓库路径 <blobRoot>/<dirKey>/<assetID> 定位；
//     同节点同根时共享同一文件，删字节会误伤目标副本，故保留。
// 源在本节点时本地直接删除；源在远端时经 HMAC 通知源节点代为释放。
func (w *Worker) releaseSource(ctx context.Context, t clusterapi.Task, assets []model.Asset, srcNode string) error {
	safe := make([]bool, len(assets))
	for i, a := range assets {
		safe[i] = w.safeToRelease(a.AssetID, t.SrcDisk)
	}

	if srcNode == "" || srcNode == w.NodeID {
		srcRoot, _ := w.Repo.DiskRoot(t.SrcDisk)
		for i, a := range assets {
			if !safe[i] {
				continue
			}
			if err := w.Repo.DeleteReplica(a.AssetID, t.SrcDisk); err != nil {
				return err
			}
			// 仅当 dst 不在本节点同根时才删物理字节（避免误删共享文件）
			if rd := w.sameDiskRoot(t.SrcDisk, t.DstDisk); !rd {
				if srcRoot != "" && a.DirKey != "" {
					_ = os.Remove(filepath.Join(srcRoot, a.DirKey, a.AssetID))
				}
			}
		}
		return nil
	}

	// 远端源节点：仅把可安全释放的资产交给它
	var toRelease []string
	for i, a := range assets {
		if safe[i] {
			toRelease = append(toRelease, a.AssetID)
		}
	}
	url, ok := w.Loc.PeerURL(srcNode)
	if !ok {
		return fmt.Errorf("no peer url for src node %s", srcNode)
	}
	return w.Client.ReleaseSource(ctx, url, t.DirKey, t.SrcDisk, t.DstDisk, srcNode, toRelease)
}

// sameDiskRoot 判断两个磁盘是否共享同一物理仓库根（用于避免误删共享文件）。
func (w *Worker) sameDiskRoot(a, b string) bool {
	ra, _ := w.Repo.DiskRoot(a)
	rb, _ := w.Repo.DiskRoot(b)
	return ra != "" && ra == rb
}

// safeToRelease 判断删除某资产在 SrcDisk 上的源副本后，剩余有效（HEALTHY）副本数
// 是否仍 ≥ MinReplicas。注意：DstDisk 副本已在 runMigration 中登记（HEALTHY），
// 故计数已包含目标副本。MinReplicas<=1 时恒为真（释放后至少保留刚登记的目标副本）。
// 无法确认（读副本失败）时保守返回 false，保留源副本。
func (w *Worker) safeToRelease(assetID, srcDisk string) bool {
	min := w.cfg().MinReplicas
	if min <= 1 {
		return true
	}
	reps, err := w.Repo.ListReplicas(assetID)
	if err != nil {
		return false
	}
	n := 0
	for _, r := range reps {
		if r.DiskSerial == srcDisk {
			continue // 即将被删除的源副本不计入
		}
		if r.Status == "HEALTHY" {
			n++
		}
	}
	return n >= min
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

// copyAssets 把资产字节搬到本节点 DstDisk。源可为本节点（同节点不同盘用 osBlobStore
// 内部拷贝；同盘同根则跳过）或远端节点（经 RemoteSource 拉取）。
// bytes locate at <blobRoot>/<dirKey>/<assetID> (§仓库即真相)。
func (w *Worker) copyAssets(srcDisk, dstDisk string, assets []model.Asset) error {
	if len(assets) == 0 {
		return nil
	}
	dirKey := assets[0].DirKey

	dstRoot, _ := w.Repo.DiskRoot(dstDisk)
	if dstRoot == "" {
		return fmt.Errorf("dst disk %s has no blob_root", dstDisk)
	}
	dstDir := filepath.Join(dstRoot, dirKey)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}

	srcNode, _ := w.Loc.DiskNode(srcDisk)
	if srcNode != "" && srcNode != w.NodeID {
		// 远端源：经 RemoteBlobStore 拉取，写入本地 dstDir
		url, ok := w.Loc.PeerURL(srcNode)
		if !ok {
			return fmt.Errorf("no peer url for src node %s", srcNode)
		}
		src := &migrationexec.RemoteSource{
			BaseURL: url,
			NodeID:  w.NodeID,
			Secret:  w.Secret,
			DiskSer: srcDisk,
			DirKey:  dirKey,
			Client:  w.Client.HTTPClient,
			Now:     w.Client.Now,
		}
		dstBS := migrationexec.NewOSBlobStore("", dstDir)
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

	// 源在本节点：src 盘与 dst 盘是否同一物理仓库根？
	srcRoot, _ := w.Repo.DiskRoot(srcDisk)
	srcDir := filepath.Join(srcRoot, dirKey)

	if srcDir == dstDir {
		// 同盘同根（或同节点多盘共享同一目录）：字节已就位，无需搬运。
		return nil
	}

	// 同节点不同盘（不同 blob_root）：用 osBlobStore 在源/目标间拷贝字节。
	dstBS := migrationexec.NewOSBlobStore(srcDir, dstDir)
	exec := migrationexec.NewExecutor(dstBS, w.cfg())
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

func (w *Worker) cfg() config.Config {
	if w.Cfg.MinReplicas == 0 {
		return config.Default()
	}
	return w.Cfg
}

// BlobPath 返回某资产在给定磁盘仓库中的物理路径（blobRoot/<dirKey>/<assetID>）。
func BlobPath(blobRoot, dirKey, assetID string) string {
	return filepath.Join(blobRoot, dirKey, assetID)
}
