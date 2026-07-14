package coordinator

import (
	"github.com/weijia/immich-go-server/internal/balancer"
	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/model"
)

// Repository 是 Coordinator 决策所需的数据来源抽象（§6 / §7 / §9.2）。
// 真实实现为 *store.Store；测试用内存 fake。SubmitTask 幂等（INSERT OR IGNORE）。
type Repository interface {
	ListDirectories() ([]model.Directory, error)
	ListDisks() ([]model.Disk, error)
	ListAssets() ([]model.Asset, error)
	ListReplicas(assetID string) ([]model.Replica, error)
	ReplicaCount(assetID string) int
	SubmitTask(task clusterapi.Task) error
}

// Coordinator 决策引擎：周期性评估并产生迁移/补副本任务（§6.4 / §7.2）。
// 自身无状态，任务下发依赖 Repository 幂等，可安全重复运行（脑裂期亦安全，§10.4）。
type Coordinator struct {
	repo Repository
	cfg  config.Config
}

// New 构造 Coordinator。
func New(repo Repository, cfg config.Config) *Coordinator {
	return &Coordinator{repo: repo, cfg: cfg}
}

// RunBalancingCycle 执行一轮均衡：
//  1. 对每个目录做 PlanMigration，需迁移则下发 MIGRATION 任务（§6.4）；
//  2. 对每个副本数不足的资产做 SelectReplicaTarget，下发 REPLICA 任务（§7.2）。
// 返回本轮下发的任务数（已存在的 task_id 被 INSERT OR IGNORE 去重）。
func (c *Coordinator) RunBalancingCycle() (int, error) {
	disks, err := c.repo.ListDisks()
	if err != nil {
		return 0, err
	}
	dirs, err := c.repo.ListDirectories()
	if err != nil {
		return 0, err
	}
	assets, err := c.repo.ListAssets()
	if err != nil {
		return 0, err
	}

	emitted := 0

	// 1) 迁移决策
	for _, dir := range dirs {
		plan, ok := balancer.PlanMigration(dir, disks, c.cfg)
		if !ok {
			continue
		}
		task := clusterapi.Task{
			TaskID:  "mig-" + dir.DirKey,
			Type:    "MIGRATION",
			DirKey:  dir.DirKey,
			SrcDisk: plan.FromDisk,
			DstDisk: plan.ToDisk,
		}
		if err := c.repo.SubmitTask(task); err != nil {
			return emitted, err
		}
		emitted++
	}

	// 2) 补副本决策
	for _, a := range assets {
		if c.repo.ReplicaCount(a.AssetID) >= c.cfg.MinReplicas {
			continue
		}
		existing, err := c.repo.ListReplicas(a.AssetID)
		if err != nil {
			return emitted, err
		}
		target, ok := balancer.SelectReplicaTarget(a, existing, disks, c.cfg)
		if !ok {
			continue // 无可用目标盘，下轮再试
		}
		task := clusterapi.Task{
			TaskID:  "rep-" + a.AssetID,
			Type:    "REPLICA",
			AssetID: a.AssetID,
			DstDisk: target.DiskSerial,
		}
		if err := c.repo.SubmitTask(task); err != nil {
			return emitted, err
		}
		emitted++
	}

	return emitted, nil
}
