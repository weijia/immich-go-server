package balancer

import (
	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/model"
	"github.com/weijia/immich-go-server/internal/space"
)

func tierRank(t model.Tier) int {
	switch t {
	case model.TierHot:
		return 3
	case model.TierWarm:
		return 2
	default:
		return 1
	}
}

// TargetTier 由目录温度决定其应处层（§6.4）。
func TargetTier(temp float64, cfg config.Config) model.Tier {
	if temp < cfg.ColdTempThr {
		return model.TierCold
	}
	if temp >= cfg.HotTempThr {
		return model.TierHot
	}
	return model.TierWarm
}

// MigrationGain 迁移收益 = |目标层rank - 当前层rank| × 目录字节（§6.4）。
func MigrationGain(current, target model.Tier, totalBytes int64) int64 {
	diff := tierRank(target) - tierRank(current)
	if diff < 0 {
		diff = -diff
	}
	return int64(diff) * totalBytes
}

func adjacent(t model.Tier) []model.Tier {
	switch t {
	case model.TierHot:
		return []model.Tier{model.TierWarm}
	case model.TierWarm:
		return []model.Tier{model.TierHot, model.TierCold}
	default:
		return []model.Tier{model.TierWarm}
	}
}

// MigrationPlan 一条整目录迁移计划。
type MigrationPlan struct {
	DirKey   string
	FromDisk string
	ToDisk   string
	Gain     int64
}

// PlanMigration 生成单条目录迁移计划（§6.4）：
// 当前层 == 应处层则不迁移；否则优先选目标层磁盘，无则放宽到相邻层。
func PlanMigration(dir model.Directory, candidates []model.Disk, cfg config.Config) (MigrationPlan, bool) {
	target := TargetTier(dir.Temperature, cfg)
	if target == dir.Tier {
		return MigrationPlan{}, false
	}
	if p, ok := selectTarget(dir, candidates, []model.Tier{target}, cfg); ok {
		return p, true
	}
	return selectTarget(dir, candidates, adjacent(target), cfg)
}

func selectTarget(dir model.Directory, candidates []model.Disk, tiers []model.Tier, cfg config.Config) (MigrationPlan, bool) {
	var cands []model.Disk
	for _, d := range candidates {
		if d.DiskSerial == dir.DiskSerial {
			continue // 不得选源目录所在盘（§6.4）
		}
		okTier := false
		for _, t := range tiers {
			if d.Tier == t {
				okTier = true
				break
			}
		}
		if !okTier {
			continue
		}
		if !space.PrecheckSpace(d, dir.TotalBytes, cfg) {
			continue
		}
		cands = append(cands, d)
	}
	if len(cands) == 0 {
		return MigrationPlan{}, false
	}
	best := cands[0]
	for _, d := range cands {
		if d.OnlineScore > best.OnlineScore {
			best = d // 最可靠优先（§6.4 目标磁盘选择）
		}
	}
	return MigrationPlan{
		DirKey:   dir.DirKey,
		FromDisk: dir.DiskSerial,
		ToDisk:   best.DiskSerial,
		Gain:     MigrationGain(dir.Tier, best.Tier, dir.TotalBytes),
	}, true
}

// SelectReplicaTarget 为某 asset 选择补副本目标磁盘（§7.2）：
// 排除同盘（反亲和）、会突破硬底线的盘、以及（若现有副本都在同一节点）同节点盘；
// 打分：层分布加分（鼓励 1 热 + 1 冷）+ onlineScore + 空闲量。
func SelectReplicaTarget(asset model.Asset, existing []model.Replica, allDisks []model.Disk, cfg config.Config) (model.Disk, bool) {
	bySerial := map[string]model.Disk{}
	for _, d := range allDisks {
		bySerial[d.DiskSerial] = d
	}
	existingDisks := map[string]bool{}
	existingNodes := map[string]bool{}
	for _, r := range existing {
		if r.AssetID != asset.AssetID {
			continue
		}
		existingDisks[r.DiskSerial] = true
		existingNodes[r.NodeID] = true
	}
	onlyNode := ""
	if len(existingNodes) == 1 {
		for n := range existingNodes {
			onlyNode = n
		}
	}
	hasCold, hasHotWarm := false, false
	for serial := range existingDisks {
		d, ok := bySerial[serial]
		if !ok {
			continue
		}
		if d.Tier == model.TierCold {
			hasCold = true
		} else {
			hasHotWarm = true
		}
	}

	type scored struct {
		d     model.Disk
		score float64
	}
	var cands []scored
	for _, d := range allDisks {
		if existingDisks[d.DiskSerial] {
			continue
		}
		if d.FreeBytes-asset.SizeBytes < space.HardReserveFloor(d, cfg) {
			continue
		}
		if onlyNode != "" && d.MountedNodeID == onlyNode {
			continue // 尽量跨节点（§7.2）
		}
		score := float64(d.OnlineScore)
		if d.Tier == model.TierCold && hasHotWarm && !hasCold {
			score += 2 // 鼓励 1 热 + 1 冷
		}
		if d.Tier != model.TierCold && hasCold && !hasHotWarm {
			score += 2
		}
		score += float64(d.FreeBytes) / 1e12
		cands = append(cands, scored{d: d, score: score})
	}
	if len(cands) == 0 {
		return model.Disk{}, false
	}
	best := cands[0]
	for _, c := range cands {
		if c.score > best.score {
			best = c
		}
	}
	return best.d, true
}
