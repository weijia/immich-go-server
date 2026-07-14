package space

import (
	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/model"
)

// HardReserveFloor 返回一块盘必须保留的最小空闲字节（§5.5）：
// 取「容量×比例」与「绝对字节」的较大值。
func HardReserveFloor(d model.Disk, cfg config.Config) int64 {
	byRatio := int64(float64(d.CapacityBytes) * cfg.DiskMinFreeRatio)
	if byRatio > cfg.DiskMinFreeBytes {
		return byRatio
	}
	return cfg.DiskMinFreeBytes
}

// MeetsHardFloor 报告写入 size 字节后，空闲是否仍 ≥ 硬底线（§5.5）。
func MeetsHardFloor(d model.Disk, size int64, cfg config.Config) bool {
	return d.FreeBytes-size >= HardReserveFloor(d, cfg)
}

// PrecheckSpace 迁移前空间预检（§6.3）：复制整目录后空闲须 ≥ 硬底线。
func PrecheckSpace(d model.Disk, dirSize int64, cfg config.Config) bool {
	required := int64(float64(dirSize) * (1 + cfg.SafetyMargin))
	floor := HardReserveFloor(d, cfg)
	return d.FreeBytes >= required+floor
}

func softTarget(t model.Tier, cfg config.Config) float64 {
	switch t {
	case model.TierHot:
		return cfg.HotFreeTarget
	case model.TierWarm:
		return cfg.WarmFreeTarget
	default:
		return cfg.ColdFreeTarget
	}
}

// BelowSoftTarget 报告磁盘空闲率是否低于其层软目标（§5.2 / §5.4(c) 反应式触发）。
func BelowSoftTarget(d model.Disk, cfg config.Config) bool {
	if d.CapacityBytes <= 0 {
		return false
	}
	return float64(d.FreeBytes)/float64(d.CapacityBytes) < softTarget(d.Tier, cfg)
}

// SelectWriteDisk 写时磁盘分配（§5.4(b)）：在本机多盘中按
// Hot→Warm→Cold 优先级挑选，且写入后空闲须同时满足硬底线(§5.5)与层软目标(§5.2)。
// 找不到满足条件的盘时返回 ok=false（对应上传 507 Storage Full）。
func SelectWriteDisk(local []model.Disk, size int64, cfg config.Config) (model.Disk, bool) {
	var cands []model.Disk
	for _, d := range local {
		if MeetsHardFloor(d, size, cfg) {
			cands = append(cands, d)
		}
	}
	if len(cands) == 0 {
		return model.Disk{}, false
	}
	for _, pref := range []model.Tier{model.TierHot, model.TierWarm, model.TierCold} {
		var pool []model.Disk
		for _, d := range cands {
			if d.Tier != pref {
				continue
			}
			after := d.FreeBytes - size
			ratio := float64(after) / float64(d.CapacityBytes)
			if ratio >= softTarget(pref, cfg) {
				pool = append(pool, d)
			}
		}
		if len(pool) > 0 {
			best := pool[0]
			for _, d := range pool {
				if d.FreeBytes > best.FreeBytes {
					best = d
				}
			}
			return best, true
		}
	}
	return model.Disk{}, false
}
