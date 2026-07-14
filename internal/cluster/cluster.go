package cluster

import (
	"sort"

	"github.com/weijia/immich-go-server/internal/model"
)

// AggregateDiskStats 全局 disk_stats 只读聚合（§4.3）：
// 同一 diskSerial 被多个来源上报时，取在线秒较大者、mountedNodeId 取最新上报者、
// freeBytes 取最新上报者、firstSeenAt 取较小者。结果只存在于调用方内存。
func AggregateDiskStats(reports []model.Node) map[string]model.Disk {
	merged := map[string]model.Disk{}
	for _, n := range reports {
		for _, d := range n.Disks {
			cur, ok := merged[d.DiskSerial]
			if !ok {
				merged[d.DiskSerial] = d
				continue
			}
			if d.OnlineSeconds > cur.OnlineSeconds {
				cur.OnlineSeconds = d.OnlineSeconds
			}
			if d.LastSeenAt > cur.LastSeenAt {
				cur.MountedNodeID = d.MountedNodeID
			}
			cur.FreeBytes = d.FreeBytes
			if d.FirstSeenAt < cur.FirstSeenAt {
				cur.FirstSeenAt = d.FirstSeenAt
			}
			merged[d.DiskSerial] = cur
		}
	}
	return merged
}

// AggregateDirectoryStats 全局目录放置图只读聚合（§8.6 控制面）：
// 同一 dir_key 被多个来源上报时，取 last_eval_at 较大者（更新者胜）；
// 时间戳相同则取 nodeId 字典序较大者以保证确定性。结果只存在于调用方内存，
// 由调用方（Node.Federate）持久化进本地库，使单节点下线后记录仍保留。
func AggregateDirectoryStats(reports []model.Node) map[string]model.Directory {
	merged := map[string]model.Directory{}
	for _, n := range reports {
		for _, d := range n.Directories {
			cur, ok := merged[d.DirKey]
			if !ok || d.LastEvalAt > cur.LastEvalAt ||
				(d.LastEvalAt == cur.LastEvalAt && d.NodeID > cur.NodeID) {
				merged[d.DirKey] = d
			}
		}
	}
	return merged
}

// ElectCoordinator 选举协调者（§10）：最高 onlineScore 优先，平票取 nodeId 最小者。
func ElectCoordinator(nodes []model.Node) string {
	best := ""
	bestScore := -1.0
	for _, n := range nodes {
		if n.OnlineScore > bestScore || (n.OnlineScore == bestScore && (best == "" || n.NodeID < best)) {
			best = n.NodeID
			bestScore = n.OnlineScore
		}
	}
	return best
}

// AssignTiers 按在线时长把所有磁盘排名分 Hot/Warm/Cold 三层（§5.1）。
// 前 1/3 为热层、后 1/3 为冷层、中间为温层；返回保持输入顺序。
func AssignTiers(disks []model.Disk) []model.Disk {
	n := len(disks)
	if n == 0 {
		return disks
	}
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		return disks[order[a]].OnlineSeconds > disks[order[b]].OnlineSeconds
	})
	tierOf := make([]model.Tier, n)
	hotN := n / 3
	coldN := n / 3
	for rank, idx := range order {
		switch {
		case rank < hotN:
			tierOf[idx] = model.TierHot
		case rank >= n-coldN:
			tierOf[idx] = model.TierCold
		default:
			tierOf[idx] = model.TierWarm
		}
	}
	result := make([]model.Disk, n)
	for i := range disks {
		result[i] = disks[i]
		result[i].Tier = tierOf[i]
	}
	return result
}
