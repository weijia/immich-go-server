// Package claim 实现磁盘认领决策与在线时长累计（§11.3）。
// 纯函数便于单测；store.ClaimOrTouchDisk 复用此处逻辑做原子写库。
package claim

import "github.com/weijia/immich-go-server/internal/model"

// EligibleForClaim 判断本节点 claimingNode 在 now 是否可以认领磁盘 d（§11.3）：
//   - 磁盘尚无挂载节点 → 可认领
//   - 挂载节点即本节点 → 已持有（无需重复认领）
//   - 挂载节点为其他节点，但其最近出现距今已超过 graceOffline（视为离线）→ 可重新认领
//   - 其余（被其他在线节点持有）→ 不可
func EligibleForClaim(d model.Disk, claimingNode string, now, graceOffline int64) bool {
	if d.MountedNodeID == "" {
		return true
	}
	if d.MountedNodeID == claimingNode {
		return false
	}
	idle := now - d.LastSeenAt
	return idle >= graceOffline
}

// AccrueOnlineSeconds 计算两次心跳之间应累加的在线秒数，封顶为间隔以防时钟回拨。
func AccrueOnlineSeconds(prevSeconds, lastSeenAt, now int64) int64 {
	delta := now - lastSeenAt
	if delta < 0 {
		delta = 0
	}
	return prevSeconds + delta
}
