package migration

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/model"
)

// 任务状态机（§6.5.1 / §6.5.2）：PLANNED → IN_PROGRESS → COPIED → VERIFIED → DONE；异常 ROLLBACK / FAILED。
const (
	StatePlanned    = "PLANNED"
	StateInProgress = "IN_PROGRESS"
	StateCopied     = "COPIED"
	StateVerified   = "VERIFIED"
	StateDone       = "DONE"
	StateRollback   = "ROLLBACK"
	StateFailed     = "FAILED"
)

// ActionMode 续传时每个源文件采取的动作（§6.5.1）。
type ActionMode int

const (
	// ModeSkip 已完成，仅轻量重校验，不重传。
	ModeSkip ActionMode = iota
	// ModeRange 字节级续传：发 Range: bytes=<BytesCopied>- 追加尾部。
	ModeRange
	// ModeWhole 从头整文件重拷。
	ModeWhole
)

// FileAction 续传决策结果：单个源文件该怎么处理。
type FileAction struct {
	AssetID     string
	Mode        ActionMode
	BytesCopied int64 // 仅 ModeRange 有意义
}

// Manifest 目标盘 .<dirKey>.migrating.json 临时清单（§6.5.1）。
// completed = 已校验落盘的文件；partial = 半成品（按字节）；源始终保留直到 DONE。
type Manifest struct {
	TaskID     string
	SrcDisk    string
	DstDisk    string
	DirKey     string
	TotalFiles int
	TotalBytes int64
	Completed  []string
	Partial    map[string]int64
	State      string
}

// MarkCompleted 文件复制+校验成功后加入 completed 并移出 partial。
func (m *Manifest) MarkCompleted(assetID string) {
	m.Completed = append(m.Completed, assetID)
	delete(m.Partial, assetID)
}

// SetPartial 记录某文件的字节级续传进度。
func (m *Manifest) SetPartial(assetID string, bytesCopied int64) {
	if m.Partial == nil {
		m.Partial = map[string]int64{}
	}
	m.Partial[assetID] = bytesCopied
}

// ComputeResume 根据源目录当前文件清单与已有 manifest，决定每个文件的续传动作（§6.5.1）。
// 已 completed → 跳过；partial 且有进度 → 字节级续传；其余 → 从头整文件重拷。
func ComputeResume(m Manifest, source []model.Asset) []FileAction {
	compSet := make(map[string]bool, len(m.Completed))
	for _, a := range m.Completed {
		compSet[a] = true
	}
	actions := make([]FileAction, 0, len(source))
	for _, f := range source {
		switch {
		case compSet[f.AssetID]:
			actions = append(actions, FileAction{AssetID: f.AssetID, Mode: ModeSkip})
		default:
			if bc, ok := m.Partial[f.AssetID]; ok && bc > 0 {
				actions = append(actions, FileAction{AssetID: f.AssetID, Mode: ModeRange, BytesCopied: bc})
			} else {
				actions = append(actions, FileAction{AssetID: f.AssetID, Mode: ModeWhole})
			}
		}
	}
	return actions
}

// AllCopied 报告源目录每个文件是否都已进入 completed（对应 COPIED 时点，§6.5.2）。
func AllCopied(m Manifest, source []model.Asset) bool {
	compSet := make(map[string]bool, len(m.Completed))
	for _, a := range m.Completed {
		compSet[a] = true
	}
	if len(compSet) != len(source) {
		return false
	}
	for _, f := range source {
		if !compSet[f.AssetID] {
			return false
		}
	}
	return true
}

// DirChecksum 计算整目录 checksum（§6.5.2 / §11）：源文件按 assetID 排序后
// 拼接 "assetID:checksum\n" 再做 SHA256，保证确定性、可跨节点比对。
func DirChecksum(files []model.Asset) string {
	sorted := make([]model.Asset, len(files))
	copy(sorted, files)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].AssetID < sorted[j].AssetID })
	h := sha256.New()
	for _, f := range sorted {
		h.Write([]byte(f.AssetID + ":" + f.Checksum + "\n"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// DirChecksumsMatch 比对源目录与目标目录的 checksum（VERIFIED 判定，§6.5.2）。
func DirChecksumsMatch(source, target []model.Asset) bool {
	return DirChecksum(source) == DirChecksum(target)
}

// CanDeleteSource 删源前置硬约束（§6.5.2）：目录下每个 asset 有效副本数 ≥ MinReplicas。
// effectiveReplicas 由调用方提供（依据 §7.2：副本所在盘可达 / last_seen 在窗口内）。
// 任一 asset 不满足 → 返回 false，绝不删源。
func CanDeleteSource(dirAssets []model.Asset, effectiveReplicas map[string]int, cfg config.Config) bool {
	for _, a := range dirAssets {
		if effectiveReplicas[a.AssetID] < cfg.MinReplicas {
			return false
		}
	}
	return true
}

// RollbackTargets 返回回滚/失败时必须清理的目标盘残留（§6.5.2）：清单文件本身 + 所有 partial 半成品。
// 清理失败不阻塞回滚（源安全优先）。
func RollbackTargets(m Manifest, manifestPath string) []string {
	out := []string{manifestPath}
	for assetID := range m.Partial {
		out = append(out, assetID)
	}
	return out
}
