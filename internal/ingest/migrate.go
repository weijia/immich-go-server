package ingest

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	"github.com/weijia/immich-go-server/internal/model"
)

// MigratePhysName 一次性迁移：把仓库下物理文件名统一为
// "<原始文件名片段>[_<assetId>][.<ext>]" 规则（原始名在前、sha256 在后，便于按原名浏览）。
//
// 背景：
//  - 早期版本：裸 <sha256>（无 ext）
//  - 上一版：<sha256>[_<name>].<ext>
//  - 当前版：<name>[_<assetId>].<ext>（原始名在前、内容寻址 sha256 在后）
// 本函数在服务启动时对各 DISK_DIRS 根执行，使存量文件一致。
//
// 实现：逐目录读取 .meta.json，对其中每条资产计算当前物理路径（ResolvePhysPath，兼容历史形态）
// 与目标路径（PhysNameInDir），不一致则重命名；目标已存在则跳过（避免覆盖）。幂等。
func MigratePhysName(roots []string) error {
	for _, root := range roots {
		if root == "" {
			continue
		}
		err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return nil // 跳过不可读分支
			}
			if d.IsDir() || d.Name() != ".meta.json" {
				return nil
			}
			dir := filepath.Dir(p)
			mf, ok := readMetaAt(dir)
			if !ok {
				return nil
			}
			for _, a := range mf.Assets {
				op := a.OriginalPath
				cur := model.ResolvePhysPath(dir, a.AssetID, op)
				target := model.PhysNameInDir(a.AssetID, op, dir)
				if cur == filepath.Join(dir, target) {
					continue
				}
				if _, serr := os.Stat(filepath.Join(dir, target)); serr == nil {
					continue // 目标已存在，跳过（避免覆盖）
				}
				if _, cerr := os.Stat(cur); cerr != nil {
					continue // 当前文件缺失，跳过
				}
				if rerr := os.Rename(cur, filepath.Join(dir, target)); rerr != nil {
					log.Printf("migrate phys name %s: %v", cur, rerr)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// readMetaAt 读取 dir 下的 .meta.json（复用 metaPath）。
func readMetaAt(dir string) (MetaFile, bool) {
	b, err := os.ReadFile(metaPath(dir, ""))
	if err != nil {
		return MetaFile{}, false
	}
	var mf MetaFile
	if err := json.Unmarshal(b, &mf); err != nil {
		return MetaFile{}, false
	}
	return mf, true
}
