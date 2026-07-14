// Package ingest 把本地文件目录摄入到某磁盘仓库（每磁盘一个 blob_root）。
//
// 设计原则（§仓库即真相）：摄入“只移动文件”，不碰 SQLite。对每个文件：
//  1. 计算 sha256 → assetID（内容寻址，天然去重）；
//  2. 取拍摄/修改时间 → dir_key = YYYY/MM（按时间规律放置）；
//  3. 同盘 os.Rename（原子、零拷贝），跨设备回退 copy+删源（move 语义）；
//  4. 把元数据写入 blobRoot/<dir_key>/.meta.json 分片（方案③：每时间桶一个 sidecar）。
// 后台 scanner 读这些 sidecar 把 asset/replica/directory 同步进 SQLite。
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// MetaAsset 单个资产在 sidecar 中的记录（方案③）。
type MetaAsset struct {
	AssetID      string `json:"asset_id"`
	Checksum     string `json:"checksum"`
	SizeBytes    int64  `json:"size_bytes"`
	OriginalPath string `json:"original_path"`
	CapturedAt   string `json:"captured_at,omitempty"`
	Kind         string `json:"kind"` // photo | video | doc | other
}

// MetaFile 某 dir_key 分片的 sidecar。
type MetaFile struct {
	DirKey string      `json:"dir_key"`
	Assets []MetaAsset `json:"assets"`
}

// TimeSource 提供决定 dir_key 的时间（按时间规律放置）。
// 默认 MTimeSource 用文件修改时间；后续可注入 EXIF/容器读取器以对
// 照片/视频取更准确的拍摄时间，不影响 ingest 其余逻辑。
type TimeSource interface {
	CaptureTime(path string) (time.Time, error)
}

// MTimeSource 默认实现：统一用文件修改时间。
type MTimeSource struct{}

func (MTimeSource) CaptureTime(path string) (time.Time, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return fi.ModTime(), nil
}

// Ingester 把本地目录摄入到某磁盘仓库。
type Ingester struct {
	TimeOf TimeSource
}

// Report 摄入统计。
type Report struct {
	Scanned int
	Moved   int
	Copied  int
	Skipped int
	Errors  int
}

// Run 扫描 srcRoot 下所有常规文件，移动（或复制）到 blobRoot/<dir_key>/<assetID>，
// 并写 blobRoot/<dir_key>/.meta.json 分片。dir_key = 时间的 YYYY/MM。
// mode 为 "move"（默认，删源）或 "copy"。返回统计与首个遍历错误。
func (g *Ingester) Run(ctx context.Context, srcRoot, blobRoot, mode string) (*Report, error) {
	if g.TimeOf == nil {
		g.TimeOf = MTimeSource{}
	}
	if mode == "" {
		mode = "move"
	}
	srcRoot, err := filepath.Abs(srcRoot)
	if err != nil {
		return nil, err
	}
	blobRoot, err = filepath.Abs(blobRoot)
	if err != nil {
		return nil, err
	}
	rep := &Report{}

	// 累积各 dir_key 的 sidecar，Walk 结束后统一写（每分片一次写，减少 IO）。
	pending := map[string][]MetaAsset{}

	walkErr := filepath.WalkDir(srcRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			rep.Errors++
			return nil // 跳过不可读项
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil // 不跟随符号链接，避免循环
		}
		// 避免把刚移入 blobRoot 的文件再次扫到（blobRoot 不应在 srcRoot 之下，防御性跳过）
		if isAncestor(blobRoot, path) {
			return nil
		}

		rep.Scanned++
		sum, size, err := checksumFile(path)
		if err != nil {
			rep.Errors++
			return nil
		}
		assetID := sum // 内容寻址：assetID = sha256 hex
		t, err := g.TimeOf.CaptureTime(path)
		if err != nil {
			t = info.ModTime()
		}
		dirKey := t.Format("2006/01")
		kind := classify(path)

		destDir := filepath.Join(blobRoot, filepath.FromSlash(dirKey))
		dest := filepath.Join(destDir, assetID)

		// 幂等：目标已存在即同内容已摄入 → 跳过（move 时顺手删源，避免孤儿）
		if exists(dest) {
			rep.Skipped++
			if mode == "move" {
				_ = os.Remove(path)
			}
			return nil
		}

		if err := os.MkdirAll(destDir, 0o755); err != nil {
			rep.Errors++
			return nil
		}
		if mode == "move" {
			if err := moveOrCopy(path, dest); err != nil {
				rep.Errors++
				return nil
			}
			rep.Moved++
		} else {
			if err := copyFile(path, dest); err != nil {
				rep.Errors++
				return nil
			}
			rep.Copied++
		}
		pending[dirKey] = append(pending[dirKey], MetaAsset{
			AssetID:      assetID,
			Checksum:     sum,
			SizeBytes:    size,
			OriginalPath: path,
			CapturedAt:   t.Format(time.RFC3339),
			Kind:         kind,
		})
		return nil
	})
	if walkErr != nil {
		return rep, walkErr
	}

	// flush sidecar 分片（合并既有，按 asset_id 去重）
	for dirKey, assets := range pending {
		if err := flushMeta(blobRoot, dirKey, assets); err != nil {
			rep.Errors++
		}
	}
	return rep, nil
}

// flushMeta 把本批资产合并进 blobRoot/<dir_key>/.meta.json（保留既有，按 asset_id 去重）。
func flushMeta(blobRoot, dirKey string, assets []MetaAsset) error {
	p := metaPath(blobRoot, dirKey)
	var existing MetaFile
	if b, err := os.ReadFile(p); err == nil {
		_ = json.Unmarshal(b, &existing)
	}
	merged := map[string]MetaAsset{}
	for _, a := range existing.Assets {
		merged[a.AssetID] = a
	}
	for _, a := range assets {
		merged[a.AssetID] = a
	}
	out := MetaFile{DirKey: dirKey, Assets: make([]MetaAsset, 0, len(merged))}
	for _, a := range merged {
		out.Assets = append(out.Assets, a)
	}
	sort.Slice(out.Assets, func(i, j int) bool { return out.Assets[i].AssetID < out.Assets[j].AssetID })
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

func metaPath(blobRoot, dirKey string) string {
	return filepath.Join(blobRoot, filepath.FromSlash(dirKey), ".meta.json")
}

func checksumFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// moveOrCopy 先尝试 rename（同盘原子、零拷贝）；跨设备(EXDEV)或失败则回退
// copy + 删除源（move 语义）。仅 move 模式调用。
func moveOrCopy(src, dest string) error {
	if err := os.Rename(src, dest); err == nil {
		return nil
	}
	if err := copyFile(src, dest); err != nil {
		return err
	}
	return os.Remove(src)
}

func copyFile(src, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// isAncestor 报告 a 是否为 b 的祖先目录（含相等）。
func isAncestor(a, b string) bool {
	if a == b {
		return true
	}
	return strings.HasPrefix(b, a+string(os.PathSeparator))
}

func classify(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".jpg", ".jpeg", ".png", ".heic", ".heif", ".gif", ".bmp", ".webp", ".tiff", ".tif":
		return "photo"
	case ".mp4", ".mov", ".avi", ".mkv", ".m4v", ".webm", ".flv":
		return "video"
	case ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
		".txt", ".md", ".pages", ".key", ".numbers", ".rtf", ".odt":
		return "doc"
	default:
		return "other"
	}
}

var _ = fmt.Sprintf // 保留导入（便于后续日志扩展）
