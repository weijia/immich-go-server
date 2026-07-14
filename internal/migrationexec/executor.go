package migrationexec

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/migration"
	"github.com/weijia/immich-go-server/internal/model"
)

// bufSize 流式拷贝块大小。
const bufSize = 1 << 20

// BlobStore 迁移执行所需的 blob 访问抽象（§6.5 执行层）。
// 生产用 osBlobStore（真实文件系统），测试用内存实现，二者满足同一接口。
type BlobStore interface {
	StatSource(assetID string) (size int64, ok bool)
	OpenSource(assetID string, offset int64) (io.ReadCloser, error)
	CreateTarget(assetID string) (io.WriteCloser, error)
	OpenTargetAppend(assetID string) (io.WriteCloser, int64, error) // 返回当前已写字节数
	RemoveTarget(assetID string) error
	ReadManifest() (migration.Manifest, bool, error)
	WriteManifest(m migration.Manifest) error
	RemoveManifest() error
}

// Executor 驱动一次整目录迁移的状态机（§6.5.1 / §6.5.2）。
type Executor struct {
	bs  BlobStore
	cfg config.Config
}

// NewExecutor 构造执行器。
func NewExecutor(bs BlobStore, cfg config.Config) *Executor {
	return &Executor{bs: bs, cfg: cfg}
}

// Start 确保存在 manifest：已有则续传（恢复 IN_PROGRESS），否则新建 PLANNED→IN_PROGRESS。
func (e *Executor) Start(srcDisk, dstDisk string, source []model.Asset) (migration.Manifest, error) {
	if m, ok, _ := e.bs.ReadManifest(); ok {
		m.State = migration.StateInProgress
		_ = e.bs.WriteManifest(m)
		return m, nil
	}
	m := migration.Manifest{
		TaskID:     genTaskID(),
		SrcDisk:    srcDisk,
		DstDisk:    dstDisk,
		TotalFiles: len(source),
		State:      migration.StateInProgress,
		Partial:    map[string]int64{},
	}
	if err := e.bs.WriteManifest(m); err != nil {
		return m, err
	}
	return m, nil
}

// CopyFile 按续传动作拷贝单个文件（流式分块），实时更新 manifest 进度。
func (e *Executor) CopyFile(m *migration.Manifest, f model.Asset, act migration.FileAction) error {
	switch act.Mode {
	case migration.ModeSkip:
		return nil
	case migration.ModeRange, migration.ModeWhole:
		// 继续
	}

	size, ok := e.bs.StatSource(f.AssetID)
	if !ok {
		return fmt.Errorf("source missing: %s", f.AssetID)
	}

	var dst io.WriteCloser
	var offset int64
	if act.Mode == migration.ModeRange {
		w, cur, err := e.bs.OpenTargetAppend(f.AssetID)
		if err != nil {
			return err
		}
		dst, offset = w, cur
	} else {
		w, err := e.bs.CreateTarget(f.AssetID)
		if err != nil {
			return err
		}
		dst, offset = w, 0
		m.SetPartial(f.AssetID, 0)
	}

	src, err := e.bs.OpenSource(f.AssetID, offset)
	if err != nil {
		_ = dst.Close()
		return err
	}
	defer src.Close()

	buf := make([]byte, bufSize)
	written := offset
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				_ = dst.Close()
				return werr
			}
			written += int64(n)
			m.SetPartial(f.AssetID, written)
			_ = e.bs.WriteManifest(*m) // 周期性落盘进度（断点续传关键）
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = dst.Close()
			return rerr
		}
	}
	if err := dst.Close(); err != nil {
		return err
	}
	if written != size {
		return fmt.Errorf("size mismatch %s: got %d want %d", f.AssetID, written, size)
	}
	m.MarkCompleted(f.AssetID)
	_ = e.bs.WriteManifest(*m)
	return nil
}

// Run 驱动 COPIED → VERIFIED：依续传计划拷完所有文件后标记 VERIFIED；任一步失败回滚。
func (e *Executor) Run(m *migration.Manifest, source []model.Asset) (string, error) {
	plan := migration.ComputeResume(*m, source)
	byID := map[string]model.Asset{}
	for _, f := range source {
		byID[f.AssetID] = f
	}
	for _, act := range plan {
		if err := e.CopyFile(m, byID[act.AssetID], act); err != nil {
			m.State = migration.StateRollback
			_ = e.bs.WriteManifest(*m)
			return m.State, err
		}
	}
	if migration.AllCopied(*m, source) {
		m.State = migration.StateVerified
		_ = e.bs.WriteManifest(*m)
	}
	return m.State, nil
}

// Finish 删源前置硬约束（§6.5.2）：目录下每个 asset 有效副本数 ≥ MinReplicas 才进入 DONE，
// 并清理目标残留 manifest（partial 此时应为空）。
func (e *Executor) Finish(m *migration.Manifest, source []model.Asset, effective func(assetID string) int) (string, error) {
	counts := make(map[string]int, len(source))
	for _, a := range source {
		counts[a.AssetID] = effective(a.AssetID)
	}
	if !migration.CanDeleteSource(source, counts, e.cfg) {
		return m.State, fmt.Errorf("cannot delete source: effective replicas below minimum")
	}
	for assetID := range m.Partial {
		_ = e.bs.RemoveTarget(assetID)
	}
	_ = e.bs.RemoveManifest()
	m.State = migration.StateDone
	return m.State, nil
}

// Rollback 回滚：清理目标盘全部已拷文件（completed+partial）与 manifest，源保留（§6.5.2）。
func (e *Executor) Rollback(m *migration.Manifest) error {
	for _, assetID := range m.Completed {
		_ = e.bs.RemoveTarget(assetID)
	}
	for assetID := range m.Partial {
		_ = e.bs.RemoveTarget(assetID)
	}
	if err := e.bs.RemoveManifest(); err != nil {
		return err
	}
	m.State = migration.StateRollback
	return nil
}

func genTaskID() string {
	return fmt.Sprintf("mig-%d", time.Now().UnixNano())
}

// ---- 生产用：真实文件系统 BlobStore ----

// osBlobStore 基于磁盘目录的 BlobStore 实现。
type osBlobStore struct {
	srcRoot string
	dstRoot string
}

// NewOSBlobStore 构造真实文件系统执行后端。
func NewOSBlobStore(srcRoot, dstRoot string) BlobStore {
	return &osBlobStore{srcRoot: srcRoot, dstRoot: dstRoot}
}

func (b *osBlobStore) path(root, assetID string) string { return filepath.Join(root, assetID) }

func (b *osBlobStore) StatSource(assetID string) (int64, bool) {
	fi, err := os.Stat(b.path(b.srcRoot, assetID))
	if err != nil {
		return 0, false
	}
	return fi.Size(), true
}

func (b *osBlobStore) OpenSource(assetID string, offset int64) (io.ReadCloser, error) {
	f, err := os.Open(b.path(b.srcRoot, assetID))
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return f, nil
}

func (b *osBlobStore) CreateTarget(assetID string) (io.WriteCloser, error) {
	return os.Create(b.path(b.dstRoot, assetID))
}

func (b *osBlobStore) OpenTargetAppend(assetID string) (io.WriteCloser, int64, error) {
	p := b.path(b.dstRoot, assetID)
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

func (b *osBlobStore) RemoveTarget(assetID string) error {
	err := os.Remove(b.path(b.dstRoot, assetID))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (b *osBlobStore) ReadManifest() (migration.Manifest, bool, error) {
	p := filepath.Join(b.dstRoot, ".migrating.json")
	data, err := os.ReadFile(p)
	if err != nil {
		return migration.Manifest{}, false, nil
	}
	var m migration.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return migration.Manifest{}, false, err
	}
	return m, true, nil
}

func (b *osBlobStore) WriteManifest(m migration.Manifest) error {
	p := filepath.Join(b.dstRoot, ".migrating.json")
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

func (b *osBlobStore) RemoveManifest() error {
	err := os.Remove(filepath.Join(b.dstRoot, ".migrating.json"))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
