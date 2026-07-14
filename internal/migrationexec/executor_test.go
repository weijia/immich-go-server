package migrationexec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/migration"
	"github.com/weijia/immich-go-server/internal/model"
)

// memFS 测试用内存 BlobStore，确定性、无需真实文件。
type memFS struct {
	src     map[string][]byte
	dst     map[string][]byte
	manifest migration.Manifest
	hasMan  bool
}

func newMemFS() *memFS {
	return &memFS{src: map[string][]byte{}, dst: map[string][]byte{}}
}

func (m *memFS) putSource(assetID string, data []byte) { m.src[assetID] = data }

func (m *memFS) StatSource(assetID string) (int64, bool) {
	d, ok := m.src[assetID]
	return int64(len(d)), ok
}

func (m *memFS) OpenSource(assetID string, offset int64) (io.ReadCloser, error) {
	d, ok := m.src[assetID]
	if !ok {
		return nil, fmt.Errorf("no source %s", assetID)
	}
	return io.NopCloser(bytes.NewReader(d[offset:])), nil
}

func (m *memFS) CreateTarget(assetID string) (io.WriteCloser, error) {
	m.dst[assetID] = nil
	return &memWriter{fs: m, id: assetID, append: false}, nil
}

func (m *memFS) OpenTargetAppend(assetID string) (io.WriteCloser, int64, error) {
	cur := int64(len(m.dst[assetID]))
	return &memWriter{fs: m, id: assetID, append: true}, cur, nil
}

func (m *memFS) RemoveTarget(assetID string) error {
	delete(m.dst, assetID)
	return nil
}

func (m *memFS) ReadManifest() (migration.Manifest, bool, error) {
	if !m.hasMan {
		return migration.Manifest{}, false, nil
	}
	return m.manifest, true, nil
}

func (m *memFS) WriteManifest(man migration.Manifest) error {
	m.manifest = man
	m.hasMan = true
	return nil
}

func (m *memFS) RemoveManifest() error {
	m.hasMan = false
	return nil
}

// memWriter 内存写器；append=true 时追加到现有切片。
type memWriter struct {
	fs     *memFS
	id     string
	append bool
	buf    []byte
}

func (w *memWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *memWriter) Close() error {
	if w.append {
		w.fs.dst[w.id] = append(w.fs.dst[w.id], w.buf...)
	} else {
		w.fs.dst[w.id] = w.buf
	}
	return nil
}

var _ BlobStore = (*memFS)(nil)

func srcAssets() []model.Asset {
	return []model.Asset{
		{AssetID: "a1", Checksum: "c1", SizeBytes: 5},
		{AssetID: "a2", Checksum: "c2", SizeBytes: 7},
		{AssetID: "a3", Checksum: "c3", SizeBytes: 11},
	}
}

func fillSource(fs *memFS) {
	fs.putSource("a1", []byte("hello"))
	fs.putSource("a2", []byte("world!!"))
	fs.putSource("a3", []byte("bigfiledata"))
}

func TestFullMigrationToDone(t *testing.T) {
	fs := newMemFS()
	fillSource(fs)
	e := NewExecutor(fs, config.Default())
	src := srcAssets()

	m, err := e.Start("SRC", "DST", src)
	if err != nil {
		t.Fatal(err)
	}
	state, err := e.Run(&m, src)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if state != migration.StateVerified {
		t.Fatalf("expected VERIFIED, got %s", state)
	}
	// 目标文件应与源一致
	for _, a := range src {
		if string(fs.dst[a.AssetID]) != string(fs.src[a.AssetID]) {
			t.Errorf("target %s content mismatch", a.AssetID)
		}
	}
	// 有效副本都 >=2 → 可删源进入 DONE
	effective := func(string) int { return 2 }
	state, err = e.Finish(&m, src, effective)
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if state != migration.StateDone {
		t.Fatalf("expected DONE, got %s", state)
	}
	if fs.hasMan {
		t.Error("manifest should be removed after DONE")
	}
}

func TestCannotDeleteWhenReplicasInsufficient(t *testing.T) {
	fs := newMemFS()
	fillSource(fs)
	e := NewExecutor(fs, config.Default())
	src := srcAssets()
	m, _ := e.Start("SRC", "DST", src)
	if _, err := e.Run(&m, src); err != nil {
		t.Fatal(err)
	}
	// a3 只有 1 份有效副本
	effective := func(id string) int {
		if id == "a3" {
			return 1
		}
		return 2
	}
	if _, err := e.Finish(&m, src, effective); err == nil {
		t.Error("should refuse to delete source when a replica is below minimum")
	}
}

func TestResumeAfterInterruption(t *testing.T) {
	fs := newMemFS()
	fillSource(fs)
	e := NewExecutor(fs, config.Default())
	src := srcAssets()

	// 第一轮：只完成 a1（模拟中断）
	m, _ := e.Start("SRC", "DST", src)
	_ = e.CopyFile(&m, src[0], migration.FileAction{AssetID: "a1", Mode: migration.ModeWhole})
	// 模拟进程崩溃：manifest 已落盘，但 a2/a3 未拷

	// 第二轮：从 manifest 续传
	m2, ok, _ := fs.ReadManifest()
	if !ok {
		t.Fatal("manifest should persist")
	}
	e2 := NewExecutor(fs, config.Default())
	state, err := e2.Run(&m2, src)
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if state != migration.StateVerified {
		t.Fatalf("expected VERIFIED, got %s", state)
	}
	if string(fs.dst["a1"]) != "hello" || string(fs.dst["a2"]) != "world!!" || string(fs.dst["a3"]) != "bigfiledata" {
		t.Error("resumed files mismatch")
	}
}

func TestRollbackCleansTarget(t *testing.T) {
	fs := newMemFS()
	fillSource(fs)
	e := NewExecutor(fs, config.Default())
	src := srcAssets()
	m, _ := e.Start("SRC", "DST", src)
	_ = e.CopyFile(&m, src[0], migration.FileAction{AssetID: "a1", Mode: migration.ModeWhole})
	if err := e.Rollback(&m); err != nil {
		t.Fatal(err)
	}
	if _, ok := fs.dst["a1"]; ok {
		t.Error("target a1 should be removed on rollback")
	}
	if fs.hasMan {
		t.Error("manifest should be removed on rollback")
	}
}

func TestManifestJSONRoundTrip(t *testing.T) {
	// 确保 Manifest 可被 json 序列化（osBlobStore 依赖此）
	m := migration.Manifest{TaskID: "t", SrcDisk: "S", DstDisk: "D", Partial: map[string]int64{"a": 3}, State: migration.StateInProgress}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var back migration.Manifest
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Partial["a"] != 3 || back.TaskID != "t" {
		t.Error("manifest round trip mismatch")
	}
}
