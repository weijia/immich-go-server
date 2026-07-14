package migrationexec

import (
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/migration"
	"github.com/weijia/immich-go-server/internal/model"
)

const (
	remoteTestNode   = "node-A"
	remoteTestSecret = "shared-cluster-secret"
	remoteFixedNow   = int64(1700000000)
)

// noopProvider 满足 clusterapi.StateProvider，本测试不关心状态/任务。
type noopProvider struct{}

func (noopProvider) GetState() clusterapi.StatePayload { return clusterapi.StatePayload{} }
func (noopProvider) GetDiskLocation(string) (string, bool) { return "", false }
func (noopProvider) RegisterReplica(string, string, string) error { return nil }
func (noopProvider) SubmitTask(clusterapi.Task) error             { return nil }

// newBlobServer 启动一个带 HMAC 鉴权、由本地目录提供 blob 的集群 API 测试服务。
func newBlobServer(t *testing.T, srcDir string) *httptest.Server {
	t.Helper()
	h := clusterapi.NewHandler(remoteTestNode, remoteTestSecret, 300, noopProvider{})
	h.Now = func() int64 { return remoteFixedNow }
	h.Source = clusterapi.FileSystemBlobSource{Root: srcDir}
	return httptest.NewServer(h.Mux())
}

func TestRemoteBlobFullPull(t *testing.T) {
	srcDir := t.TempDir()
	data := []byte("remote-cluster-blob-payload-ABCDEFGHIJ")
	if err := os.WriteFile(filepath.Join(srcDir, "a1"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	srv := newBlobServer(t, srcDir)
	defer srv.Close()

	dstDir := t.TempDir()
	remote := &RemoteSource{
		BaseURL: srv.URL,
		NodeID:  remoteTestNode,
		Secret:  remoteTestSecret,
		Now:     func() int64 { return remoteFixedNow },
	}
	store := NewRemoteBlobStore(remote, NewOSBlobStore("", dstDir))
	exec := NewExecutor(store, config.Default())

	source := []model.Asset{{AssetID: "a1", SizeBytes: int64(len(data))}}
	m, err := exec.Start("src", "dst", source)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if state, err := exec.Run(&m, source); err != nil {
		t.Fatalf("Run: %v", err)
	} else if state != migration.StateVerified {
		t.Fatalf("expected VERIFIED, got %s", state)
	}

	got, err := os.ReadFile(filepath.Join(dstDir, "a1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("pulled content mismatch: %q", string(got))
	}
}

func TestRemoteBlobResume(t *testing.T) {
	srcDir := t.TempDir()
	data := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	if err := os.WriteFile(filepath.Join(srcDir, "a1"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	srv := newBlobServer(t, srcDir)
	defer srv.Close()

	dstDir := t.TempDir()
	// 预置半成品：已写入前 10 字节
	if err := os.WriteFile(filepath.Join(dstDir, "a1"), data[:10], 0o644); err != nil {
		t.Fatal(err)
	}

	remote := &RemoteSource{
		BaseURL: srv.URL,
		NodeID:  remoteTestNode,
		Secret:  remoteTestSecret,
		Now:     func() int64 { return remoteFixedNow },
	}
	store := NewRemoteBlobStore(remote, NewOSBlobStore("", dstDir))
	exec := NewExecutor(store, config.Default())

	// 预置已中断的 manifest（partial a1:10）
	pre := migration.Manifest{
		TaskID:     "mig-resume",
		SrcDisk:    "src",
		DstDisk:    "dst",
		TotalFiles: 1,
		State:      migration.StateInProgress,
		Partial:    map[string]int64{"a1": 10},
	}
	if err := store.WriteManifest(pre); err != nil {
		t.Fatal(err)
	}

	source := []model.Asset{{AssetID: "a1", SizeBytes: int64(len(data))}}
	m, err := exec.Start("src", "dst", source) // 恢复 IN_PROGRESS 并读到 partial
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if m.Partial["a1"] != 10 {
		t.Fatalf("resume manifest should keep partial=10, got %v", m.Partial)
	}
	if state, err := exec.Run(&m, source); err != nil {
		t.Fatalf("Run: %v", err)
	} else if state != migration.StateVerified {
		t.Fatalf("expected VERIFIED, got %s", state)
	}

	got, _ := os.ReadFile(filepath.Join(dstDir, "a1"))
	if string(got) != string(data) {
		t.Errorf("resumed content mismatch: %q", string(got))
	}
}

func TestRemoteStatSourceMissing(t *testing.T) {
	srv := newBlobServer(t, t.TempDir())
	defer srv.Close()
	remote := &RemoteSource{
		BaseURL: srv.URL,
		NodeID:  remoteTestNode,
		Secret:  remoteTestSecret,
		Now:     func() int64 { return remoteFixedNow },
	}
	if _, ok := remote.StatSource("nope"); ok {
		t.Error("missing blob StatSource should return ok=false")
	}
}

// 验证从 HMAC 鉴权端点拉取的字节与源一致（透过 Range 续传路径）。
func TestRemoteSourceRangeBytes(t *testing.T) {
	srcDir := t.TempDir()
	data := []byte("abcdefghijklmnopqrstuvwxyz")
	if err := os.WriteFile(filepath.Join(srcDir, "a1"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	srv := newBlobServer(t, srcDir)
	defer srv.Close()
	remote := &RemoteSource{
		BaseURL: srv.URL,
		NodeID:  remoteTestNode,
		Secret:  remoteTestSecret,
		Now:     func() int64 { return remoteFixedNow },
	}
	// 从偏移 5 起拉取，应得到后段
	rc, err := remote.OpenSource("a1", 5)
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != string(data[5:]) {
		t.Errorf("range pull mismatch: %q want %q", string(got), string(data[5:]))
	}
}
