package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(t *testing.T, path, content string, mtime time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func TestIngestMoveRename(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	blob := filepath.Join(root, "blobs")

	// 两个文件，分别落在不同月份（验证按时间 YYYY/MM 分目录）
	writeFile(t, filepath.Join(src, "a.txt"), "hello", time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC))
	writeFile(t, filepath.Join(src, "b.bin"), "world", time.Date(2023, 11, 2, 0, 0, 0, 0, time.UTC))

	ing := &Ingester{TimeOf: MTimeSource{}}
	rep, err := ing.Run(context.Background(), src, blob, "move")
	if err != nil {
		t.Fatal(err)
	}
	if rep.Scanned != 2 || rep.Moved != 2 || rep.Skipped != 0 {
		t.Fatalf("report=%+v", rep)
	}

	// 源文件应被移走（move 删源）
	if exists(filepath.Join(src, "a.txt")) || exists(filepath.Join(src, "b.bin")) {
		t.Fatal("source files should be removed after move")
	}

	// 物理落盘：blobRoot/<YYYY/MM>/<assetID>
	aID := sha256Hex("hello")
	bID := sha256Hex("world")
	if !exists(filepath.Join(blob, "2024/06", aID)) {
		t.Fatalf("a not at 2024/06/%s", aID)
	}
	if !exists(filepath.Join(blob, "2023/11", bID)) {
		t.Fatalf("b not at 2023/11/%s", bID)
	}

	// sidecar 分片存在且含原路径
	meta, err := os.ReadFile(filepath.Join(blob, "2024/06", ".meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) == 0 || !contains(string(meta), aID) || !contains(string(meta), "original_path") {
		t.Fatalf("meta incomplete: %s", meta)
	}
}

func TestIngestCopyKeepsSource(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	blob := filepath.Join(root, "blobs")
	writeFile(t, filepath.Join(src, "a.txt"), "hello", time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC))

	ing := &Ingester{TimeOf: MTimeSource{}}
	rep, err := ing.Run(context.Background(), src, blob, "copy")
	if err != nil {
		t.Fatal(err)
	}
	if rep.Copied != 1 || rep.Moved != 0 {
		t.Fatalf("report=%+v", rep)
	}
	// copy 模式：源保留
	if !exists(filepath.Join(src, "a.txt")) {
		t.Fatal("source should remain under copy mode")
	}
}

func TestIngestDedupByContent(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	blob := filepath.Join(root, "blobs")
	// 同内容、不同文件名 → 同 assetID → 第二个被跳过
	writeFile(t, filepath.Join(src, "x.txt"), "dup", time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	writeFile(t, filepath.Join(src, "y.txt"), "dup", time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))

	ing := &Ingester{TimeOf: MTimeSource{}}
	rep, err := ing.Run(context.Background(), src, blob, "move")
	if err != nil {
		t.Fatal(err)
	}
	// 扫描 2，但只有 1 个被实际移动（另 1 个因目标已存在而跳过并删源）
	if rep.Scanned != 2 || rep.Moved != 1 || rep.Skipped != 1 {
		t.Fatalf("report=%+v", rep)
	}
	id := sha256Hex("dup")
	if !exists(filepath.Join(blob, "2024/06", id)) {
		t.Fatal("dedup target missing")
	}
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
