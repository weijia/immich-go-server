package clusterapi

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestFileSystemBlobSource(t *testing.T) {
	dir := t.TempDir()
	data := []byte("filesystem-blob-payload-1234567890")
	if err := os.WriteFile(filepath.Join(dir, "a1"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	src := FileSystemBlobSource{Root: dir}

	// 存在
	size, _, ok := src.StatBlob("a1")
	if !ok || size != int64(len(data)) {
		t.Fatalf("StatBlob a1: size=%d ok=%v", size, ok)
	}

	// 不存在
	if _, _, ok := src.StatBlob("missing"); ok {
		t.Error("missing blob should be not-ok")
	}

	// 路径穿越被拒
	if _, _, ok := src.StatBlob("../escape"); ok {
		t.Error("path traversal should be rejected")
	}

	// 全量读取
	rc, err := src.OpenBlob("a1", 0)
	if err != nil {
		t.Fatalf("OpenBlob: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != string(data) {
		t.Errorf("body mismatch: %q", string(got))
	}

	// Range 续传：从偏移 10 读起
	rc2, err := src.OpenBlob("a1", 10)
	if err != nil {
		t.Fatalf("OpenBlob offset: %v", err)
	}
	got2, _ := io.ReadAll(rc2)
	rc2.Close()
	if string(got2) != string(data[10:]) {
		t.Errorf("offset body mismatch: %q want %q", string(got2), string(data[10:]))
	}
}
