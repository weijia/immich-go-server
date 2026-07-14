package clusterapi

import (
	"io"
	"os"
	"path/filepath"
	"strings"
)

// FileSystemBlobSource 基于本地文件系统的 BlobSource 实现（生产用，§9.1）。
// assetID 映射到 Root 目录下的文件；assetID 中的路径分隔符会被视为非法并拒绝，
// 以避免目录穿越。
type FileSystemBlobSource struct {
	Root string
}

// cleanAssetID 规整并校验 assetID，防止目录穿越。
func cleanAssetID(id string) (string, bool) {
	if id == "" || strings.ContainsRune(id, os.PathSeparator) || strings.Contains(id, "..") {
		return "", false
	}
	return id, true
}

// StatBlob 返回 blob 大小；校验和不在此计算（开销大），由接收端另行校验。
func (f FileSystemBlobSource) StatBlob(assetID string) (int64, string, bool) {
	id, ok := cleanAssetID(assetID)
	if !ok {
		return 0, "", false
	}
	fi, err := os.Stat(filepath.Join(f.Root, id))
	if err != nil {
		return 0, "", false
	}
	return fi.Size(), "", true
}

// OpenBlob 从 offset 起打开文件供流式读取（支持 Range 续传）。
func (f FileSystemBlobSource) OpenBlob(assetID string, offset int64) (io.ReadCloser, error) {
	id, ok := cleanAssetID(assetID)
	if !ok {
		return nil, os.ErrInvalid
	}
	file, err := os.Open(filepath.Join(f.Root, id))
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return file, nil
}
