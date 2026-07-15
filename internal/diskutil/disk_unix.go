//go:build !windows

package diskutil

import "golang.org/x/sys/unix"

// GetSpace 返回 path 所在挂载点的可用字节与总字节（Linux/macOS）。
func GetSpace(path string) (free, total int64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	free = int64(st.Bavail) * int64(st.Bsize)
	total = int64(st.Blocks) * int64(st.Bsize)
	return free, total, nil
}
