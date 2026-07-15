//go:build windows

package diskutil

import "golang.org/x/sys/windows"

// GetSpace 返回 path 所在卷的可用字节与总字节（Windows）。
// 传入目录路径即可，GetDiskFreeSpaceEx 会定位其所在卷。
func GetSpace(path string) (free, total int64, err error) {
	var f, t uint64
	if err := windows.GetDiskFreeSpaceEx(windows.StringToUTF16Ptr(path), &f, &t, nil); err != nil {
		return 0, 0, err
	}
	return int64(f), int64(t), nil
}
