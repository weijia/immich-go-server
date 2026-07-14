package diskid

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// DiskIDFile 磁盘身份文件 .immich-disk-id（§11.2），只写一次。
type DiskIDFile struct {
	DiskID      string `json:"diskId"`
	GeneratedAt int64  `json:"generatedAt"`
	HostNodeID  string `json:"hostNodeId"`
	Label       string `json:"label"`
}

// DiskStatsFile 磁盘统计落盘文件 .immich-disk-stats（§11.4），频繁更新。
type DiskStatsFile struct {
	DiskID        string `json:"diskId"`
	OnlineSeconds int64  `json:"onlineSeconds"`
	FirstSeenAt   int64  `json:"firstSeenAt"`
	LastTickAt    int64  `json:"lastTickAt"`
	UpdatedAt     int64  `json:"updatedAt"`
}

const idName = ".immich-disk-id"
const statsName = ".immich-disk-stats"

// ReadOrCreateDiskID 读取已有 disk-id；不存在则生成并落盘（§11.2/§11.3 认领流程）。
// 已存在时复用，hostNodeId 不随迁移改写。
func ReadOrCreateDiskID(dir, nodeID string) (DiskIDFile, error) {
	p := filepath.Join(dir, idName)
	if data, err := os.ReadFile(p); err == nil {
		var f DiskIDFile
		if json.Unmarshal(data, &f) == nil && f.DiskID != "" {
			return f, nil
		}
	}
	f := DiskIDFile{
		DiskID:      genUUID(),
		GeneratedAt: time.Now().UnixMilli(),
		HostNodeID:  nodeID,
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return f, err
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		return f, err
	}
	return f, nil
}

// WriteDiskStats 写入磁盘统计落盘文件（§11.4）。
func WriteDiskStats(dir string, stats DiskStatsFile) error {
	p := filepath.Join(dir, statsName)
	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

// ReadDiskStats 读取磁盘统计落盘文件；文件缺失时 ok=false。
func ReadDiskStats(dir string) (DiskStatsFile, bool, error) {
	p := filepath.Join(dir, statsName)
	data, err := os.ReadFile(p)
	if err != nil {
		return DiskStatsFile{}, false, nil
	}
	var s DiskStatsFile
	if err := json.Unmarshal(data, &s); err != nil {
		return DiskStatsFile{}, false, err
	}
	return s, true, nil
}

func genUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b)
}
