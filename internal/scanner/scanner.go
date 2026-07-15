// Package scanner 把磁盘仓库（blob_root）中的物理资产同步进节点 SQLite 元数据。
//
// 设计原则（§仓库即真相）：物理仓库是真相，SQLite 只是索引/缓存，可由仓库重建。
// Scanner 遍历 blob_root 下各 <dir_key>/.meta.json 分片，把 asset/replica/directory
// 同步进 Store。崩溃后重扫仓库即可恢复一致。物理缺失的资产不会被登记为副本。
package scanner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/weijia/immich-go-server/internal/eval"
	"github.com/weijia/immich-go-server/internal/ingest"
	"github.com/weijia/immich-go-server/internal/model"
	"github.com/weijia/immich-go-server/internal/store"
)

// ScanRepository 扫描单个磁盘仓库（blobRoot），把其中的资产元数据同步进 Store。
// diskSerial/nodeID 用于登记副本与目录 owner。
func ScanRepository(st *store.Store, blobRoot, diskSerial, nodeID string) error {
	return filepath.WalkDir(blobRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() != ".meta.json" {
			return nil
		}
		dir := filepath.Dir(path)
		rel, err := filepath.Rel(blobRoot, dir)
		if err != nil {
			return nil
		}
		dirKey := filepath.ToSlash(rel)
		return syncDir(st, blobRoot, dirKey, diskSerial, nodeID, path)
	})
}

// syncDir 处理单个 dir_key 分片：重建 asset/replica，并聚合 directory。
func syncDir(st *store.Store, blobRoot, dirKey, diskSerial, nodeID, metaPath string) error {
	b, err := os.ReadFile(metaPath)
	if err != nil {
		return err
	}
	var mf ingest.MetaFile
	if err := json.Unmarshal(b, &mf); err != nil {
		return err
	}
	if len(mf.Assets) == 0 {
		return nil
	}

	var total int64
	now := time.Now().Unix()
	for _, a := range mf.Assets {
		// 物理存在性校验：缺失则跳过 replica 登记（保留真相，不臆造副本）。
		phys := filepath.Join(blobRoot, filepath.FromSlash(dirKey), a.AssetID)
		if !exists(phys) {
			continue
		}
		if err := st.SaveAsset(model.Asset{
			AssetID:      a.AssetID,
			SizeBytes:    a.SizeBytes,
			Checksum:     a.Checksum,
			DirKey:       dirKey,
			OriginalPath: a.OriginalPath,
		}); err != nil {
			return err
		}
		if err := st.AddReplica(model.Replica{
			ReplicaID:  a.AssetID + "@" + diskSerial,
			AssetID:    a.AssetID,
			DiskSerial: diskSerial,
			NodeID:     nodeID,
			Checksum:   a.Checksum,
			Status:     "HEALTHY",
		}); err != nil {
			return err
		}
		total += a.SizeBytes
	}

	// 聚合 directory：仅当本节点是该 dir_key 的 owner（或首次出现）时才写，
	// 避免覆盖从对端聚合来的、本节点并非 owner 的目录记录（§8.6 控制面）。
	existing, ok, err := st.GetDirectory(dirKey)
	if err == nil && ok && existing.NodeID != nodeID {
		return nil
	}
	tier := model.TierWarm
	if d, ok, _ := st.GetDisk(diskSerial); ok {
		tier = d.Tier
	}
	temp, acc := eval.Evaluate(dirKey, total, now)
	dir := model.Directory{
		DirKey:      dirKey,
		NodeID:      nodeID,
		DiskSerial:  diskSerial,
		Tier:        tier,
		Temperature: temp,
		AccessScore: acc,
		TotalBytes:  total,
		LastEvalAt:  now,
	}
	return st.SaveDirectory(dir)
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
