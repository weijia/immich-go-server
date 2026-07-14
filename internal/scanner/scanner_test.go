package scanner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/weijia/immich-go-server/internal/ingest"
	"github.com/weijia/immich-go-server/internal/model"
	"github.com/weijia/immich-go-server/internal/store"
)

func TestScanRepositorySync(t *testing.T) {
	root := t.TempDir()
	blob := filepath.Join(root, "blobs")
	dirKey := "2024/06"
	assetID := "abc123def456"
	assetPath := filepath.Join(blob, dirKey, assetID)
	if err := os.MkdirAll(filepath.Dir(assetPath), 0o755); err != nil {
		t.Fatal(err)
	}
	// 物理字节必须存在，否则 scanner 跳过 replica 登记
	if err := os.WriteFile(assetPath, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	// 写分片 sidecar
	meta := ingest.MetaFile{
		DirKey: dirKey,
		Assets: []ingest.MetaAsset{{
			AssetID:      assetID,
			Checksum:     "cs",
			SizeBytes:    7,
			OriginalPath: "/orig/photo.jpg",
			Kind:         "photo",
		}},
	}
	metaBytes, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(filepath.Join(blob, dirKey, ".meta.json"), metaBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	db := filepath.Join(root, "s.db")
	st, err := store.NewStore(db, "nodeX")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// 该盘已认领，tier=HOT，应被 directory 采用
	if err := st.SaveDisk(model.Disk{DiskSerial: "D1", Tier: model.TierHot, BlobRoot: blob}); err != nil {
		t.Fatal(err)
	}

	if err := ScanRepository(st, blob, "D1", "nodeX"); err != nil {
		t.Fatal(err)
	}

	a, ok, err := st.GetAsset(assetID)
	if err != nil || !ok {
		t.Fatalf("asset not synced: ok=%v err=%v", ok, err)
	}
	if a.OriginalPath != "/orig/photo.jpg" || a.DirKey != dirKey {
		t.Fatalf("asset meta wrong: %+v", a)
	}
	reps, err := st.ListReplicas(assetID)
	if err != nil {
		t.Fatal(err)
	}
	if len(reps) != 1 || reps[0].DiskSerial != "D1" || reps[0].Status != "HEALTHY" {
		t.Fatalf("replica wrong: %+v", reps)
	}
	d, ok, err := st.GetDirectory(dirKey)
	if err != nil || !ok {
		t.Fatalf("directory not synced: ok=%v err=%v", ok, err)
	}
	if d.NodeID != "nodeX" || d.DiskSerial != "D1" || d.Tier != model.TierHot || d.TotalBytes != 7 {
		t.Fatalf("directory wrong: %+v", d)
	}
}

func TestScanSkipsMissingPhysical(t *testing.T) {
	root := t.TempDir()
	blob := filepath.Join(root, "blobs")
	dirKey := "2024/06"
	// sidecar 声称有 asset2，但物理文件不存在 → 不应登记
	meta := ingest.MetaFile{
		DirKey: dirKey,
		Assets: []ingest.MetaAsset{{
			AssetID: "missing-id", Checksum: "cs", SizeBytes: 7, OriginalPath: "/x",
		}},
	}
	metaBytes, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.MkdirAll(filepath.Join(blob, dirKey), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blob, dirKey, ".meta.json"), metaBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	db := filepath.Join(root, "s.db")
	st, err := store.NewStore(db, "nodeX")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if err := ScanRepository(st, blob, "D1", "nodeX"); err != nil {
		t.Fatal(err)
	}
	// 物理缺失 → 资产不应被登记
	if _, ok, _ := st.GetAsset("missing-id"); ok {
		t.Fatal("asset with missing physical bytes must not be registered")
	}
}
