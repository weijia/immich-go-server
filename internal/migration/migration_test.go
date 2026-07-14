package migration

import (
	"testing"

	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/model"
)

var srcFiles = []model.Asset{
	{AssetID: "a1", Checksum: "c1", SizeBytes: 100},
	{AssetID: "a2", Checksum: "c2", SizeBytes: 200},
	{AssetID: "a3", Checksum: "c3", SizeBytes: 300}, // 大文件，半成品
	{AssetID: "a4", Checksum: "c4", SizeBytes: 400}, // 全新未传
}

func TestComputeResume(t *testing.T) {
	m := Manifest{
		TaskID:    "mig-1",
		Completed: []string{"a1"},
		Partial:   map[string]int64{"a3": 73},
	}
	acts := ComputeResume(m, srcFiles)
	if len(acts) != len(srcFiles) {
		t.Fatalf("expected %d actions, got %d", len(srcFiles), len(acts))
	}
	want := map[string]ActionMode{"a1": ModeSkip, "a2": ModeWhole, "a3": ModeRange, "a4": ModeWhole}
	for _, a := range acts {
		if want[a.AssetID] != a.Mode {
			t.Errorf("asset %s: want mode %d, got %d", a.AssetID, want[a.AssetID], a.Mode)
		}
	}
	rng := acts[2]
	if rng.AssetID != "a3" || rng.BytesCopied != 73 {
		t.Errorf("range action wrong: %+v", rng)
	}
}

func TestAllCopied(t *testing.T) {
	partial := Manifest{Completed: []string{"a1"}}
	if AllCopied(partial, srcFiles) {
		t.Error("should be false when not all copied")
	}
	full := Manifest{Completed: []string{"a1", "a2", "a3", "a4"}}
	if !AllCopied(full, srcFiles) {
		t.Error("should be true when all in completed")
	}
}

func TestDirChecksumsMatch(t *testing.T) {
	target := []model.Asset{
		{AssetID: "a4", Checksum: "c4"},
		{AssetID: "a2", Checksum: "c2"},
		{AssetID: "a1", Checksum: "c1"},
		{AssetID: "a3", Checksum: "c3"},
	}
	if !DirChecksumsMatch(srcFiles, target) {
		t.Error("same set should match regardless of order")
	}
	corrupt := []model.Asset{
		{AssetID: "a1", Checksum: "WRONG"},
		{AssetID: "a2", Checksum: "c2"},
		{AssetID: "a3", Checksum: "c3"},
		{AssetID: "a4", Checksum: "c4"},
	}
	if DirChecksumsMatch(srcFiles, corrupt) {
		t.Error("mismatched checksum should not match")
	}
}

func TestCanDeleteSource(t *testing.T) {
	cfg := config.Default()
	// 每份都恰好 2 份有效副本
	ok := map[string]int{"a1": 2, "a2": 2, "a3": 2, "a4": 2}
	if !CanDeleteSource(srcFiles, ok, cfg) {
		t.Error("all >=2 should allow delete")
	}
	// a3 只有 1 份有效副本
	bad := map[string]int{"a1": 2, "a2": 2, "a3": 1, "a4": 2}
	if CanDeleteSource(srcFiles, bad, cfg) {
		t.Error("asset below MinReplicas must block delete")
	}
}

func TestRollbackTargets(t *testing.T) {
	m := Manifest{
		Completed: []string{"a1"},
		Partial:   map[string]int64{"a3": 73, "a9": 10},
	}
	got := RollbackTargets(m, "/disk/.x.migrating.json")
	// manifest + 1 completed + 2 partials
	if len(got) != 4 {
		t.Fatalf("expected 4 cleanup targets, got %d: %v", len(got), got)
	}
	if got[0] != "/disk/.x.migrating.json" {
		t.Errorf("manifest path should be first, got %s", got[0])
	}
	has := map[string]bool{}
	for _, g := range got {
		has[g] = true
	}
	if !has["a1"] || !has["a3"] || !has["a9"] {
		t.Errorf("missing completed/partial targets: %v", got)
	}
}
