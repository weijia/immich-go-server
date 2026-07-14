package cluster

import (
	"testing"

	"github.com/weijia/immich-go-server/internal/model"
)

func TestAggregateDiskStats(t *testing.T) {
	reports := []model.Node{
		{NodeID: "A", Disks: []model.Disk{
			{DiskSerial: "D1", OnlineSeconds: 100, FreeBytes: 10, MountedNodeID: "A", LastSeenAt: 100, FirstSeenAt: 1},
		}},
		{NodeID: "B", Disks: []model.Disk{
			{DiskSerial: "D1", OnlineSeconds: 80, FreeBytes: 20, MountedNodeID: "B", LastSeenAt: 200, FirstSeenAt: 1},
		}},
	}
	m := AggregateDiskStats(reports)
	d, ok := m["D1"]
	if !ok {
		t.Fatal("D1 missing")
	}
	if d.OnlineSeconds != 100 {
		t.Fatalf("onlineSeconds want 100 got %d", d.OnlineSeconds)
	}
	if d.FreeBytes != 20 {
		t.Fatalf("freeBytes want 20 got %d", d.FreeBytes)
	}
	if d.MountedNodeID != "B" {
		t.Fatalf("mounted want B got %s", d.MountedNodeID)
	}
}

func TestElectCoordinator(t *testing.T) {
	nodes := []model.Node{
		{NodeID: "B", OnlineScore: 5},
		{NodeID: "A", OnlineScore: 9},
		{NodeID: "C", OnlineScore: 9},
	}
	if got := ElectCoordinator(nodes); got != "A" {
		t.Fatalf("tie broken by id, want A got %s", got)
	}
}

func TestAssignTiers(t *testing.T) {
	disks := []model.Disk{
		{DiskSerial: "mid", OnlineSeconds: 50},
		{DiskSerial: "hi", OnlineSeconds: 100},
		{DiskSerial: "lo", OnlineSeconds: 10},
	}
	out := AssignTiers(disks)
	bySerial := map[string]model.Tier{}
	for _, d := range out {
		bySerial[d.DiskSerial] = d.Tier
	}
	if bySerial["hi"] != model.TierHot || bySerial["lo"] != model.TierCold || bySerial["mid"] != model.TierWarm {
		t.Fatalf("unexpected tiers %v", bySerial)
	}
}

// TestAggregateDirectoryStats 验证目录放置图 LWW 合并（§8.6 控制面）：
// 同一 dir_key 取 last_eval_at 较大者；时间戳相同取 nodeId 较大者（确定性）。
func TestAggregateDirectoryStats(t *testing.T) {
	dir := func(key, node string, ts int64) model.Directory {
		return model.Directory{DirKey: key, NodeID: node, DiskSerial: "D-" + node, LastEvalAt: ts}
	}
	reports := []model.Node{
		{NodeID: "A", Directories: []model.Directory{dir("2026/06", "A", 100)}},
		{NodeID: "B", Directories: []model.Directory{dir("2026/06", "B", 200)}}, // 更新者胜
		{NodeID: "C", Directories: []model.Directory{dir("2026/07", "C", 1)}},
	}
	m := AggregateDirectoryStats(reports)

	// 2026/06 被 A、B 同时上报：B 的 last_eval_at(200) 更大 → 取 B
	d06, ok := m["2026/06"]
	if !ok {
		t.Fatal("2026/06 missing")
	}
	if d06.NodeID != "B" || d06.LastEvalAt != 200 {
		t.Fatalf("2026/06 want owner=B ts=200, got owner=%s ts=%d", d06.NodeID, d06.LastEvalAt)
	}

	// 2026/07 仅 C 上报
	if d07, ok := m["2026/07"]; !ok || d07.NodeID != "C" {
		t.Fatalf("2026/07 want owner=C, got %+v", m["2026/07"])
	}

	// 平票：last_eval_at 相同，取 nodeId 字典序较大者
	tie := []model.Node{
		{NodeID: "X", Directories: []model.Directory{dir("2026/08", "X", 50)}},
		{NodeID: "Y", Directories: []model.Directory{dir("2026/08", "Y", 50)}},
	}
	mt := AggregateDirectoryStats(tie)
	dt, ok := mt["2026/08"]
	if !ok {
		t.Fatal("2026/08 missing")
	}
	if dt.NodeID != "Y" {
		t.Fatalf("tie should break to Y (larger id), got %s", dt.NodeID)
	}
}
