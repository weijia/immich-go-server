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
