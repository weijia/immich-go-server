package balancer

import (
	"testing"

	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/model"
)

const giB = int64(1) << 30

func TestTargetTier(t *testing.T) {
	cfg := config.Default()
	cases := []struct {
		temp float64
		want model.Tier
	}{
		{0.3, model.TierCold},
		{0.5, model.TierWarm},
		{0.9, model.TierHot},
	}
	for _, c := range cases {
		if got := TargetTier(c.temp, cfg); got != c.want {
			t.Fatalf("temp %.1f: want %s got %s", c.temp, c.want, got)
		}
	}
}

func TestMigrationGain(t *testing.T) {
	if g := MigrationGain(model.TierHot, model.TierCold, 100); g != 200 {
		t.Fatalf("want 200 got %d", g)
	}
	if g := MigrationGain(model.TierWarm, model.TierWarm, 100); g != 0 {
		t.Fatalf("want 0 got %d", g)
	}
}

func TestPlanMigration(t *testing.T) {
	cfg := config.Default()
	dir := model.Directory{DirKey: "2026/06", DiskSerial: "SRC", Tier: model.TierHot, Temperature: 0.3, TotalBytes: 50 * giB}
	candidates := []model.Disk{
		{DiskSerial: "COLD1", CapacityBytes: 1000 * giB, FreeBytes: 900 * giB, Tier: model.TierCold, OnlineSeconds: 100},
	}
	p, ok := PlanMigration(dir, candidates, cfg)
	if !ok {
		t.Fatal("expected plan")
	}
	if p.ToDisk != "COLD1" || p.FromDisk != "SRC" {
		t.Fatalf("unexpected plan %+v", p)
	}
	if p.Gain != 2*50*giB {
		t.Fatalf("gain want %d got %d", 2*50*giB, p.Gain)
	}
}

func TestPlanMigrationNoMove(t *testing.T) {
	cfg := config.Default()
	dir := model.Directory{DirKey: "2026/06", DiskSerial: "SRC", Tier: model.TierCold, Temperature: 0.3, TotalBytes: 50 * giB}
	candidates := []model.Disk{{DiskSerial: "COLD1", CapacityBytes: 1000 * giB, FreeBytes: 900 * giB, Tier: model.TierCold}}
	if _, ok := PlanMigration(dir, candidates, cfg); ok {
		t.Fatal("already in correct tier, should not migrate")
	}
}

func TestPlanMigrationNoCandidate(t *testing.T) {
	cfg := config.Default()
	dir := model.Directory{DirKey: "2026/06", DiskSerial: "SRC", Tier: model.TierHot, Temperature: 0.3, TotalBytes: 50 * giB}
	candidates := []model.Disk{{DiskSerial: "COLD1", CapacityBytes: 10 * giB, FreeBytes: 1 * giB, Tier: model.TierCold}}
	if _, ok := PlanMigration(dir, candidates, cfg); ok {
		t.Fatal("no space, should fail")
	}
}

func TestSelectReplicaTarget(t *testing.T) {
	cfg := config.Default()
	asset := model.Asset{AssetID: "a1", SizeBytes: 1 * giB}
	existing := []model.Replica{{AssetID: "a1", DiskSerial: "X", NodeID: "nodeA"}}
	allDisks := []model.Disk{
		{DiskSerial: "X", CapacityBytes: 100 * giB, FreeBytes: 50 * giB, Tier: model.TierHot, MountedNodeID: "nodeA"},
		{DiskSerial: "Y", CapacityBytes: 100 * giB, FreeBytes: 50 * giB, Tier: model.TierCold, MountedNodeID: "nodeB"},
		{DiskSerial: "Z", CapacityBytes: 100 * giB, FreeBytes: 50 * giB, Tier: model.TierHot, MountedNodeID: "nodeA"},
	}
	d, ok := SelectReplicaTarget(asset, existing, allDisks, cfg)
	if !ok {
		t.Fatal("expected target")
	}
	if d.DiskSerial != "Y" {
		t.Fatalf("expected cross-node cold Y, got %s", d.DiskSerial)
	}
}
