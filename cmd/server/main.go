package main

import (
	"fmt"

	"github.com/weijia/immich-go-server/internal/balancer"
	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/migration"
	"github.com/weijia/immich-go-server/internal/model"
	"github.com/weijia/immich-go-server/internal/space"
)

func main() {
	cfg := config.Default()

	disks := []model.Disk{
		{DiskSerial: "SSD-A", CapacityBytes: 100 << 30, FreeBytes: 60 << 30, Tier: model.TierHot, OnlineSeconds: 900000},
		{DiskSerial: "HDD-B", CapacityBytes: 1000 << 30, FreeBytes: 900 << 30, Tier: model.TierCold, OnlineSeconds: 100000},
	}

	if d, ok := space.SelectWriteDisk(disks, 10<<20, cfg); ok {
		fmt.Printf("write target: %s\n", d.DiskSerial)
	} else {
		fmt.Println("507 Storage Full")
	}

	fmt.Printf("tier for temp 0.3: %s\n", balancer.TargetTier(0.3, cfg))

	// 演示迁移断点续传决策（§6.5.1）
	src := []model.Asset{
		{AssetID: "a1", Checksum: "c1"},
		{AssetID: "a2", Checksum: "c2"},
		{AssetID: "a3", Checksum: "c3"},
	}
	m := migration.Manifest{TaskID: "mig-1", Completed: []string{"a1"}, Partial: map[string]int64{"a3": 73}}
	for _, act := range migration.ComputeResume(m, src) {
		fmt.Printf("resume %s -> %d\n", act.AssetID, act.Mode)
	}

	fmt.Println("immich-go-server core booted")
}
