package main

import (
	"fmt"

	"github.com/weijia/immich-go-server/internal/balancer"
	"github.com/weijia/immich-go-server/internal/config"
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
	fmt.Println("immich-go-server core booted")
}
