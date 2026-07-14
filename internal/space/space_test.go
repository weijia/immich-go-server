package space

import (
	"testing"

	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/model"
)

const giB = int64(1) << 30

func TestHardReserveFloor(t *testing.T) {
	cfg := config.Default()
	// 小盘：比例法得的底线低于绝对底线 → 取绝对 10GiB
	if got := HardReserveFloor(model.Disk{CapacityBytes: 50 * giB}, cfg); got != cfg.DiskMinFreeBytes {
		t.Fatalf("want %d got %d", cfg.DiskMinFreeBytes, got)
	}
	// 大盘：比例法得的底线高于绝对底线 → 取 100GiB
	if got := HardReserveFloor(model.Disk{CapacityBytes: 1000 * giB}, cfg); got != 100*giB {
		t.Fatalf("want %d got %d", 100*giB, got)
	}
}

func TestMeetsHardFloor(t *testing.T) {
	cfg := config.Default()
	d := model.Disk{CapacityBytes: 100 * giB, FreeBytes: 15 * giB}
	if !MeetsHardFloor(d, 4*giB, cfg) {
		t.Fatal("15-4=11GiB >= 10GiB floor, should meet")
	}
	if MeetsHardFloor(d, 6*giB, cfg) {
		t.Fatal("15-6=9GiB < 10GiB floor, should fail")
	}
}

func TestPrecheckSpace(t *testing.T) {
	cfg := config.Default()
	d := model.Disk{CapacityBytes: 100 * giB, FreeBytes: 20 * giB}
	// 目录 10GiB，含余量 11GiB，需再加 10GiB 底线 = 21GiB > 20GiB → 不足
	if PrecheckSpace(d, 10*giB, cfg) {
		t.Fatal("should fail precheck")
	}
	big := model.Disk{CapacityBytes: 1000 * giB, FreeBytes: 900 * giB}
	if !PrecheckSpace(big, 10*giB, cfg) {
		t.Fatal("should pass precheck")
	}
}

func TestSelectWriteDiskPrefersHot(t *testing.T) {
	cfg := config.Default()
	disks := []model.Disk{
		{DiskSerial: "HOT", CapacityBytes: 100 * giB, FreeBytes: 60 * giB, Tier: model.TierHot, OnlineSeconds: 900000},
		{DiskSerial: "COLD", CapacityBytes: 100 * giB, FreeBytes: 90 * giB, Tier: model.TierCold, OnlineSeconds: 100000},
	}
	d, ok := SelectWriteDisk(disks, 10*giB, cfg)
	if !ok || d.DiskSerial != "HOT" {
		t.Fatalf("expected HOT, got %+v ok=%v", d, ok)
	}
}

func TestSelectWriteDiskFallsBackToCold(t *testing.T) {
	cfg := config.Default()
	disks := []model.Disk{
		{DiskSerial: "HOT", CapacityBytes: 100 * giB, FreeBytes: 45 * giB, Tier: model.TierHot},
		{DiskSerial: "WARM", CapacityBytes: 100 * giB, FreeBytes: 25 * giB, Tier: model.TierWarm},
		{DiskSerial: "COLD", CapacityBytes: 100 * giB, FreeBytes: 90 * giB, Tier: model.TierCold},
	}
	d, ok := SelectWriteDisk(disks, 10*giB, cfg)
	if !ok || d.DiskSerial != "COLD" {
		t.Fatalf("expected COLD fallback, got %+v ok=%v", d, ok)
	}
}

func TestSelectWriteDiskFull(t *testing.T) {
	cfg := config.Default()
	disks := []model.Disk{
		{DiskSerial: "X", CapacityBytes: 100 * giB, FreeBytes: 5 * giB, Tier: model.TierHot},
	}
	if _, ok := SelectWriteDisk(disks, 200*giB, cfg); ok {
		t.Fatal("should be 507 / no disk")
	}
}
