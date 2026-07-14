package claim

import (
	"testing"

	"github.com/weijia/immich-go-server/internal/model"
)

func TestEligibleForClaim(t *testing.T) {
	now := int64(10000)
	grace := int64(3600)

	cases := []struct {
		name  string
		disk  model.Disk
		node  string
		elig bool
	}{
		{"unclaimed", model.Disk{DiskSerial: "D1", LastSeenAt: 0}, "node-A", true},
		{"owned by self", model.Disk{DiskSerial: "D2", MountedNodeID: "node-A", LastSeenAt: now}, "node-A", false},
		{"owned by other, online", model.Disk{DiskSerial: "D3", MountedNodeID: "node-B", LastSeenAt: now}, "node-A", false},
		{"owned by other, offline beyond grace", model.Disk{DiskSerial: "D4", MountedNodeID: "node-B", LastSeenAt: now - 4000}, "node-A", true},
		{"owned by other, just at grace boundary", model.Disk{DiskSerial: "D5", MountedNodeID: "node-B", LastSeenAt: now - 3600}, "node-A", true},
	}
	for _, c := range cases {
		got := EligibleForClaim(c.disk, c.node, now, grace)
		if got != c.elig {
			t.Errorf("%s: EligibleForClaim=%v want %v", c.name, got, c.elig)
		}
	}
}

func TestAccrueOnlineSeconds(t *testing.T) {
	// 从 100 起，过去 60 秒，应累计到 160
	if got := AccrueOnlineSeconds(100, 1000, 1060); got != 160 {
		t.Errorf("accrue = %d want 160", got)
	}
	// 时钟回拨（now < lastSeen）：增量封顶为 0
	if got := AccrueOnlineSeconds(100, 2000, 1900); got != 100 {
		t.Errorf("accrue clock-back = %d want 100", got)
	}
}
