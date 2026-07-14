package diskid

import (
	"path/filepath"
	"testing"
)

func TestReadOrCreateDiskID(t *testing.T) {
	dir := t.TempDir()
	first, err := ReadOrCreateDiskID(dir, "nodeA")
	if err != nil {
		t.Fatal(err)
	}
	if first.DiskID == "" {
		t.Fatal("empty disk id")
	}
	second, err := ReadOrCreateDiskID(dir, "nodeB")
	if err != nil {
		t.Fatal(err)
	}
	if second.DiskID != first.DiskID {
		t.Fatal("should reuse existing disk id, not regenerate")
	}
	if second.HostNodeID != "nodeA" {
		t.Fatalf("host node id should remain original, got %s", second.HostNodeID)
	}
}

func TestDiskStatsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	in := DiskStatsFile{DiskID: "abc", OnlineSeconds: 123, FirstSeenAt: 1, LastTickAt: 2, UpdatedAt: 3}
	if err := WriteDiskStats(dir, in); err != nil {
		t.Fatal(err)
	}
	out, ok, err := ReadDiskStats(dir)
	if err != nil || !ok {
		t.Fatalf("read failed ok=%v err=%v", ok, err)
	}
	if out != in {
		t.Fatalf("round trip mismatch %+v != %+v", out, in)
	}
	if _, ok2, _ := ReadDiskStats(filepath.Join(dir, "nope")); ok2 {
		t.Fatal("expected missing file to return ok=false")
	}
}
