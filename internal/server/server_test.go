package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/model"
)

func newTestNode(t *testing.T, discover bool) *Node {
	t.Helper()
	dir := t.TempDir()
	db := filepath.Join(dir, "s.db")
	blob := filepath.Join(dir, "blob")
	if err := os.Mkdir(blob, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		NodeID:    "n1",
		Secret:    "sec",
		ListenAddr: "127.0.0.1:0",
		BlobRoot:  blob,
		DBPath:    db,
	}
	if discover {
		cfg.DiscoverAddr = "127.0.0.1:0" // 同机回环自发现
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return n
}

func waitAddr(t *testing.T, n *Node) string {
	t.Helper()
	for i := 0; i < 100; i++ {
		if a := n.Addr(); a != "" {
			return a
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("node did not bind in time")
	return ""
}

// signedGet 发起带 HMAC 鉴权的 GET 请求。
func signedGet(t *testing.T, base, nodeID, secret, path string) *http.Response {
	t.Helper()
	hdr, err := clusterapi.SignHeaders(nodeID, secret, http.MethodGet, path, nil, time.Now().Unix())
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, base+path, nil)
	req.Header = hdr
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

func TestNodeStateAndTaskAndBlob(t *testing.T) {
	n := newTestNode(t, false)
	defer n.Close()
	// 预置一块磁盘，验证 GetState 反映
	if err := n.Store().SaveDisk(model.Disk{DiskSerial: "SSD-A", Tier: model.TierHot, MountedNodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = n.Run(ctx) }()
	base := "http://" + waitAddr(t, n)

	// 1) GET /state 带鉴权 → 200，nodeID 正确
	resp := signedGet(t, base, "n1", "sec", "/api/cluster/state")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("state status=%d", resp.StatusCode)
	}
	var st clusterapi.StatePayload
	_ = json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	if st.NodeID != "n1" {
		t.Errorf("state nodeID=%q", st.NodeID)
	}
	if len(st.Disks) != 1 || st.Disks[0].DiskSerial != "SSD-A" {
		t.Errorf("state disks=%+v", st.Disks)
	}

	// 2) POST /task 带鉴权 → 入库
	body, _ := json.Marshal(clusterapi.Task{TaskID: "t1", Type: "MIGRATION"})
	hdr, _ := clusterapi.SignHeaders("n1", "sec", http.MethodPost, "/api/cluster/task", body, time.Now().Unix())
	req, _ := http.NewRequest(http.MethodPost, base+"/api/cluster/task", bytes.NewReader(body))
	req.Header = hdr
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("task status=%d", resp2.StatusCode)
	}
	resp2.Body.Close()
	tasks, _ := n.Store().ListTasks()
	if len(tasks) != 1 || tasks[0].TaskID != "t1" {
		t.Errorf("task not persisted: %+v", tasks)
	}

	// 3) GET /blob/a1 带鉴权且文件存在 → 200
	blobDir := n.cfg.BlobRoot
	_ = os.WriteFile(filepath.Join(blobDir, "a1"), []byte("payload-x"), 0o644)
	resp3 := signedGet(t, base, "n1", "sec", "/api/cluster/blob/a1")
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("blob status=%d", resp3.StatusCode)
	}
	b, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	if string(b) != "payload-x" {
		t.Errorf("blob body=%q", string(b))
	}

	// 4) 未带鉴权 → 401
	bad, _ := http.Get(base + "/api/cluster/state")
	if bad.StatusCode != http.StatusUnauthorized {
		t.Errorf("unsigned state should be 401, got %d", bad.StatusCode)
	}
	bad.Body.Close()
}

func TestNodeDiscoverySelfSeen(t *testing.T) {
	n := newTestNode(t, true)
	defer n.Close()
	// 缩短发现间隔以便快速自发现
	n.cfg.DiscoverInterval = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = n.Run(ctx) }()
	_ = waitAddr(t, n)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(n.Registry().Peers()) >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("self beacon not seen in registry: %+v", n.Registry().Peers())
}
