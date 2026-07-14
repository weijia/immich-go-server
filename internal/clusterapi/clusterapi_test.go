package clusterapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/weijia/immich-go-server/internal/crypto"
)

type fakeProvider struct {
	state    StatePayload
	location map[string]string // diskSerial -> mountedNodeID
	regs     []string
	tasks    []Task
}

func (f *fakeProvider) GetState() StatePayload { return f.state }
func (f *fakeProvider) GetDiskLocation(s string) (string, bool) {
	n, ok := f.location[s]
	return n, ok
}
func (f *fakeProvider) RegisterReplica(assetID, diskSerial, checksum string) error {
	f.regs = append(f.regs, assetID+"@"+diskSerial)
	return nil
}
func (f *fakeProvider) SubmitTask(t Task) error {
	f.tasks = append(f.tasks, t)
	return nil
}

const (
	testNode  = "node-A"
	testSecret = "shared-cluster-secret"
)

var fixedNow int64 = 1700000000

func newTestHandler(p StateProvider) *Handler {
	h := NewHandler(testNode, testSecret, 300, p)
	h.Now = func() int64 { return fixedNow }
	return h
}

func doSigned(t *testing.T, h *Handler, method, path string, body []byte) *http.Response {
	t.Helper()
	hdr, err := SignHeaders(testNode, testSecret, method, path, body, fixedNow)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header = hdr
	rec := httptest.NewRecorder()
	h.Mux().ServeHTTP(rec, req)
	return rec.Result()
}

func TestStateOK(t *testing.T) {
	p := &fakeProvider{state: StatePayload{NodeID: testNode, Disks: []DiskState{{DiskSerial: "SSD-A", Tier: "HOT"}}}}
	h := newTestHandler(p)
	resp := doSigned(t, h, http.MethodGet, "/api/cluster/state", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var got StatePayload
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if got.Signature == "" {
		t.Error("state payload should carry signature (§9.5)")
	}
}

func TestStateTamperedBody(t *testing.T) {
	p := &fakeProvider{}
	h := newTestHandler(p)
	// 签名基于 body="{}"，但发送篡改后的 body
	hdr, _ := SignHeaders(testNode, testSecret, http.MethodPost, "/api/cluster/replica/register", []byte(`{}`), fixedNow)
	req := httptest.NewRequest(http.MethodPost, "/api/cluster/replica/register",
		bytes.NewReader([]byte(`{"assetId":"x","diskSerial":"D"}`)))
	req.Header = hdr
	rec := httptest.NewRecorder()
	h.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("tampered body should be 401, got %d", rec.Code)
	}
}

func TestStateReplayNonce(t *testing.T) {
	p := &fakeProvider{}
	h := newTestHandler(p)
	hdr, _ := SignHeaders(testNode, testSecret, http.MethodGet, "/api/cluster/state", nil, fixedNow)
	// 第一次成功
	req1 := httptest.NewRequest(http.MethodGet, "/api/cluster/state", nil)
	req1.Header = hdr
	rec1 := httptest.NewRecorder()
	h.Mux().ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first req should be 200, got %d", rec1.Code)
	}
	// 复用同一 nonce 重放 → 应拒
	req2 := httptest.NewRequest(http.MethodGet, "/api/cluster/state", nil)
	req2.Header = hdr
	rec2 := httptest.NewRecorder()
	h.Mux().ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("replayed nonce should be 401, got %d", rec2.Code)
	}
}

func TestStateClockSkew(t *testing.T) {
	p := &fakeProvider{}
	h := newTestHandler(p)
	// 用远超前的时间戳签名，超出 MaxSkew
	hdr, _ := SignHeaders(testNode, testSecret, http.MethodGet, "/api/cluster/state", nil, fixedNow+1000)
	req := httptest.NewRequest(http.MethodGet, "/api/cluster/state", nil)
	req.Header = hdr
	rec := httptest.NewRecorder()
	h.Mux().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("clock skew should be 401, got %d", rec.Code)
	}
}

func TestDiskLocation(t *testing.T) {
	p := &fakeProvider{location: map[string]string{"SSD-A": "node-B"}}
	h := newTestHandler(p)
	resp := doSigned(t, h, http.MethodGet, "/api/cluster/disk/SSD-A", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var got map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if got["mountedNodeId"] != "node-B" {
		t.Errorf("wrong node %q", got["mountedNodeId"])
	}
	// 未知盘 → 404
	resp404 := doSigned(t, h, http.MethodGet, "/api/cluster/disk/UNKNOWN", nil)
	if resp404.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp404.StatusCode)
	}
}

func TestRegisterReplica(t *testing.T) {
	p := &fakeProvider{}
	h := newTestHandler(p)
	body, _ := json.Marshal(map[string]string{"assetId": "a1", "diskSerial": "SSD-A", "checksum": "c1"})
	resp := doSigned(t, h, http.MethodPost, "/api/cluster/replica/register", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(p.regs) != 1 || p.regs[0] != "a1@SSD-A" {
		t.Errorf("replica not registered: %v", p.regs)
	}
}

func TestSubmitTask(t *testing.T) {
	p := &fakeProvider{}
	h := newTestHandler(p)
	body, _ := json.Marshal(Task{TaskID: "t1", Type: "MIGRATION", DirKey: "2026-06", SrcDisk: "A", DstDisk: "B"})
	resp := doSigned(t, h, http.MethodPost, "/api/cluster/task", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(p.tasks) != 1 || p.tasks[0].TaskID != "t1" {
		t.Errorf("task not submitted: %v", p.tasks)
	}
}

// 验证响应 payload 自带的签名可被独立重算（§9.5）。
func TestStatePayloadSignature(t *testing.T) {
	p := &fakeProvider{state: StatePayload{NodeID: testNode, Disks: []DiskState{{DiskSerial: "SSD-A"}}}}
	h := newTestHandler(p)
	resp := doSigned(t, h, http.MethodGet, "/api/cluster/state", nil)
	raw, _ := io.ReadAll(resp.Body)
	var got StatePayload
	_ = json.Unmarshal(raw, &got)

	// 去掉 signature 后重算，应与原 signature 一致
	got.Signature = ""
	recomputed := crypto.SignPayload(h.Secret, got.NodeID, fixedNow, mustJSON(got))
	if recomputed != payloadSigOf(t, raw) {
		t.Error("payload signature mismatch")
	}
}

func mustJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

// payloadSigOf 从原始响应体读取 signature 字段，用于交叉验证。
func payloadSigOf(t *testing.T, raw []byte) string {
	t.Helper()
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	s, _ := m["signature"].(string)
	return s
}

// ---- blob 拉取端点（§9.1）---- 

// memBlob 内存实现 BlobSource，供测试。
type memBlob struct {
	data map[string][]byte
}

func (m *memBlob) StatBlob(id string) (int64, string, bool) {
	b, ok := m.data[id]
	if !ok {
		return 0, "", false
	}
	return int64(len(b)), fmt.Sprintf("%x", sha256.Sum256(b)), true
}

func (m *memBlob) OpenBlob(id string, off int64) (io.ReadCloser, error) {
	b, ok := m.data[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	if off > int64(len(b)) {
		return nil, io.EOF
	}
	return io.NopCloser(bytes.NewReader(b[off:])), nil
}

func newTestHandlerWithBlob(p StateProvider, b BlobSource) *Handler {
	h := newTestHandler(p)
	h.Source = b
	return h
}

func TestBlobFull(t *testing.T) {
	p := &fakeProvider{}
	b := &memBlob{data: map[string][]byte{"a1": []byte("hello-cluster-blob")}}
	h := newTestHandlerWithBlob(p, b)
	resp := doSigned(t, h, http.MethodGet, "/api/cluster/blob/a1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello-cluster-blob" {
		t.Errorf("unexpected body %q", string(body))
	}
	if resp.Header.Get("Accept-Ranges") != "bytes" {
		t.Error("missing Accept-Ranges")
	}
	wantCS := sha256.Sum256([]byte("hello-cluster-blob"))
	if got := resp.Header.Get("X-Blob-Checksum"); got != hex.EncodeToString(wantCS[:]) {
		t.Errorf("checksum header mismatch: got %s", got)
	}
}

func TestBlobRange(t *testing.T) {
	b := &memBlob{data: map[string][]byte{"a1": []byte("0123456789")}}
	h := newTestHandlerWithBlob(&fakeProvider{}, b)

	cases := []struct {
		rng      string
		wantCode int
		wantLen  int
		wantBody string
	}{
		{"bytes=0-4", http.StatusPartialContent, 5, "01234"},
		{"bytes=5-9", http.StatusPartialContent, 5, "56789"},
		{"bytes=2-", http.StatusPartialContent, 8, "23456789"},
		{"bytes=-3", http.StatusPartialContent, 3, "789"},
		{"bytes=0-99", http.StatusRequestedRangeNotSatisfiable, 0, ""},
	}
	for _, c := range cases {
		hdr, _ := SignHeaders(testNode, testSecret, http.MethodGet, "/api/cluster/blob/a1", nil, fixedNow)
		req := httptest.NewRequest(http.MethodGet, "/api/cluster/blob/a1", nil)
		req.Header = hdr
		req.Header.Set("Range", c.rng)
		rec := httptest.NewRecorder()
		h.Mux().ServeHTTP(rec, req)
		if rec.Code != c.wantCode {
			t.Errorf("%s: code=%d want %d", c.rng, rec.Code, c.wantCode)
			continue
		}
		if c.wantCode == http.StatusPartialContent {
			if rec.Body.String() != c.wantBody {
				t.Errorf("%s: body=%q want %q", c.rng, rec.Body.String(), c.wantBody)
			}
			if rec.Header().Get("Content-Length") != fmt.Sprintf("%d", c.wantLen) {
				t.Errorf("%s: content-length=%s want %d", c.rng, rec.Header().Get("Content-Length"), c.wantLen)
			}
		}
	}
}

func TestBlobNotFound(t *testing.T) {
	h := newTestHandlerWithBlob(&fakeProvider{}, &memBlob{data: map[string][]byte{}})
	resp := doSigned(t, h, http.MethodGet, "/api/cluster/blob/missing", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestBlobNoSource(t *testing.T) {
	// 未配置 Source 的节点应返回 501
	h := newTestHandler(&fakeProvider{})
	resp := doSigned(t, h, http.MethodGet, "/api/cluster/blob/a1", nil)
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d", resp.StatusCode)
	}
}

func TestParseByteRange(t *testing.T) {
	cases := []struct {
		rng     string
		size    int64
		ok      bool
		start   int64
		end     int64
	}{
		{"bytes=0-4", 10, true, 0, 4},
		{"bytes=5-9", 10, true, 5, 9},
		{"bytes=2-", 10, true, 2, 9},
		{"bytes=-3", 10, true, 7, 9},
		{"bytes=0-99", 10, false, 0, 0},
		{"bytes=10-11", 10, false, 0, 0}, // start >= size
		{"bytes=5-4", 10, false, 0, 0},   // end < start
		{"items=0-1", 10, false, 0, 0},   // bad prefix
		{"bytes=-", 10, false, 0, 0},     // empty
		{"bytes", 10, false, 0, 0},
	}
	for _, c := range cases {
		s, e, ok := parseByteRange(c.rng, c.size)
		if ok != c.ok || s != c.start || e != c.end {
			t.Errorf("parseByteRange(%q,%d)=%v,%v,%v want %v,%v,%v", c.rng, c.size, s, e, ok, c.start, c.end, c.ok)
		}
	}
}
