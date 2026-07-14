package clusterapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/store"
)

// newAssetServer 构造一个带本地 blob 根的资产测试服务（无磁盘，回退 BlobRoot 单根）。
func newAssetServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.NewStore(dbPath, "node-test")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	blobRoot := t.TempDir()
	h := clusterapi.NewHandler("node-test", "secret", 30, st)
	h.ServerID = "srv-test"
	h.BlobRoot = blobRoot
	h.AssetStore = st
	srv := httptest.NewServer(h.Mux())
	t.Cleanup(func() {
		srv.Close()
		_ = st.Close()
	})
	return srv, blobRoot
}

// postMultipart 发送 multipart 表单（字段 + 单文件 assetData）。
func postMultipart(t *testing.T, url string, fields map[string]string, filename string, content []byte) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for k, v := range fields {
		_ = w.WriteField(k, v)
	}
	fw, err := w.CreateFormFile("assetData", filename)
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	_, _ = fw.Write(content)
	_ = w.Close()
	resp, err := http.Post(url, w.FormDataContentType(), &buf)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func mustPost(t *testing.T, url string, body []byte) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	return resp
}

func mustGet(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	return resp
}

func TestAssetUploadListDownloadDelete(t *testing.T) {
	srv, blobRoot := newAssetServer(t)
	base := srv.URL + "/api/assets"

	// 1) 上传前查重：不存在。
	checkBody, _ := json.Marshal(clusterapi.BulkUploadCheckRequest{DeviceID: "dev1", DeviceAssetIDs: []string{"da1"}})
	resp := mustPost(t, base+"/bulk-upload-check", checkBody)
	var chk clusterapi.BulkUploadCheckResponse
	_ = json.NewDecoder(resp.Body).Decode(&chk)
	resp.Body.Close()
	if len(chk.Results) != 1 || chk.Results[0].Exists {
		t.Fatalf("pre-check should report not-exists, got %+v", chk.Results)
	}

	// 2) 上传（.jpg → image/jpeg → IMAGE）。
	resp = postMultipart(t, base, map[string]string{
		"deviceAssetId": "da1", "deviceId": "dev1",
		"fileCreatedAt": "2026-06-01T10:00:00Z", "fileModifiedAt": "2026-06-01T10:00:00Z",
	}, "photo.jpg", []byte("hello-immich"))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload status = %d", resp.StatusCode)
	}
	var up clusterapi.AssetUploadResponse
	_ = json.NewDecoder(resp.Body).Decode(&up)
	if up.ID == "" || up.Duplicate {
		t.Fatalf("upload response unexpected: %+v", up)
	}
	id := up.ID

	// 物理字节应落在 blobRoot/2026/06/<id>（内容寻址，dir_key 由 fileCreatedAt 推导）。
	phys := filepath.Join(blobRoot, "2026/06", id)
	if _, err := os.Stat(phys); err != nil {
		t.Fatalf("uploaded file not at expected path %s: %v", phys, err)
	}

	// 3) 列表应含该资产。
	resp = mustGet(t, base)
	var list []clusterapi.AssetResponse
	_ = json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	if len(list) != 1 || list[0].ID != id || list[0].Type != "IMAGE" || list[0].FileSize != int64(len("hello-immich")) {
		t.Fatalf("list unexpected: %+v", list)
	}

	// 4) 单资产元信息。
	resp = mustGet(t, base+"/"+id)
	var one clusterapi.AssetResponse
	_ = json.NewDecoder(resp.Body).Decode(&one)
	resp.Body.Close()
	if one.ID != id || one.DeviceAssetID != "da1" || one.MimeType != "image/jpeg" {
		t.Fatalf("get unexpected: %+v", one)
	}

	// 5) 下载原图。
	resp = mustGet(t, base+"/"+id+"/original")
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != "hello-immich" {
		t.Fatalf("original bytes = %q", string(got))
	}

	// 6) 再次上传同 (deviceId, deviceAssetId) → duplicate:true。
	resp = postMultipart(t, base, map[string]string{"deviceAssetId": "da1", "deviceId": "dev1"}, "photo.jpg", []byte("hello-immich"))
	var dup clusterapi.AssetUploadResponse
	_ = json.NewDecoder(resp.Body).Decode(&dup)
	resp.Body.Close()
	if !dup.Duplicate || dup.ID != id {
		t.Fatalf("dedup unexpected: %+v", dup)
	}

	// 7) 删除 → 列表清空，物理字节移除。
	req, _ := http.NewRequest(http.MethodDelete, base+"/"+id, nil)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d", resp2.StatusCode)
	}
	resp2.Body.Close()
	if _, err := os.Stat(phys); !os.IsNotExist(err) {
		t.Fatalf("physical file should be removed after delete")
	}

	resp = mustGet(t, base)
	var after []clusterapi.AssetResponse
	_ = json.NewDecoder(resp.Body).Decode(&after)
	resp.Body.Close()
	if len(after) != 0 {
		t.Fatalf("after delete list should be empty, got %+v", after)
	}
}
