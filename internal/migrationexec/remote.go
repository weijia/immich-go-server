package migrationexec

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/migration"
)

// RemoteSource 经 HMAC 鉴权的集群 blob 端点拉取源 blob（§9.1）。
// 通过 Range 实现字节级续传：OpenSource(offset) 发起 "bytes=<offset>-" 请求。
type RemoteSource struct {
	BaseURL string // 远端节点基址，如 http://node-b:8080
	NodeID  string // 本节点在集群中的身份（用于签名）
	Secret  string // 共享集群密钥
	Client  *http.Client
	Now     func() int64
}

func (r *RemoteSource) client() *http.Client {
	if r.Client != nil {
		return r.Client
	}
	return http.DefaultClient
}

func (r *RemoteSource) now() int64 {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now().Unix()
}

func (r *RemoteSource) sign(method, path string) http.Header {
	hdr, _ := clusterapi.SignHeaders(r.NodeID, r.Secret, method, path, nil, r.now())
	return hdr
}

func (r *RemoteSource) url(assetID string) string {
	return strings.TrimRight(r.BaseURL, "/") + "/api/cluster/blob/" + assetID
}

// StatSource 经 "bytes=0-0" 探测总大小（从 Content-Range 解析），不存在返回 ok=false。
func (r *RemoteSource) StatSource(assetID string) (int64, bool) {
	path := "/api/cluster/blob/" + assetID
	hdr := r.sign(http.MethodGet, path)
	hdr.Set("Range", "bytes=0-0")
	req, _ := http.NewRequest(http.MethodGet, r.url(assetID), nil)
	req.Header = hdr
	resp, err := r.client().Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, false
	}
	if resp.StatusCode != http.StatusPartialContent {
		return 0, false
	}
	n := parseContentRangeTotal(resp.Header.Get("Content-Range"))
	if n < 0 {
		return 0, false
	}
	return n, true
}

// OpenSource 从 offset 起拉取字节流；offset<=0 取全量（仍以 Range 请求以走续传通道）。
func (r *RemoteSource) OpenSource(assetID string, offset int64) (io.ReadCloser, error) {
	path := "/api/cluster/blob/" + assetID
	hdr := r.sign(http.MethodGet, path)
	if offset <= 0 {
		hdr.Set("Range", "bytes=0-")
	} else {
		hdr.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}
	req, _ := http.NewRequest(http.MethodGet, r.url(assetID), nil)
	req.Header = hdr
	resp, err := r.client().Do(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusNotFound:
		resp.Body.Close()
		return nil, fmt.Errorf("blob not found: %s", assetID)
	case http.StatusPartialContent, http.StatusOK:
		return resp.Body, nil
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status %d fetching %s", resp.StatusCode, assetID)
	}
}

// parseContentRangeTotal 从 "bytes 0-0/<size>" 解析总大小。
func parseContentRangeTotal(cr string) int64 {
	const pfx = "bytes "
	if !strings.HasPrefix(cr, pfx) {
		return -1
	}
	rest := cr[len(pfx):]
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return -1
	}
	total := rest[slash+1:]
	if total == "*" {
		return -1
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// RemoteBlobStore 组合：源来自远端节点（RemoteSource），目标与清单落在本地磁盘。
// 用于执行跨节点迁移（§6.5 / §9.1）。
type RemoteBlobStore struct {
	src *RemoteSource
	tgt BlobStore
}

// NewRemoteBlobStore 构造跨节点执行后端。
func NewRemoteBlobStore(src *RemoteSource, target BlobStore) BlobStore {
	return &RemoteBlobStore{src: src, tgt: target}
}

func (r *RemoteBlobStore) StatSource(assetID string) (int64, bool) {
	return r.src.StatSource(assetID)
}

func (r *RemoteBlobStore) OpenSource(assetID string, offset int64) (io.ReadCloser, error) {
	return r.src.OpenSource(assetID, offset)
}

func (r *RemoteBlobStore) CreateTarget(assetID string) (io.WriteCloser, error) {
	return r.tgt.CreateTarget(assetID)
}

func (r *RemoteBlobStore) OpenTargetAppend(assetID string) (io.WriteCloser, int64, error) {
	return r.tgt.OpenTargetAppend(assetID)
}

func (r *RemoteBlobStore) RemoveTarget(assetID string) error { return r.tgt.RemoveTarget(assetID) }

func (r *RemoteBlobStore) ReadManifest() (migration.Manifest, bool, error) {
	return r.tgt.ReadManifest()
}

func (r *RemoteBlobStore) WriteManifest(m migration.Manifest) error { return r.tgt.WriteManifest(m) }

func (r *RemoteBlobStore) RemoveManifest() error { return r.tgt.RemoveManifest() }
