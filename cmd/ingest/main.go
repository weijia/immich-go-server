// Command ingest 把本地文件目录直接加入管理库：
// 扫描 --path 下所有文件，移动到目标磁盘仓库（--disk 对应的 blob_root），
// 按时间规律（YYYY/MM）分目录存放，并写 sidecar；随后跑 scanner 把元数据同步进节点库。
//
// 用法：
//   ingest --path=/Photos --disk=D1 --db=immich-go.db --node-id=node-local --mode=move
// 若 disk 表已设置 blob_root，可省略 --blob-root；否则用 --blob-root 显式指定仓库根。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/weijia/immich-go-server/internal/ingest"
	"github.com/weijia/immich-go-server/internal/scanner"
	"github.com/weijia/immich-go-server/internal/store"
)

func main() {
	var (
		path     = flag.String("path", "", "本地目录根（待摄入）")
		disk     = flag.String("disk", "", "目标磁盘 serial（须在本节点 store 中已认领）")
		blobRoot = flag.String("blob-root", "", "仓库根（默认取 disk 表的 blob_root）")
		dbPath   = flag.String("db", "immich-go.db", "节点 SQLite 路径")
		nodeID   = flag.String("node-id", envOr("NODE_ID", "node-local"), "节点 ID")
		mode     = flag.String("mode", "move", "move | copy")
	)
	flag.Parse()

	if *path == "" || *disk == "" {
		log.Fatal("--path 与 --disk 必填")
	}

	st, err := store.NewStore(*dbPath, *nodeID)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	root := *blobRoot
	if root == "" {
		d, ok, err := st.GetDisk(*disk)
		if err != nil {
			log.Fatalf("get disk: %v", err)
		}
		if !ok {
			log.Fatalf("disk %q not found in store", *disk)
		}
		root = d.BlobRoot
	}
	if root == "" {
		log.Fatal("blob-root 为空：请用 --blob-root 指定，或确保 disk 表已设置 blob_root（如经 DISK_DIRS 认领）")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ing := &ingest.Ingester{TimeOf: ingest.MTimeSource{}}
	rep, err := ing.Run(ctx, *path, root, *mode)
	if err != nil {
		log.Fatalf("ingest: %v", err)
	}
	log.Printf("ingest done: scanned=%d moved=%d copied=%d skipped=%d errors=%d",
		rep.Scanned, rep.Moved, rep.Copied, rep.Skipped, rep.Errors)

	if err := scanner.ScanRepository(st, root, *disk, *nodeID); err != nil {
		log.Fatalf("scan: %v", err)
	}
	log.Printf("scan done: repository %q synced into store (disk=%s)", root, *disk)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var _ = fmt.Sprintf
