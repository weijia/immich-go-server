package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/coordinator"
	"github.com/weijia/immich-go-server/internal/diskid"
	"github.com/weijia/immich-go-server/internal/model"
	"github.com/weijia/immich-go-server/internal/server"
)

func main() {
	nodeID := envOr("NODE_ID", "node-local")
	secret := envOr("CLUSTER_SECRET", "dev-secret")
	listen := envOr("LISTEN", "127.0.0.1:8080")
	dbPath := envOr("DB_PATH", "immich-go.db")
	blobRoot := envOr("BLOB_ROOT", "./blobs")
	discover := envOr("DISCOVER_ADDR", "") // 例：239.0.0.1:9999
	diskDirs := splitList(envOr("DISK_DIRS", ""))
	claimGrace := int64(envOrInt("CLAIM_GRACE_SEC", 3600))

	base := server.Config{
		NodeID:   nodeID,
		Secret:   secret,
		ListenAddr: listen,
		BlobRoot:  blobRoot,
		DBPath:    dbPath,
		DiscoverAddr: discover,
	}
	if len(diskDirs) > 0 {
		base.OnTick = makeTick(nodeID, diskDirs, claimGrace)
	}

	node, err := server.New(base)
	if err != nil {
		log.Fatalf("init node: %v", err)
	}
	defer node.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := node.Run(ctx); err != nil {
			log.Printf("node stopped: %v", err)
		}
	}()

	addr := node.Addr()
	if addr == "" {
		// Run 已异步启动，稍等监听就绪
		for i := 0; i < 100; i++ {
			if a := node.Addr(); a != "" {
				addr = a
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	log.Printf("immich-go-server node %q listening on %s (discovery=%q)", nodeID, addr, discover)
	<-ctx.Done()
	log.Println("shutting down")
}

// makeTick 返回每轮 Tick 执行的逻辑：认领本地磁盘 + 运行一次调度均衡。
func makeTick(nodeID string, diskDirs []string, claimGrace int64) func(context.Context, *server.Node) {
	return func(ctx context.Context, n *server.Node) {
		now := time.Now().Unix()
		for _, dir := range diskDirs {
			if err := claimDisk(n, dir, nodeID, now, claimGrace); err != nil {
				log.Printf("claim %s: %v", dir, err)
			}
		}
		// 跨节点聚合：拉取 peer 状态得到全局磁盘视图，再让 Coordinator 跨节点调度；
		// 联邦失败（如全部 peer 不可达）则回退到本节点本地视图。
		gv, err := n.Federate(ctx)
		var repo coordinator.Repository = n.Store()
		if err != nil {
			log.Printf("federate: %v (fallback to local view)", err)
		} else {
			repo = n.GlobalRepository(gv)
			if gv.Coordinator != nodeID {
				log.Printf("coordinator is %s (self=%s)", gv.Coordinator, nodeID)
			}
		}
		cond := coordinator.New(repo, config.Default())
		if emitted, err := cond.RunBalancingCycle(); err != nil {
			log.Printf("balancing cycle: %v", err)
		} else if emitted > 0 {
			log.Printf("balancing emitted %d tasks", emitted)
		}
	}
}

// claimDisk 对每个本地磁盘目录：生成/读取 disk-id、落盘统计、
// 并在 store 中认领（或续占）该盘（§11.2 / §11.3 / §11.4）。
func claimDisk(n *server.Node, dir, nodeID string, now, grace int64) error {
	idFile, err := diskid.ReadOrCreateDiskID(dir, nodeID)
	if err != nil {
		return fmt.Errorf("disk-id: %w", err)
	}
	stats, ok, err := diskid.ReadDiskStats(dir)
	if err != nil {
		return fmt.Errorf("disk-stats: %w", err)
	}
	if !ok {
		stats = diskid.DiskStatsFile{DiskID: idFile.DiskID, FirstSeenAt: now, LastTickAt: now}
	}
	disk := model.Disk{
		DiskSerial: idFile.DiskID,
		Label:      idFile.Label,
		Tier:       model.TierHot, // 真实环境按 SMART/容量判定；此处默认
		FirstSeenAt: stats.FirstSeenAt,
		LastSeenAt: now,
	}
	if err := n.Store().SaveDisk(disk); err != nil {
		return fmt.Errorf("save disk: %w", err)
	}
	claimed, err := n.Store().ClaimOrTouchDisk(idFile.DiskID, nodeID, now, grace)
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	stats.OnlineSeconds = claimed.OnlineSeconds
	stats.LastTickAt = now
	stats.UpdatedAt = now
	if err := diskid.WriteDiskStats(dir, stats); err != nil {
		return fmt.Errorf("write stats: %w", err)
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
