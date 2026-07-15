package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/weijia/immich-go-server/internal/cluster"
	"github.com/weijia/immich-go-server/internal/config"
	"github.com/weijia/immich-go-server/internal/coordinator"
	"github.com/weijia/immich-go-server/internal/diskid"
	"github.com/weijia/immich-go-server/internal/model"
	"github.com/weijia/immich-go-server/internal/scanner"
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
	serverName := envOr("SERVER_NAME", "immich-go-server")
	serverURL := envOr("SERVER_URL", "")
	clientDiscover := envOr("CLIENT_DISCOVER_ADDR", ":2284") // 空则禁用客户端发现

	// WebUI 看板令牌：DASHBOARD_TOKEN 固定则用之；否则每次启动随机生成并打印。
	dashboardToken := envOr("DASHBOARD_TOKEN", "")
	if dashboardToken == "" {
		dashboardToken = randomHexToken()
	}

	base := server.Config{
		NodeID:   nodeID,
		Secret:   secret,
		ListenAddr: listen,
		BlobRoot:  blobRoot,
		DBPath:    dbPath,
		DiscoverAddr: discover,
		ServerName: serverName,
		ServerURL: serverURL,
		ClientDiscoverAddr: clientDiscover,
		DashboardToken:     dashboardToken,
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
	log.Printf("immich-go-server node %q listening on %s (cluster-discovery=%q, client-discovery-udp=%s)", nodeID, addr, discover, clientDiscover)
	log.Printf("WebUI 看板令牌 (DASHBOARD_TOKEN): %s", dashboardToken)
	log.Printf("  WebUI 信息端点: GET /api/dashboard/state  （需带 Authorization: Bearer %s 或 ?token=%s）", dashboardToken, dashboardToken)
	<-ctx.Done()
	log.Println("shutting down")
}

// makeTick 返回每轮 Tick 执行的逻辑：认领本地磁盘 + 运行一次调度均衡。
func makeTick(nodeID string, diskDirs []string, claimGrace int64) func(context.Context, *server.Node) {
	scanTick := 0
	return func(ctx context.Context, n *server.Node) {
		now := time.Now().Unix()
		for _, dir := range diskDirs {
			if err := claimDisk(n, dir, nodeID, now, claimGrace); err != nil {
				log.Printf("claim %s: %v", dir, err)
			}
		}
		// 周期扫描各磁盘仓库，把摄入的物理资产同步进元数据（§仓库即真相）。
		// 每 4 个 tick（约 60s）跑一次，避免频繁全扫。
		scanTick++
		if scanTick%4 == 0 {
			disks, err := n.Store().ListDisks()
			if err != nil {
				log.Printf("list disks: %v", err)
			} else {
				for _, d := range disks {
					if d.BlobRoot == "" {
						continue
					}
					if err := scanner.ScanRepository(n.Store(), d.BlobRoot, d.DiskSerial, nodeID); err != nil {
						log.Printf("scan %s: %v", d.DiskSerial, err)
					}
				}
			}
		}
		// 跨节点聚合：拉取 peer 状态得到全局磁盘视图。
		gv, ferr := n.Federate(ctx)
		if ferr != nil {
			log.Printf("federate: %v (skip scheduling)", ferr)
			gv = cluster.GlobalView{SelfNodeID: nodeID} // worker 仍可处理本地任务
		} else {
			// 仅当选定协调者才下发任务；多节点结论一致且幂等，安全。
			if gv.Coordinator == nodeID {
				repo := n.GlobalRepository(gv)
				cond := coordinator.New(repo, config.Default())
				if emitted, err := cond.RunBalancingCycle(); err != nil {
					log.Printf("balancing cycle: %v", err)
				} else if emitted > 0 {
					log.Printf("balancing emitted %d tasks", emitted)
				}
			} else {
				log.Printf("coordinator is %s (self=%s); skipping schedule", gv.Coordinator, nodeID)
			}
		}
		// 本节点 worker 始终执行目标盘在本地的任务（含跨节点拉取的字节搬运）。
		if err := n.Worker(gv).RunOnce(ctx); err != nil {
			log.Printf("worker: %v", err)
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
		BlobRoot:   dir,           // DISK_DIRS 的目录即该盘的物理仓库根（每磁盘一个仓库）
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
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// randomHexToken 生成 32 字节（64 字符）十六进制随机令牌，供 WebUI 看板使用。
func randomHexToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		// rand.Read 仅在系统熵耗尽时失败，极罕见；退化为时间戳兜底（仍可用）。
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
