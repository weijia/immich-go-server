package store

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/model"
)

// Store 是单节点本地 SQLite 仓储（§8），同时实现 clusterapi.StateProvider 接口，
// 作为 ClusterApi 与 Coordinator 的真实后端。每节点一份库，Coordinator 通过 API 聚合。
type Store struct {
	db     *sql.DB
	nodeID string
}

// schema 建表（disk / node / replica / directory / task）。
const schema = `
CREATE TABLE IF NOT EXISTS disk (
  disk_serial    TEXT PRIMARY KEY,
  label          TEXT,
  capacity_bytes INTEGER,
  free_bytes     INTEGER,
  tier           TEXT,
  mounted_node_id TEXT,
  online_seconds INTEGER,
  first_seen_at  INTEGER,
  last_seen_at   INTEGER,
  suspect        INTEGER
);
CREATE TABLE IF NOT EXISTS node (
  node_id      TEXT PRIMARY KEY,
  node_url     TEXT,
  battery_level INTEGER,
  online_score REAL
);
CREATE TABLE IF NOT EXISTS replica (
  replica_id TEXT PRIMARY KEY,
  asset_id   TEXT,
  disk_serial TEXT,
  node_id    TEXT,
  checksum   TEXT,
  status     TEXT
);
CREATE TABLE IF NOT EXISTS directory (
  dir_key      TEXT PRIMARY KEY,
  node_id      TEXT,
  disk_serial  TEXT,
  tier         TEXT,
  temperature  REAL,
  total_bytes  INTEGER,
  access_score REAL
);
CREATE TABLE IF NOT EXISTS task (
  task_id   TEXT PRIMARY KEY,
  type      TEXT,
  dir_key   TEXT,
  asset_id  TEXT,
  src_disk  TEXT,
  dst_disk  TEXT,
  created_at INTEGER,
  status    TEXT
);
`

// NewStore 打开（或创建）SQLite 库并初始化表。
func NewStore(path, nodeID string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &Store{db: db, nodeID: nodeID}, nil
}

// Close 关闭底层连接。
func (s *Store) Close() error { return s.db.Close() }

// ---- disk ----

// SaveDisk 插入或更新一块磁盘记录。
func (s *Store) SaveDisk(d model.Disk) error {
	_, err := s.db.Exec(`
INSERT INTO disk (disk_serial,label,capacity_bytes,free_bytes,tier,mounted_node_id,online_seconds,first_seen_at,last_seen_at,suspect)
VALUES (?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(disk_serial) DO UPDATE SET
  label=excluded.label, capacity_bytes=excluded.capacity_bytes, free_bytes=excluded.free_bytes,
  tier=excluded.tier, mounted_node_id=excluded.mounted_node_id, online_seconds=excluded.online_seconds,
  last_seen_at=excluded.last_seen_at, suspect=excluded.suspect`,
		d.DiskSerial, d.Label, d.CapacityBytes, d.FreeBytes, string(d.Tier),
		d.MountedNodeID, d.OnlineSeconds, d.FirstSeenAt, d.LastSeenAt, boolToInt(d.Suspect))
	return err
}

// GetDisk 读取单块磁盘；不存在返回 ok=false。
func (s *Store) GetDisk(serial string) (model.Disk, bool, error) {
	rows, err := s.db.Query(`SELECT disk_serial,label,capacity_bytes,free_bytes,tier,mounted_node_id,online_seconds,first_seen_at,last_seen_at,suspect FROM disk WHERE disk_serial=?`, serial)
	if err != nil {
		return model.Disk{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return model.Disk{}, false, nil
	}
	d, err := scanDisk(rows)
	return d, true, err
}

// ListDisks 返回所有磁盘。
func (s *Store) ListDisks() ([]model.Disk, error) {
	rows, err := s.db.Query(`SELECT disk_serial,label,capacity_bytes,free_bytes,tier,mounted_node_id,online_seconds,first_seen_at,last_seen_at,suspect FROM disk`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Disk
	for rows.Next() {
		d, err := scanDisk(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// UpdateFree 写后校正（§5.4(a)）：更新某盘空闲字节。
func (s *Store) UpdateFree(serial string, freeBytes int64) error {
	_, err := s.db.Exec(`UPDATE disk SET free_bytes=? WHERE disk_serial=?`, freeBytes, serial)
	return err
}

func scanDisk(rows *sql.Rows) (model.Disk, error) {
	var d model.Disk
	var tier, mounted string
	var suspect int
	if err := rows.Scan(&d.DiskSerial, &d.Label, &d.CapacityBytes, &d.FreeBytes, &tier, &mounted, &d.OnlineSeconds, &d.FirstSeenAt, &d.LastSeenAt, &suspect); err != nil {
		return model.Disk{}, err
	}
	d.Tier = model.Tier(tier)
	d.MountedNodeID = mounted
	d.Suspect = suspect != 0
	return d, nil
}

// ---- node ----

// UpsertNode 插入或更新节点。
func (s *Store) UpsertNode(n model.Node) error {
	_, err := s.db.Exec(`INSERT INTO node (node_id,node_url,battery_level,online_score) VALUES (?,?,?,?)
ON CONFLICT(node_id) DO UPDATE SET node_url=excluded.node_url, battery_level=excluded.battery_level, online_score=excluded.online_score`,
		n.NodeID, n.NodeURL, n.BatteryLevel, n.OnlineScore)
	return err
}

// ---- replica ----

// AddReplica 记录一份副本（状态由调用方给定，通常 HEALTHY）。
func (s *Store) AddReplica(r model.Replica) error {
	_, err := s.db.Exec(`INSERT INTO replica (replica_id,asset_id,disk_serial,node_id,checksum,status) VALUES (?,?,?,?,?,?)
ON CONFLICT(replica_id) DO UPDATE SET checksum=excluded.checksum, status=excluded.status`,
		r.ReplicaID, r.AssetID, r.DiskSerial, r.NodeID, r.Checksum, r.Status)
	return err
}

// replicaID 由 asset+disk 决定，保证同一资产同盘唯一（幂等）。
func replicaID(assetID, diskSerial string) string { return assetID + "@" + diskSerial }

// RegisterReplica 供 ClusterApi 调用（§9.2）：登记一份副本，默认 PENDING。
func (s *Store) RegisterReplica(assetID, diskSerial, checksum string) error {
	nodeID := s.nodeID
	return s.AddReplica(model.Replica{
		ReplicaID:  replicaID(assetID, diskSerial),
		AssetID:    assetID,
		DiskSerial: diskSerial,
		NodeID:     nodeID,
		Checksum:   checksum,
		Status:     "PENDING",
	})
}

// ListReplicas 返回某 asset 的所有副本。
func (s *Store) ListReplicas(assetID string) ([]model.Replica, error) {
	rows, err := s.db.Query(`SELECT replica_id,asset_id,disk_serial,node_id,checksum,status FROM replica WHERE asset_id=?`, assetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Replica
	for rows.Next() {
		var r model.Replica
		if err := rows.Scan(&r.ReplicaID, &r.AssetID, &r.DiskSerial, &r.NodeID, &r.Checksum, &r.Status); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// CountHealthyReplicas 统计某 asset 在“非可疑”磁盘上的副本数（§6.5.2 有效副本）。
func (s *Store) CountHealthyReplicas(assetID string) (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM replica r JOIN disk d ON r.disk_serial=d.disk_serial
WHERE r.asset_id=? AND d.suspect=0`, assetID).Scan(&n)
	return n, err
}

// ---- directory ----

// SaveDirectory 插入或更新月份目录聚合视图（§8.5）。
func (s *Store) SaveDirectory(dir model.Directory) error {
	_, err := s.db.Exec(`INSERT INTO directory (dir_key,node_id,disk_serial,tier,temperature,total_bytes,access_score) VALUES (?,?,?,?,?,?,?)
ON CONFLICT(dir_key) DO UPDATE SET node_id=excluded.node_id, disk_serial=excluded.disk_serial, tier=excluded.tier,
  temperature=excluded.temperature, total_bytes=excluded.total_bytes, access_score=excluded.access_score`,
		dir.DirKey, dir.NodeID, dir.DiskSerial, string(dir.Tier), dir.Temperature, dir.TotalBytes, dir.AccessScore)
	return err
}

// ListDirectories 返回所有目录。
func (s *Store) ListDirectories() ([]model.Directory, error) {
	rows, err := s.db.Query(`SELECT dir_key,node_id,disk_serial,tier,temperature,total_bytes,access_score FROM directory`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Directory
	for rows.Next() {
		var d model.Directory
		var tier string
		if err := rows.Scan(&d.DirKey, &d.NodeID, &d.DiskSerial, &tier, &d.Temperature, &d.TotalBytes, &d.AccessScore); err != nil {
			return nil, err
		}
		d.Tier = model.Tier(tier)
		out = append(out, d)
	}
	return out, nil
}

// ---- task (§9.2) ----

// SubmitTask 记录一条集群任务；同一 task_id 幂等（INSERT OR IGNORE）。
func (s *Store) SubmitTask(task clusterapi.Task) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO task (task_id,type,dir_key,asset_id,src_disk,dst_disk,created_at,status)
VALUES (?,?,?,?,?,?,?,?)`,
		task.TaskID, task.Type, task.DirKey, task.AssetID, task.SrcDisk, task.DstDisk, time.Now().Unix(), "QUEUED")
	return err
}

// ---- clusterapi.StateProvider 实现 ----

// GetState 返回集群状态 payload（§9.1）；NodeID 为本节点。
func (s *Store) GetState() clusterapi.StatePayload {
	disks, err := s.ListDisks()
	if err != nil {
		return clusterapi.StatePayload{NodeID: s.nodeID}
	}
	out := make([]clusterapi.DiskState, 0, len(disks))
	for _, d := range disks {
		out = append(out, clusterapi.DiskState{
			DiskSerial:    d.DiskSerial,
			Tier:          string(d.Tier),
			FreeBytes:     d.FreeBytes,
			MountedNodeID: d.MountedNodeID,
		})
	}
	return clusterapi.StatePayload{NodeID: s.nodeID, Disks: out}
}

// GetDiskLocation 返回磁盘当前挂载节点（§9.4）。
func (s *Store) GetDiskLocation(diskSerial string) (string, bool) {
	var nodeID string
	err := s.db.QueryRow(`SELECT mounted_node_id FROM disk WHERE disk_serial=?`, diskSerial).Scan(&nodeID)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		return "", false
	}
	return nodeID, true
}

// 编译期保证 Store 满足 clusterapi.StateProvider。
var _ clusterapi.StateProvider = (*Store)(nil)

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
