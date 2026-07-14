package store

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/weijia/immich-go-server/internal/claim"
	"github.com/weijia/immich-go-server/internal/clusterapi"
	"github.com/weijia/immich-go-server/internal/model"
)

// Store 是单节点本地 SQLite 仓储（§8），同时实现 clusterapi.StateProvider 接口，
// 作为 ClusterApi 与 Coordinator 的真实后端。每节点一份库，Coordinator 通过 API 聚合。
type Store struct {
	db     *sql.DB
	nodeID string
}

// schema 建表（disk / node / replica / directory / asset / task）。
// disk 增加 blob_root（每磁盘仓库根）；asset 增加 original_path（保留摄入前原路径）。
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
  suspect        INTEGER,
  blob_root      TEXT DEFAULT ''
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
  access_score REAL,
  last_eval_at INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS asset (
  asset_id      TEXT PRIMARY KEY,
  size_bytes    INTEGER,
  checksum      TEXT,
  dir_key       TEXT,
  original_path TEXT DEFAULT ''
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
CREATE TABLE IF NOT EXISTS server_config (
  k TEXT PRIMARY KEY,
  v TEXT
);
CREATE TABLE IF NOT EXISTS asset_meta (
  asset_id          TEXT PRIMARY KEY,
  device_asset_id   TEXT,
  device_id         TEXT,
  file_created_at   TEXT,
  file_modified_at  TEXT,
  is_favorite       INTEGER DEFAULT 0,
  duration          TEXT,
  type              TEXT,
  mime_type         TEXT,
  original_file_name TEXT,
  width             INTEGER,
  height            INTEGER
);
CREATE TABLE IF NOT EXISTS device_asset (
  device_id       TEXT NOT NULL,
  device_asset_id TEXT NOT NULL,
  asset_id        TEXT NOT NULL,
  PRIMARY KEY (device_id, device_asset_id)
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
INSERT INTO disk (disk_serial,label,capacity_bytes,free_bytes,tier,mounted_node_id,online_seconds,first_seen_at,last_seen_at,suspect,blob_root)
VALUES (?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(disk_serial) DO UPDATE SET
  label=excluded.label, capacity_bytes=excluded.capacity_bytes, free_bytes=excluded.free_bytes,
  tier=excluded.tier, mounted_node_id=excluded.mounted_node_id, online_seconds=excluded.online_seconds,
  last_seen_at=excluded.last_seen_at, suspect=excluded.suspect`,
		d.DiskSerial, d.Label, d.CapacityBytes, d.FreeBytes, string(d.Tier),
		d.MountedNodeID, d.OnlineSeconds, d.FirstSeenAt, d.LastSeenAt, boolToInt(d.Suspect), d.BlobRoot)
	return err
}

// GetDisk 读取单块磁盘；不存在返回 ok=false。
func (s *Store) GetDisk(serial string) (model.Disk, bool, error) {
	rows, err := s.db.Query(`SELECT disk_serial,label,capacity_bytes,free_bytes,tier,mounted_node_id,online_seconds,first_seen_at,last_seen_at,suspect,blob_root FROM disk WHERE disk_serial=?`, serial)
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
	rows, err := s.db.Query(`SELECT disk_serial,label,capacity_bytes,free_bytes,tier,mounted_node_id,online_seconds,first_seen_at,last_seen_at,suspect,blob_root FROM disk`)
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

// ClaimOrTouchDisk 认领（或续占）磁盘（§11.3）：若当前无主或原主已离线超 grace，则挂载到本节点；
// 同时按 now-lastSeenAt 累加在线秒数并更新 lastSeenAt。返回最终磁盘状态或错误。
func (s *Store) ClaimOrTouchDisk(serial, nodeID string, now, graceOffline int64) (model.Disk, error) {
	d, ok, err := s.GetDisk(serial)
	if err != nil {
		return model.Disk{}, err
	}
	if !ok {
		return model.Disk{}, fmt.Errorf("disk not found: %s", serial)
	}
	if d.MountedNodeID == "" || d.MountedNodeID == nodeID {
		d.MountedNodeID = nodeID
	} else if claim.EligibleForClaim(d, nodeID, now, graceOffline) {
		d.MountedNodeID = nodeID // 重新认领离线磁盘
	} else {
		return d, fmt.Errorf("disk %s owned by online node %s", serial, d.MountedNodeID)
	}
	d.OnlineSeconds = claim.AccrueOnlineSeconds(d.OnlineSeconds, d.LastSeenAt, now)
	d.LastSeenAt = now
	if err := s.SaveDisk(d); err != nil {
		return model.Disk{}, err
	}
	return d, nil
}

func scanDisk(rows *sql.Rows) (model.Disk, error) {
	var d model.Disk
	var tier, mounted, blobRoot string
	var suspect int
	if err := rows.Scan(&d.DiskSerial, &d.Label, &d.CapacityBytes, &d.FreeBytes, &tier, &mounted, &d.OnlineSeconds, &d.FirstSeenAt, &d.LastSeenAt, &suspect, &blobRoot); err != nil {
		return model.Disk{}, err
	}
	d.Tier = model.Tier(tier)
	d.MountedNodeID = mounted
	d.BlobRoot = blobRoot
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

// SaveDirectory 插入或更新月份目录聚合视图（§8.5 / §8.6）。
// 作为"控制面放置图"的 LWW upsert：仅在传入 LastEvalAt 更新（更大）时才覆盖，
// 避免拉取到的较旧副本把本节点刚写的最新放置冲掉。本地权威写入若不显式带
// LastEvalAt（==0）则默认取当前时刻，保证本地新写总能胜出。
func (s *Store) SaveDirectory(dir model.Directory) error {
	if dir.LastEvalAt == 0 {
		dir.LastEvalAt = time.Now().UnixNano()
	}
	_, err := s.db.Exec(`INSERT INTO directory (dir_key,node_id,disk_serial,tier,temperature,total_bytes,access_score,last_eval_at) VALUES (?,?,?,?,?,?,?,?)
ON CONFLICT(dir_key) DO UPDATE SET node_id=excluded.node_id, disk_serial=excluded.disk_serial, tier=excluded.tier,
  temperature=excluded.temperature, total_bytes=excluded.total_bytes, access_score=excluded.access_score, last_eval_at=excluded.last_eval_at
WHERE excluded.last_eval_at > directory.last_eval_at`,
		dir.DirKey, dir.NodeID, dir.DiskSerial, string(dir.Tier), dir.Temperature, dir.TotalBytes, dir.AccessScore, dir.LastEvalAt)
	return err
}

// ListDirectories 返回所有目录（含跨节点聚合持久化后的全局放置图，§8.6）。
func (s *Store) ListDirectories() ([]model.Directory, error) {
	rows, err := s.db.Query(`SELECT dir_key,node_id,disk_serial,tier,temperature,total_bytes,access_score,last_eval_at FROM directory`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Directory
	for rows.Next() {
		var d model.Directory
		var tier string
		if err := rows.Scan(&d.DirKey, &d.NodeID, &d.DiskSerial, &tier, &d.Temperature, &d.TotalBytes, &d.AccessScore, &d.LastEvalAt); err != nil {
			return nil, err
		}
		d.Tier = model.Tier(tier)
		out = append(out, d)
	}
	return out, nil
}

// ---- asset (§8) ----

// SaveAsset 记录一个资产（size/checksum/dir_key/original_path）。
func (s *Store) SaveAsset(a model.Asset) error {
	_, err := s.db.Exec(`INSERT INTO asset (asset_id,size_bytes,checksum,dir_key,original_path) VALUES (?,?,?,?,?)
ON CONFLICT(asset_id) DO UPDATE SET size_bytes=excluded.size_bytes, checksum=excluded.checksum, dir_key=excluded.dir_key`,
		a.AssetID, a.SizeBytes, a.Checksum, a.DirKey, a.OriginalPath)
	return err
}

// ListAssets 返回所有资产。
func (s *Store) ListAssets() ([]model.Asset, error) {
	rows, err := s.db.Query(`SELECT asset_id,size_bytes,checksum,dir_key,original_path FROM asset`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Asset
	for rows.Next() {
		a, err := scanAsset(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

// ReplicaCount 返回某 asset 的健康（非可疑盘）副本数（§6.5.2 有效副本）。
func (s *Store) ReplicaCount(assetID string) int {
	n, _ := s.CountHealthyReplicas(assetID)
	return n
}

// ---- task (§9.2) ----

// ListTasks 返回所有已记录任务（用于验证 Coordinator 产出）。
func (s *Store) ListTasks() ([]clusterapi.Task, error) {
	return s.listTasks("")
}

// ListPendingTasks 返回处于某状态（如 "QUEUED"）的任务，供 worker 认领执行。
func (s *Store) ListPendingTasks(status string) ([]clusterapi.Task, error) {
	return s.listTasks(status)
}

func (s *Store) listTasks(statusFilter string) ([]clusterapi.Task, error) {
	q := `SELECT task_id,type,dir_key,asset_id,src_disk,dst_disk,status FROM task`
	var rows *sql.Rows
	var err error
	if statusFilter != "" {
		rows, err = s.db.Query(q+` WHERE status=?`, statusFilter)
	} else {
		rows, err = s.db.Query(q)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []clusterapi.Task
	for rows.Next() {
		var t clusterapi.Task
		if err := rows.Scan(&t.TaskID, &t.Type, &t.DirKey, &t.AssetID, &t.SrcDisk, &t.DstDisk, &t.Status); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// UpdateTaskStatus 更新任务状态（QUEUED→RUNNING→DONE/FAILED）。
func (s *Store) UpdateTaskStatus(taskID, status string) error {
	_, err := s.db.Exec(`UPDATE task SET status=? WHERE task_id=?`, status, taskID)
	return err
}

// GetDirectory 读取单个月份目录视图。
func (s *Store) GetDirectory(dirKey string) (model.Directory, bool, error) {
	rows, err := s.db.Query(`SELECT dir_key,node_id,disk_serial,tier,temperature,total_bytes,access_score,last_eval_at FROM directory WHERE dir_key=?`, dirKey)
	if err != nil {
		return model.Directory{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return model.Directory{}, false, nil
	}
	var d model.Directory
	var tier string
	if err := rows.Scan(&d.DirKey, &d.NodeID, &d.DiskSerial, &tier, &d.Temperature, &d.TotalBytes, &d.AccessScore, &d.LastEvalAt); err != nil {
		return model.Directory{}, false, err
	}
	d.Tier = model.Tier(tier)
	return d, true, nil
}

// ListAssetsByDir 返回某目录下的全部资产（迁移执行时遍历源文件用）。
func (s *Store) ListAssetsByDir(dirKey string) ([]model.Asset, error) {
	rows, err := s.db.Query(`SELECT asset_id,size_bytes,checksum,dir_key,original_path FROM asset WHERE dir_key=?`, dirKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Asset
	for rows.Next() {
		a, err := scanAsset(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

// GetAsset 读取单个资产。
func (s *Store) GetAsset(assetID string) (model.Asset, bool, error) {
	rows, err := s.db.Query(`SELECT asset_id,size_bytes,checksum,dir_key,original_path FROM asset WHERE asset_id=?`, assetID)
	if err != nil {
		return model.Asset{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return model.Asset{}, false, nil
	}
	a, err := scanAsset(rows)
	return a, true, err
}

func scanAsset(rows *sql.Rows) (model.Asset, error) {
	var a model.Asset
	var dirKey, orig string
	if err := rows.Scan(&a.AssetID, &a.SizeBytes, &a.Checksum, &dirKey, &orig); err != nil {
		return model.Asset{}, err
	}
	a.DirKey = dirKey
	a.OriginalPath = orig
	return a, nil
}

// UpdateDirectoryDisk 迁移完成后把目录的归属盘更新为目标盘（§6.5.2）。
func (s *Store) UpdateDirectoryDisk(dirKey, diskSerial string) error {
	_, err := s.db.Exec(`UPDATE directory SET disk_serial=? WHERE dir_key=?`, diskSerial, dirKey)
	return err
}

// RelinquishDirectory 目录重宿主时放弃本地陈旧的目录放置记录（§9.x）。
// 升格为全局放置图（§8.6）后，目录行按 dir_key 唯一且记录最新 owner；
// 为避免误删对端刚写的最新放置，仅当本节点仍是该记录的登记 owner 时才删除。
func (s *Store) RelinquishDirectory(dirKey string) error {
	_, err := s.db.Exec(`DELETE FROM directory WHERE dir_key=? AND node_id=?`, dirKey, s.nodeID)
	return err
}

// DeleteReplica 删除某 asset 在某盘上的一份副本记录（§9.x 真实源盘释放）：
// 仅删除 asset@srcDisk 这一行，保留该资产在其他盘上的副本。
func (s *Store) DeleteReplica(assetID, diskSerial string) error {
	_, err := s.db.Exec(`DELETE FROM replica WHERE asset_id=? AND disk_serial=?`, assetID, diskSerial)
	return err
}

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
			OnlineSeconds: d.OnlineSeconds,
		})
	}
	// 目录放置图作为控制面随 /state 上报（§8.6），供对端拉取聚合。
	dirs, _ := s.ListDirectories()
	ddtos := make([]clusterapi.DirectoryDTO, 0, len(dirs))
	for _, d := range dirs {
		ddtos = append(ddtos, clusterapi.DirectoryFromModel(d))
	}
	return clusterapi.StatePayload{NodeID: s.nodeID, Disks: out, Directories: ddtos}
}

// DiskRoot 返回磁盘的物理仓库根（blob_root），ok=false 表示未知或未设置。
// 用于 worker/blob handler 按磁盘解析物理字节路径。
func (s *Store) DiskRoot(diskSerial string) (string, bool) {
	d, ok, _ := s.GetDisk(diskSerial)
	if !ok || d.BlobRoot == "" {
		return "", false
	}
	return d.BlobRoot, true
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

// ---- server_config（服务器身份：serverId / serverToken / serverName） ----

// GetServerConfig 读取一条服务器配置；不存在返回 ok=false。
func (s *Store) GetServerConfig(k string) (string, bool, error) {
	var v string
	err := s.db.QueryRow(`SELECT v FROM server_config WHERE k=?`, k).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// SetServerConfig 写入/覆盖一条服务器配置。
func (s *Store) SetServerConfig(k, v string) error {
	_, err := s.db.Exec(`INSERT INTO server_config (k,v) VALUES (?,?)
ON CONFLICT(k) DO UPDATE SET v=excluded.v`, k, v)
	return err
}

// ---- asset_meta（资产 API 侧元信息） ----

// SaveAssetMeta 写入/覆盖一条资产 API 元信息。
func (s *Store) SaveAssetMeta(m model.AssetMeta) error {
	_, err := s.db.Exec(`INSERT INTO asset_meta
  (asset_id,device_asset_id,device_id,file_created_at,file_modified_at,is_favorite,duration,type,mime_type,original_file_name,width,height)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(asset_id) DO UPDATE SET
  device_asset_id=excluded.device_asset_id, device_id=excluded.device_id,
  file_created_at=excluded.file_created_at, file_modified_at=excluded.file_modified_at,
  is_favorite=excluded.is_favorite, duration=excluded.duration, type=excluded.type,
  mime_type=excluded.mime_type, original_file_name=excluded.original_file_name,
  width=excluded.width, height=excluded.height`,
		m.AssetID, m.DeviceAssetID, m.DeviceID, m.FileCreatedAt, m.FileModifiedAt,
		boolToInt(m.IsFavorite), m.Duration, m.Type, m.MimeType, m.OriginalFileName, m.Width, m.Height)
	return err
}

// GetAssetMeta 读取单资产 API 元信息；不存在返回 ok=false。
func (s *Store) GetAssetMeta(assetID string) (model.AssetMeta, bool, error) {
	var m model.AssetMeta
	var fav int
	err := s.db.QueryRow(`SELECT asset_id,device_asset_id,device_id,file_created_at,file_modified_at,
  is_favorite,duration,type,mime_type,original_file_name,width,height FROM asset_meta WHERE asset_id=?`, assetID).
		Scan(&m.AssetID, &m.DeviceAssetID, &m.DeviceID, &m.FileCreatedAt, &m.FileModifiedAt,
			&fav, &m.Duration, &m.Type, &m.MimeType, &m.OriginalFileName, &m.Width, &m.Height)
	if err == sql.ErrNoRows {
		return model.AssetMeta{}, false, nil
	}
	if err != nil {
		return model.AssetMeta{}, false, err
	}
	m.IsFavorite = fav != 0
	return m, true, nil
}

// ---- device_asset（(deviceId, deviceAssetId) -> assetId 去重映射） ----

// SaveDeviceAsset 记录一条设备资产映射（幂等）。
func (s *Store) SaveDeviceAsset(deviceID, deviceAssetID, assetID string) error {
	_, err := s.db.Exec(`INSERT OR REPLACE INTO device_asset (device_id,device_asset_id,asset_id) VALUES (?,?,?)`,
		deviceID, deviceAssetID, assetID)
	return err
}

// LookupDeviceAssets 返回该设备下、给定 deviceAssetId 列表里已存在映射的 {deviceAssetId: assetId}。
func (s *Store) LookupDeviceAssets(deviceID string, deviceAssetIDs []string) (map[string]string, error) {
	out := map[string]string{}
	if len(deviceAssetIDs) == 0 {
		return out, nil
	}
	rows, err := s.db.Query(`SELECT device_asset_id, asset_id FROM device_asset WHERE device_id=?`, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var daid, aid string
		if err := rows.Scan(&daid, &aid); err != nil {
			return nil, err
		}
		out[daid] = aid
	}
	return out, nil
}

// DeleteDeviceAssets 删除某资产在所有设备下的映射（删除资产时清理）。
func (s *Store) DeleteDeviceAssets(assetID string) error {
	_, err := s.db.Exec(`DELETE FROM device_asset WHERE asset_id=?`, assetID)
	return err
}

// ---- 资产写入 / 删除（仓库即真相） ----

// SaveUploadedAsset 记录一次上传产物：asset 行 + 副本（asset@disk, HEALTHY）+
// 目录聚合（dir_key -> disk，累加 total_bytes）。无盘（diskSerial 空）时仅记 asset+元信息，
// 目录归属留空，等待 scanner / 后续均衡补齐。
func (s *Store) SaveUploadedAsset(a model.Asset, diskSerial string, sizeBytes int64) error {
	if err := s.SaveAsset(a); err != nil {
		return err
	}
	if diskSerial != "" {
		if err := s.AddReplica(model.Replica{
			ReplicaID:  a.AssetID + "@" + diskSerial,
			AssetID:    a.AssetID,
			DiskSerial: diskSerial,
			NodeID:     s.nodeID,
			Checksum:   a.Checksum,
			Status:     "HEALTHY",
		}); err != nil {
			return err
		}
		// 目录聚合：仅当本节点是该 dir_key 的 owner（或首次出现）时才写。
		existing, ok, err := s.GetDirectory(a.DirKey)
		if err == nil && ok && existing.NodeID != s.nodeID {
			return nil
		}
		tier := model.TierWarm
		if d, ok, _ := s.GetDisk(diskSerial); ok {
			tier = d.Tier
		}
		dir := model.Directory{
			DirKey:     a.DirKey,
			NodeID:     s.nodeID,
			DiskSerial: diskSerial,
			Tier:       tier,
			TotalBytes: sizeBytes,
			LastEvalAt: time.Now().UnixNano(),
		}
		if ok && existing.NodeID == s.nodeID {
			dir.TotalBytes = existing.TotalBytes + sizeBytes // 累加，而非覆盖
		}
		if err := s.SaveDirectory(dir); err != nil {
			return err
		}
	}
	return nil
}

// DeleteAsset 删除资产全部元数据：asset 行、所有副本、asset_meta、device 映射。
// 返回被删资产（含 dir_key、checksum），供上层定位物理字节与 sidecar 清理。
func (s *Store) DeleteAsset(assetID string) (model.Asset, bool, error) {
	a, ok, err := s.GetAsset(assetID)
	if err != nil {
		return model.Asset{}, false, err
	}
	if !ok {
		return model.Asset{}, false, nil
	}
	if _, err := s.db.Exec(`DELETE FROM asset WHERE asset_id=?`, assetID); err != nil {
		return model.Asset{}, false, err
	}
	if _, err := s.db.Exec(`DELETE FROM replica WHERE asset_id=?`, assetID); err != nil {
		return model.Asset{}, false, err
	}
	if err := s.DeleteDeviceAssets(assetID); err != nil {
		return model.Asset{}, false, err
	}
	if _, err := s.db.Exec(`DELETE FROM asset_meta WHERE asset_id=?`, assetID); err != nil {
		return model.Asset{}, false, err
	}
	// 若该 dir_key 下已无资产则清理目录记录。
	var cnt int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM asset WHERE dir_key=?`, a.DirKey).Scan(&cnt); err == nil && cnt == 0 {
		_, _ = s.db.Exec(`DELETE FROM directory WHERE dir_key=? AND node_id=?`, a.DirKey, s.nodeID)
	}
	return a, true, nil
}

// ListMountedDisks 返回挂载在本节点、且非可疑的磁盘，按 free_bytes 降序（利于选盘）。
func (s *Store) ListMountedDisks(nodeID string) ([]model.Disk, error) {
	rows, err := s.db.Query(`SELECT disk_serial,label,capacity_bytes,free_bytes,tier,mounted_node_id,online_seconds,first_seen_at,last_seen_at,suspect,blob_root
FROM disk WHERE mounted_node_id=? AND suspect=0 ORDER BY free_bytes DESC`, nodeID)
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

// 编译期保证 Store 满足 clusterapi.StateProvider。
var _ clusterapi.StateProvider = (*Store)(nil)

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
