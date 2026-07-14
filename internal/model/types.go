package model

// Tier 表示磁盘分层（§5.1）。
type Tier string

const (
	TierHot  Tier = "HOT"
	TierWarm Tier = "WARM"
	TierCold Tier = "COLD"
)

// Disk 一块物理磁盘，跨节点以 diskSerial 唯一标识（§4.1 / §2）。
type Disk struct {
	DiskSerial    string
	Label         string
	CapacityBytes int64
	FreeBytes     int64
	Tier          Tier
	MountedNodeID string
	OnlineSeconds int64
	OnlineScore   float64 // 由 OnlineSeconds 派生的可靠性评分（§4.2 / §5.1）
	FirstSeenAt   int64
	LastTickAt    int64
	LastSeenAt    int64
	Suspect       bool
}

// Node 一个运行中的服务器实例（§2）。
type Node struct {
	NodeID       string
	NodeURL      string
	BatteryLevel int
	OnlineScore  float64
	Disks        []Disk
}

// Asset 一个照片/视频资产（§2）。
type Asset struct {
	AssetID   string
	SizeBytes int64
	Checksum  string
	DirKey    string
}

// Replica 某 asset 在某磁盘上的一份物理拷贝（§2 / §8.3）。
type Replica struct {
	ReplicaID  string
	AssetID    string
	DiskSerial string
	NodeID     string
	Checksum   string
	Status     string // PENDING / HEALTHY
}

// Directory 月份目录聚合视图（§6 / §8.5），迁移决策的基本单元。
type Directory struct {
	DirKey       string
	NodeID       string
	DiskSerial   string
	Tier         Tier
	Temperature  float64
	TotalBytes   int64
	AccessScore  float64
}
