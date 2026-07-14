package config

// Config 集中存放设计文档 §14 的所有可调参数，供各逻辑包共享。
type Config struct {
	MinReplicas            int
	HotFreeTarget          float64
	WarmFreeTarget         float64
	ColdFreeTarget         float64
	HotTempThr             float64
	ColdTempThr            float64
	SafetyMargin           float64
	DiskMinFreeRatio       float64
	DiskMinFreeBytes       int64
	MigrationBytesPerCycle int64
	OfflineSuspectDays     int
	MaxClockSkew           int64
	MigrationMaxRetry      int
	PullInterval           int64
	TempWeights            [3]float64
}

// Default 返回设计文档 §14 的默认配置。
func Default() Config {
	return Config{
		MinReplicas:            2,
		HotFreeTarget:          0.40,
		WarmFreeTarget:         0.20,
		ColdFreeTarget:         0.05,
		HotTempThr:             0.80,
		ColdTempThr:            0.40,
		SafetyMargin:           0.10,
		DiskMinFreeRatio:       0.10,
		DiskMinFreeBytes:       10 * 1024 * 1024 * 1024, // 10 GiB
		MigrationBytesPerCycle: 100 * 1024 * 1024 * 1024,
		OfflineSuspectDays:     30,
		MaxClockSkew:           300,
		MigrationMaxRetry:      5,
		PullInterval:           60,
		TempWeights:            [3]float64{0.4, 0.5, 0.1},
	}
}
