> 本文档从 `immich-android-server` 迁移至 `immich-go-server` 仓库，作为 Go 实现的分布式存储均衡设计蓝本。

# 多服务器存储均衡设计文档

> 状态：**设计草案（Draft）**，尚未实现，不涉及任何代码改动。
> 关联文档：`discovery-protocol.md`（服务发现）、`storage-structure.md`（单机存储结构）、`client-server-comm.md`。

## 1. 背景与目标

当前 Immich Android Server 为单机架构：文件保存在本机 `Documents/Immich/uploads/YYYY/MM/`，元数据存本机 SQLite。多台设备（手机、平板、旧机、PC）已能通过 UDP 发现协议互相发现，但彼此的存储相互独立，没有协同。

本设计引入 **多服务器存储均衡（Distributed Storage Balancing）**，把局域网内多台服务器组成一个协作集群，实现以下目标：

| 编号 | 需求 | 设计目标 |
|------|------|----------|
| R1 | **空间分配** | 在线时长最长的磁盘尽量腾空（作为"热层/缓存层"），在线时长短的磁盘承载久远/冷数据 |
| R2 | **文件迁移** | 高访问频率的文件迁移到高在线率服务器，低频文件下沉到低在线率服务器 |
| R3 | **副本保证** | 任意文件在集群中至少保留 **2 个副本**，且不在同一物理磁盘 |
| R4 | **磁盘在线时长统计** | 由**挂载磁盘的服务器本地自行统计**（自管自的、不互相更新），负载均衡时各节点各自上报 |

### 1.1 设计原则

| 原则 | 说明 |
|------|------|
| 最终一致 | 局域网内设备频繁上下线，采用最终一致性而非强一致 |
| 去中心优先 | 不强依赖单一主节点；协调者角色可选举、可漂移 |
| 本地自治 | 每个节点独立掌握本机磁盘真实状态，元数据可重建 |
| 数据安全第一 | 副本数不足时，宁可牺牲均衡也要先补副本；迁移必须"先复制后删除" |
| 复用现有机制 | 复用现有 `serverId` / `serverToken` / HMAC 签名 / UDP 发现 |

---

## 2. 核心概念

| 概念 | 定义 |
|------|------|
| **Node（节点）** | 一个运行中的服务器实例，由 `serverId`（UUID）唯一标识 |
| **Disk（磁盘）** | 一块物理存储介质，由**磁盘序列号 `diskSerial`** 唯一标识；一个节点可挂载多块磁盘 |
| **Asset（资产）** | 一张照片/一段视频，由 `assetId`（UUID）唯一标识 |
| **Replica（副本）** | 某个 asset 在某块磁盘上的一份物理拷贝 |
| **Coordinator（协调者）** | 负责运行均衡决策的节点；由集群选举产生，可漂移 |
| **Cluster（集群）** | 共享同一 `clusterSecret` 的一组节点 |

### 2.1 为什么用磁盘序列号而不是节点 ID

- 磁盘可在不同节点间**物理拔插迁移**（如 SD 卡、移动硬盘），序列号跟着磁盘走，节点 ID 不行。
- 同一节点可能挂载多块磁盘，需分别统计。
- "在线时长"本质是**磁盘的可用时长**，而非进程的可用时长，因此以 `diskSerial` 为统计主键最准确。
- 节点重装/换机后，只要磁盘还在，历史在线统计与副本归属不丢失。

---

## 3. 系统架构

```
┌───────────────────────────────────────────────────────────────┐
│                        LAN 集群                                 │
│                                                               │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     │
│   │  Node A     │     │  Node B     │     │  Node C     │     │
│   │ (常在线PC)  │     │ (旧手机)    │     │ (平板)      │     │
│   │             │     │             │     │             │     │
│   │ Disk A1 ●●● │     │ Disk B1 ●   │     │ Disk C1 ●●  │     │
│   │ (SSD 高在线)│     │ (低在线)    │     │ (中在线)    │     │
│   └──────┬──────┘     └──────┬──────┘     └──────┬──────┘     │
│          │                   │                   │            │
│          └───────────────────┼───────────────────┘            │
│                              │                                 │
│                Pull(按需拉取) + Cluster API (HTTP)             │
│                   UDP 发现（复用现有协议）                     │
│                              │                                 │
│                      ┌───────┴────────┐                        │
│                      │  Coordinator   │ (选举产生，本例=Node A) │
│                      │  均衡决策引擎  │                        │
│                      └────────────────┘                        │
└───────────────────────────────────────────────────────────────┘
```

### 3.1 分层

| 层 | 职责 |
|----|------|
| **发现层** | 复用现有 UDP 发现（`discovery-protocol.md`），扩展节点为"集群成员" |
| **成员/状态层** | 去心跳化：UDP 发现维护成员关系；按需拉取各节点状态，拉取可达性即在线判断 |
| **元数据层** | 分布式资产目录：每个 asset 有哪些副本、在哪些磁盘上、访问热度 |
| **决策层** | Coordinator 上运行的均衡引擎（空间分配 R1、迁移 R2、副本 R3） |
| **执行层** | 节点间文件复制/删除（HTTP 拉取 + 校验 + 确认） |

---

## 4. 磁盘在线时长管理（R4）

### 4.1 磁盘身份

每块磁盘首次被节点挂载时，生成一条磁盘记录：

| 字段 | 说明 |
|------|------|
| `diskSerial` | 磁盘序列号（主键，跨节点唯一） |
| `label` | 用户可读标签（如"客厅PC-SSD"） |
| `capacityBytes` | 磁盘总容量 |
| `mountedNodeId` | 当前挂载它的节点 `serverId`（可变） |

> **获取序列号**：Android 需读取块设备信息（受权限限制，回退方案见 §11）；JVM 可用 `wmic`/`lsblk`/`diskutil`；不可得时用 `文件系统 UUID` 或落盘的 `disk-id` 文件兜底（详见 §11.2）。

### 4.2 在线时长统计模型（本地自治）

磁盘在线时长是**本地自治**的：哪台服务器挂载了某块磁盘，就由那台服务器程序自己统计并累加，集群中的其他服务器**不负责更新他人的磁盘时长**，节点之间也**不互相改写**这个值。

计时与结算完全在本机完成：

- **挂载 / 启动**：服务器程序检测到磁盘可读写，开始计时。
- **运行中**：持续累加该磁盘的在线秒数（存于本机 `disk` 表）。
- **卸载 / 关机 / 休眠**：停止计时并结算当前累计值。
- **重新挂载（含迁移到新节点）**：从磁盘上已持久化的累计值继续累加，**不归零**。

```
// 本机服务器程序只对「本机挂载的磁盘」自行累加
if 磁盘当前可读写:
    onlineSeconds += (now - lastTickAt)
    lastTickAt = now
```

> 关键点：累加动作只发生在**挂载该磁盘的那台服务器**上。其它节点无需、也不会修改这个值；它们只会在负载均衡时收到该磁盘的最新累计值（见 §4.3）。

派生指标：

| 指标 | 公式 | 用途 |
|------|------|------|
| **累计在线时长** `onlineSeconds` | 本机计时累加 | 长期可靠性排名 |
| **在线率** `uptimeRatio` | `onlineSeconds / (now - firstSeenAt)` | R1/R2 的核心排序依据 |
| **近期在线率** `recentUptime` | 滑动窗口（如近 7 天）内在线率 | 反映"最近是否常开机" |
| **在线评分** `onlineScore` | `0.7×recentUptime + 0.3×uptimeRatio` | 综合排序，兼顾近期与长期 |

### 4.3 上报与聚合（各自上报，互不更新）

- 在线时长由**本机自治维护**，不做节点间互相更新。
- 负载均衡决策时，每个节点**上报本机所有磁盘**的当前统计：`{diskSerial, onlineSeconds, firstSeenAt, capacityBytes, freeBytes, mountedNodeId}`（其中 `onlineSeconds` 为本机自管的**当前累计值**，非增量）。
- Coordinator（或每个节点本地）**读取并聚合**这些上报值，形成全局 `disk_stats` 视图用于排序 / 分层；聚合是只读的"汇总"，**不会回写、也不会修改任何节点的本地统计**。
- 磁盘物理迁移到新节点时，`mountedNodeId` 更新，但 `diskSerial` 与历史 `onlineSeconds` 随磁盘延续（由 §11.2 的 disk-id 文件或序列号承载）。
- 冲突合并规则：同一 `diskSerial` 被多个来源上报时（如迁移瞬间双节点可见），取 `onlineSeconds` 较大者、`mountedNodeId` 取上报最新者——这只是聚合时的取舍，**不改写任何本地值**。

---

## 5. 存储分层与空间分配（R1）

### 5.1 磁盘分层

按 `onlineScore` 对所有磁盘排序，划分为三层（阈值可配置）：

| 层 | 条件 | 定位 | 空间策略 |
|----|------|------|----------|
| **热层 Hot** | `onlineScore ≥ 0.8` | 常在线，作为主副本/缓存 | **尽量腾空**，只放热点文件 + 主副本，保留高水位空闲 |
| **温层 Warm** | `0.4 ≤ onlineScore < 0.8` | 中等在线 | 常规副本 |
| **冷层 Cold** | `onlineScore < 0.4` | 很少在线 | **承载久远/冷文件**，作为归档副本 |

> R1 的核心："在线时长最多的磁盘空间空出来" = 热层保持高空闲水位；"在线时间少的保存更久远的文件" = 冷文件下沉到冷层。

### 5.2 空闲水位目标

| 层 | 目标空闲率 | 说明 |
|----|-----------|------|
| Hot | ≥ 40% | 预留给新上传和热点回迁 |
| Warm | ≥ 20% | 常规缓冲 |
| Cold | ≥ 5% | 尽量填满，最大化归档容量 |

当某磁盘空闲率低于目标时，触发**下沉迁移**（把该磁盘上最冷的文件迁到更冷的层）。

### 5.3 文件冷热与"久远"定义

用 `assetTemperature` 综合评估，越低越冷、越该下沉：

```
assetTemperature =
      w1 × recencyScore     // 文件时间：file_created_at 越新越热
    + w2 × accessScore      // 访问热度：见 §6.1
    + w3 × favoriteBoost    // is_favorite / 相册命中 加权
```

- "久远的文件" ≈ `file_created_at` 很旧且 `accessScore` 低 → 低温 → 下沉冷层。
- 默认权重：`w1=0.4, w2=0.5, w3=0.1`（可配置）。
- 该温度是目录级迁移决策的输入；实际迁移以**目录**为单元、且默认仅对**上个月**目录执行（见 §6）。

---

## 6. 文件迁移（R2）

### 6.0 迁移单元：以目录为单位

- 迁移的**最小决策与执行单元是"目录"**，而非单个文件。目录即 `storage-structure.md` 中定义的月份目录 `uploads/YYYY/MM/`。
- 决策结果二选一：**整目录迁移** 或 **整目录不迁移**；目录下所有文件随目录一起搬走，不存在"目录内部分文件迁移"。
- 理由：
  - 与现有月份目录结构天然契合，无需额外分组逻辑。
  - 整目录搬运便于校验、回滚与副本一致性（整目录副本要么都在、要么都不在）。
  - 降低决策与任务调度开销（以月为粒度，目录数量可控）。

### 6.1 访问热度采集（目录级聚合）

保留文件级访问事件采集，但**目录温度**由其包含文件的温度聚合得到：

| 指标 | 说明 |
|------|------|
| `accessCount` | 目录内各文件累计访问次数之和 |
| `lastAccessAt` | 目录内最近一次访问时间 |
| `accessScore` | 目录内各文件 `accessScore` 之和（或均值，可配置） |

> 文件级采集仍在本机随访问记录，随状态拉取/批量上报汇总，避免每次访问都写全局。

### 6.2 迁移范围：只对"上个月"目录

- 默认迁移窗口 = **上个月**的月份目录。例如当前为 2026-07，则仅评估 `2026/06/`。
- 当月目录（`2026/07/`）视为"活跃数据"，**不迁移**，留在热/温层保证访问速度。
- 更早的目录（早于上个月）通常已处于最终归档位置（冷层），不再频繁重评估；仅在层级错配或空间告警时按需纳入。
- 每月初触发一次"上个月目录"的均衡评估（周期可配置）。
- 评估时直接查 `directory` 表（§8.5）取该目录的 `tier` / `temperature` / `total_bytes`，**不遍历目录内文件**。

### 6.3 迁移前空间预检（硬约束）

迁移决策**生成前**必须先校验目标磁盘容量，确保整目录放得下：

```
对每个候选目标磁盘 D：
    required = 源目录总字节数 × (1 + SAFETY_MARGIN)   // 含碎片/增长余量
    if D.freeBytes >= required:
        D 进入可用目标列表
    else:
        跳过 D（空间不足）
if 无可用目标: 放弃本次迁移，下周期重试 / 触发空间告警
```

| 参数 | 默认 | 说明 |
|------|------|------|
| `SAFETY_MARGIN` | 10% | 预留碎片/增长余量 |
| 目录总大小 | 实时统计 | 目录内**待迁出那份**副本的字节之和（避免重复计入多副本） |

> 空间预检是**硬约束**：宁可少迁、不迁，也不让目标磁盘写满。

### 6.4 迁移决策（整目录上迁 / 下沉）

Coordinator 在每月初（或配置周期）对"上个月"目录执行：

```
1. 从 `directory` 表（§8.5）取出候选目录的 `tier` / `temperature` / `total_bytes`（O(目录数)，不扫描文件）
2. 下沉：目录温度低（久远+少访问）且位于热/温层 → 计划下沉到更冷的层
3. 上迁：目录温度高（被大量回看）且主副本在冷层 → 计划上迁到热层
4. 对每条计划先做 §6.3 空间预检，筛掉空间不足的目标
5. 生成 MigrationPlan[]（整目录粒度），按收益排序，受配额限制（见 §6.6）
```

### 6.5 迁移执行（先整目录复制后删除）

```
Coordinator                 SourceNode              TargetNode
   │  下发 MigrationTask(目录)  │                       │
   │──────────────────────────>│                       │
   │                            │  TargetNode 批量拉取   │
   │                            │<──────────────────────│  GET /cluster/blob/:assetId (逐文件)
   │                            │  流式 + 逐文件 checksum │
   │                            │──────────────────────>│
   │                            │                       │ 逐文件校验一致后写盘
   │                            │      目录整体写盘 + 注册副本 │
   │                            │<──────────────────────│  确认
   │  更新该目录所有文件副本元数据│                       │
   │<───────────────────────────────────────────────────│
   │  确认目录级有效副本数≥2 后  │                       │
   │───────────────────────────>│  删除源目录旧副本      │
```

**关键安全约束**：
- 整目录"复制 + 逐文件校验 + 更新元数据 + 删除源"，任一步失败则**整目录回滚**，源副本保留。
- 删除源副本前，必须确认该目录内每个 asset 的**有效副本数 ≥ 2**（见 R3）。
- 传输带校验和（复用 `asset.checksum`，为空则迁移时计算并回填）。
- 目录级原子性：以目录为回滚单位，避免"迁了一半"。

### 6.6 迁移节流

| 限制 | 目的 |
|------|------|
| 单节点并发迁移数上限 | 避免占满带宽/IO |
| 迁移仅在源和目标都在线时进行 | 保证可完成 |
| 低电量/移动网络时暂停（复用 `BatteryMonitor`） | 保护移动设备 |
| 每周期迁移字节配额 | 平滑负载 |
| 单目录大小上限（可选） | 超大目录拆批或跳过 |

---

## 7. 副本保证（R3）

### 7.1 副本策略

| 规则 | 说明 |
|------|------|
| **最小副本数** `MIN_REPLICAS = 2` | 每个未删除 asset 至少 2 份有效副本 |
| **反亲和** | 2 个副本不得在同一 `diskSerial` 上；尽量不在同一节点 |
| **层分布** | 理想：1 份在热/温层（快速访问）+ 1 份在冷层（归档兜底） |

### 7.2 副本健康检查与修复

Coordinator 周期扫描 `replica` 元数据：

```
for each asset (未删除):
    有效副本 = 副本所在磁盘当前可达/或最近在线
    if 有效副本数 < MIN_REPLICAS:
        标记 asset 为 UNDER_REPLICATED
        选择新目标磁盘（满足反亲和 + 有空间 + 在线）
        生成补副本任务（优先级高于均衡迁移）
```

- **副本不足优先级最高**：先补副本，再谈均衡（R1/R2）。
- 某磁盘长期离线（超过阈值，如 30 天）→ 其上副本视为"可疑"，为相关 asset 预防性补副本。
- 删除文件：仅当用户删除 asset 时，才删除其**所有**副本（软删除 `is_trashed`，回收站过期后物理删除）。

### 7.3 副本状态机

```
        创建/上传
           │
           ▼
      ┌─────────┐  副本数达标   ┌──────────┐
      │ PENDING │─────────────>│  HEALTHY │
      └─────────┘              └────┬─────┘
                                    │ 磁盘离线/丢失
                                    ▼
                            ┌──────────────────┐  补副本完成
                            │ UNDER_REPLICATED │───────────┐
                            └────────┬─────────┘           │
                                     │                     ▼
                                     │ 无法补足        ┌──────────┐
                                     └───────────────>│ AT_RISK  │
                                                      └──────────┘
```

---

## 8. 数据模型（拟新增，SQLDelight）

> 与现有 `Asset.sq`、`ServerConfig.sq` 并列，新增以下表。**仅为设计示意，暂不建表。**

### 8.1 `disk`（磁盘登记与在线统计，R4）

```sql
CREATE TABLE disk (
    disk_serial TEXT PRIMARY KEY,       -- 磁盘序列号（跨节点唯一）
    label TEXT NOT NULL DEFAULT '',
    capacity_bytes INTEGER NOT NULL DEFAULT 0,
    free_bytes INTEGER NOT NULL DEFAULT 0,
    mounted_node_id TEXT,               -- 当前挂载节点 serverId
    online_seconds INTEGER NOT NULL DEFAULT 0,  -- 累计在线秒数
    first_seen_at INTEGER NOT NULL DEFAULT 0,
    last_seen_at INTEGER NOT NULL DEFAULT 0,         -- 最近被拉取/上报时间
    recent_uptime REAL NOT NULL DEFAULT 0,      -- 近 7 天在线率
    online_score REAL NOT NULL DEFAULT 0,       -- 综合在线评分
    tier TEXT NOT NULL DEFAULT 'WARM'           -- HOT / WARM / COLD
);
```

### 8.2 `node`（集群成员）

```sql
CREATE TABLE node (
    node_id TEXT PRIMARY KEY,           -- serverId
    node_name TEXT NOT NULL DEFAULT '',
    last_url TEXT,                      -- 最近已知 serverUrl
    last_seen_at INTEGER NOT NULL DEFAULT 0,
    is_coordinator INTEGER NOT NULL DEFAULT 0,
    battery_level INTEGER,             -- 供节流决策
    is_online INTEGER NOT NULL DEFAULT 0
);
```

### 8.3 `replica`（副本分布，R3 核心）

```sql
CREATE TABLE replica (
    id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL,             -- 关联 asset.id
    disk_serial TEXT NOT NULL,          -- 副本所在磁盘
    relative_path TEXT NOT NULL,        -- 磁盘内相对路径
    checksum TEXT,                      -- 副本校验和
    status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING/HEALTHY/UNDER_REPLICATED/AT_RISK
    created_at INTEGER NOT NULL DEFAULT 0,
    verified_at INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX idx_replica_asset_disk ON replica(asset_id, disk_serial); -- 反亲和：同盘不重复
CREATE INDEX idx_replica_asset ON replica(asset_id);
CREATE INDEX idx_replica_disk ON replica(disk_serial);
```

### 8.4 `asset_access`（访问热度，R2）

```sql
CREATE TABLE asset_access (
    asset_id TEXT PRIMARY KEY,
    access_count INTEGER NOT NULL DEFAULT 0,
    last_access_at INTEGER NOT NULL DEFAULT 0,
    access_score REAL NOT NULL DEFAULT 0,   -- 时间衰减热度
    temperature REAL NOT NULL DEFAULT 0     -- 综合冷热
);
```

> **对现有 `asset` 表的影响**：
> - `original_path`（单机绝对路径）语义弱化，改由 `replica` 表描述"文件在集群哪些磁盘上"。迁移期可保留 `original_path` 作为本机主副本的兼容字段。
> - 需**新增一列 `dir_key TEXT`**（形如 `"2026/06"`），标记该文件所属月份目录。作用：① 库内"某目录全部文件元信息" = `SELECT * FROM asset WHERE dir_key = ?`，无需为目录另行维护明细表；② 与磁盘上的 `.immich-dir.json`（见 §8.5.1）、`directory` 缓存表（§8.5）的 `dir_key` 三处对齐，互为校验与重建依据。
> - 现状提醒：`checksum` 当前上传后为 `null`（见前序分析），需在上载时补算 SHA-256，否则 `replica.checksum`、`.immich-dir.json` 的校验值与 §6.5 迁移校验均无据可依。

### 8.5 `directory`（目录缓存，R2 加速）

为让每月均衡评估从"遍历文件"降为"查目录"，缓存每个月份目录的聚合视图。键为 `YYYY/MM` 形式的相对目录路径。

```sql
CREATE TABLE directory (
    dir_key TEXT PRIMARY KEY,            -- 形如 "2026/06"（相对 uploads 的路径）
    node_id TEXT NOT NULL,               -- 该目录当前主副本所在节点
    disk_serial TEXT NOT NULL,           -- 该目录当前所在磁盘
    tier TEXT NOT NULL DEFAULT 'WARM',  -- 当前所在层 HOT/WARM/COLD
    asset_count INTEGER NOT NULL DEFAULT 0,   -- 目录内文件数
    total_bytes INTEGER NOT NULL DEFAULT 0,   -- 目录内"待迁出那份"副本字节和
    access_score REAL NOT NULL DEFAULT 0,     -- 目录级访问热度（§6.1）
    temperature REAL NOT NULL DEFAULT 0,      -- 目录温度（§6.1）
    last_eval_at INTEGER NOT NULL DEFAULT 0,  -- 最近一次评估时间
    updated_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_directory_tier ON directory(tier);
CREATE INDEX idx_directory_disk ON directory(disk_serial);
```

| 字段 | 评估用途 |
|------|----------|
| `tier` | 直接判断是否需要上迁/下沉，无需先看文件 |
| `temperature` | 排序/分层决策的输入（§6.4） |
| `total_bytes` | 迁移前空间预检（§6.3）直接取值，不必实时遍历 |
| `asset_count` / `access_score` | 监控与阈值判断 |

**维护时机**：
- 新文件写入 / 删除时，增量更新对应目录行（`asset_count`、`total_bytes`）。
- 访问热度汇总（§6.1）落地时刷新 `access_score` / `temperature`。
- Coordinator 每月初评估前，可先 `SELECT * FROM directory WHERE dir_key = 'YYYY/MM'`，O(目录数) 完成决策，不再扫描文件。

> 该表是**派生缓存**，可从 `asset` / `replica` / `asset_access` 重建，丢失不致命。

### 8.5.1 磁盘级目录清单 `.immich-dir.json`（随盘自描述，R3 辅助）

`directory` 表是**集群级聚合缓存**（存在 Coordinator/节点 DB，给均衡决策用）。与之互补，应在**每个磁盘的月份目录**中放一份自描述清单，记录本目录**所有文件**的元信息，随磁盘走。

**位置**：`uploads/YYYY/MM/.immich-dir.json`（与 §11.2 `.immich-disk-id` 同思路，是"disk-id 认领"在目录级的延伸）。

**字段（每个文件一条）**：

| 字段 | 说明 |
|------|------|
| `assetId` | 对应 `asset.id` |
| `dirKey` | `"YYYY/MM"`，与 `asset.dir_key` 一致 |
| `checksum` | SHA-256（与 `replica.checksum` 同源，迁移校验用） |
| `size` | 文件字节数 |
| `type` | IMAGE / VIDEO |
| `mimeType` | MIME |
| `fileCreatedAt` | 文件创建时间 |
| `originalFileName` | 原始文件名 |
| `replicaOn` | 该文件在本磁盘之外的其它副本所在 `disk_serial` 列表（便于跨盘认领时核对副本分布） |

**价值**：
- **换节点即认领**：磁盘插到新节点，新节点扫 `.immich-dir.json` 即可完整认领本目录全部文件与副本，**不依赖全局 `asset` 表**（契合你之前定的"磁盘靠序列号/disk-id 认领、可拔插跨节点"）。
- **离线校验 / 迁移免扫盘**：迁移前空间预检（§6.3）、逐文件校验（§6.5）直接读清单，不必逐个读库或遍历目录物理扫描。
- **三处互校**：`asset.dir_key`（库内明细）、`directory`（聚合缓存）、`.immich-dir.json`（磁盘自描述）三者 `dir_key` 对齐，任一处丢失可从另两处重建。

**维护时机**：
- 文件写入 / 删除 / 迁移复制成功时，增量更新对应目录的 `.immich-dir.json`（写前取锁，避免并发损坏）。
- 与 `directory` 表区分：`.immich-dir.json` 是**明细**，随磁盘读写；`directory` 表是**聚合**，供 Coordinator 决策。两者通过 `dir_key` 关联，方向一致。

---

## 9. 集群协议与 API

> 复用现有 `serverToken`/HMAC 做节点间鉴权；下列为集群内 HTTP 接口（拟新增 `/api/cluster/*`）。

### 9.1 成员与状态拉取（按需，去心跳）

| 接口 | 说明 |
|------|------|
| `GET /api/cluster/state` | **主要机制**：Coordinator 按需拉取各节点状态（节点信息 + 本机磁盘统计含 `onlineSeconds` 当前累计值 + 访问热度）；拉取不可达即判定该节点离线 |
| `POST /api/cluster/heartbeat`（可选） | 节点主动上报状态的备选机制（非必需），内容与上同 |

节点状态 payload 示意（状态拉取返回 / 主动上报内容）：

```json
{
  "nodeId": "550e8400-...",
  "nodeUrl": "http://192.168.1.21:2283/api",
  "batteryLevel": 78,
  "disks": [
    {"diskSerial": "WD-1234", "capacityBytes": 512000000000,
     "freeBytes": 300000000000, "onlineSeconds": 8640000, "firstSeenAt": 1770000000}
  ],
  "accessDelta": [
    {"assetId": "a1", "accessCount": 3, "lastAccessAt": 1781050000}
  ],
  "signature": "hmac-sha256..."
}
```

### 9.2 副本与迁移

| 接口 | 说明 |
|------|------|
| `GET /api/cluster/blob/:assetId` | 目标节点从源节点拉取文件字节（带 checksum 头） |
| `POST /api/cluster/replica/register` | 复制完成后登记新副本 |
| `POST /api/cluster/replica/verify` | 请求节点校验某副本 checksum |
| `DELETE /api/cluster/replica/:id` | 删除某副本（仅 Coordinator 在副本数达标后下发） |
| `POST /api/cluster/task` | Coordinator 下发迁移/补副本任务 |

### 9.3 发现层扩展

在现有 UDP 发现响应中增加集群字段（向后兼容，旧客户端忽略未知键）：

```json
{ "...现有字段...": "...", "clusterId": "home-cluster", "clusterRole": "coordinator" }
```

---

## 10. Coordinator 选举

- 采用**最高在线评分优先**的简单选举：`onlineScore` 最高且当前在线的节点成为 Coordinator（平票用 `serverId` 字典序）。
- 每个节点通过按需拉取感知谁是 Coordinator；Coordinator 连续 N 次（`COORD_TIMEOUT`）拉取不可达即触发重新选举。
- Coordinator 无状态可重建：决策所需的全局视图由各节点状态按需拉取聚合，故障漂移不丢数据。
- 也支持"无 Coordinator"降级：每个节点仅保证本机相关 asset 的副本数（弱化的 R3），暂停全局均衡。

---

## 11. 磁盘序列号获取（各平台）

### 11.1 平台方案

| 平台 | 方案 |
|------|------|
| JVM/Desktop | `lsblk -o SERIAL`（Linux）、`diskutil info`（macOS）、`wmic diskdrive get serialnumber`（Windows） |
| Android | 受限；可尝试读取 `/sys/block/*/device/serial`，SD 卡用 `cid`；多数场景需回退 |
| iOS | 沙盒无法获取硬件序列号，使用回退方案 |

### 11.2 回退策略（获取不到真实序列号时）

按优先级：
1. 文件系统 UUID（`blkid` / mount 信息）。
2. 在该磁盘存储根写入 `.immich-disk-id` 文件，内含随机生成的稳定 UUID，作为逻辑"磁盘序列号"。
3. 该 disk-id 随磁盘物理迁移而延续（因为文件在磁盘上），满足 R4 的"跟随磁盘"语义。

> 逻辑 disk-id 的缺点：磁盘格式化会丢失。可在文档标注为已知限制。

---

## 12. 端到端流程示例

### 12.1 新文件上传

```
1. 客户端上传到就近在线节点 N（复用现有 POST /api/assets）
2. N 保存主副本到本机热/温层磁盘，写 replica(status=PENDING)
3. Coordinator 在下次状态拉取中发现该 asset 副本数=1 < 2
4. Coordinator 下发补副本任务 → 选一块反亲和磁盘（优先冷层做归档副本）
5. 复制 + 校验 + register，副本数=2 → status=HEALTHY
```

### 12.2 上个月目录下沉（R1）

```
1. 每月初，Coordinator 圈定"上个月"目录（如 2026-07 月初评估 2026/06/）
2. 该目录温度低（久远且少访问）→ 判定为冷，应下沉
3. 先做空间预检：选一块冷层磁盘，确认其剩余空间 ≥ 整目录大小×(1+余量)
4. 整目录下沉到冷层（先复制后删源，保证过程中副本数≥2）
5. 热/温层腾出空间，冷层承载久远文件
```

### 12.3 热文件上迁（R2）

```
1. 某老照片被相册翻看，accessScore 上升 → 温度升高
2. 其主副本在冷层（离线概率高，访问慢）
3. Coordinator 计划上迁：在热层节点新增一份副本
4. 若总副本数超限，可回收冷层多余副本（但仍保持 ≥2）
```

---

## 13. 边界情况与冲突处理

| 场景 | 处理 |
|------|------|
| 唯一持有副本的磁盘离线，且无法补副本 | 标记 `AT_RISK`，告警用户；恢复在线后优先补副本 |
| 迁移中途源或目标掉线 | 任务超时回滚，源副本保留，下周期重试 |
| 两节点同时认为自己是 Coordinator（脑裂） | 以 `onlineScore` 高者为准，低者退让；操作幂等避免重复迁移 |
| 磁盘拔到新节点 | 新节点扫描 `.immich-disk-id` / 序列号，认领已有副本，更新 `mounted_node_id` |
| 同一 asset 元数据冲突 | 以 `updated_at` 较新者为准（LWW），副本集合取并集后按健康检查收敛 |
| 用户删除 asset | 软删除→回收站→过期后删除所有副本 |
| 全集群仅 1 个节点/1 块磁盘 | 无法满足 R3，降级为单副本并告警；接入第二块磁盘后自动补副本 |

---

## 14. 配置项（拟）

| 配置 | 默认 | 说明 |
|------|------|------|
| `MIN_REPLICAS` | 2 | 最小副本数 |
| `COORD_TIMEOUT` | 3×拉取周期 | Coordinator 连续不可达判定离线并重选 |
| `HOT_SCORE_THRESHOLD` | 0.8 | 热层阈值 |
| `COLD_SCORE_THRESHOLD` | 0.4 | 冷层阈值 |
| `HOT_FREE_TARGET` | 40% | 热层目标空闲率 |
| `MIGRATION_BYTES_PER_CYCLE` | 可配 | 每周期迁移配额 |
| `TEMP_WEIGHTS` | 0.4/0.5/0.1 | 温度权重 w1/w2/w3 |
| `OFFLINE_SUSPECT_DAYS` | 30 | 磁盘离线多久视为可疑 |

---

## 15. 实现路线图（分阶段）

### 阶段一：磁盘身份与在线统计（R4 基础）
- [ ] 磁盘序列号获取（各平台 + 回退 disk-id）
- [ ] `disk` 表 + 本机磁盘登记与在线时长累加
- [ ] 磁盘统计本机自治维护（无需上报通道；决策时按需拉取）

### 阶段二：集群成员与元数据
- [ ] `node` / `replica` / `asset_access` 表
- [ ] `GET /api/cluster/state`（按需拉取）；可选轻量存活上报
- [ ] Coordinator 选举

### 阶段三：副本保证（R3，最高优先业务价值）
- [ ] 副本健康检查
- [ ] 补副本任务（复制 + 校验 + register）
- [ ] 反亲和约束

### 阶段四：均衡与迁移（R1 + R2）
- [ ] 分层与空闲水位监控
- [ ] 访问热度采集与目录温度计算（落 `directory` 表，§8.5）
- [ ] 目录级迁移单元 + 仅"上个月"窗口
- [ ] 迁移前目标磁盘空间预检
- [ ] 下沉/上迁决策引擎 + 节流（整目录粒度）

### 阶段五：健壮性
- [ ] 脑裂/掉线/回滚处理
- [ ] 磁盘物理迁移认领
- [ ] 监控面板（各磁盘层级/副本健康/迁移进度）

---

## 16. 与现有代码的衔接点（供后续实现参考）

| 现有组件 | 衔接方式 |
|----------|----------|
| `DiscoveryProtocol` / `DiscoveryServer` | 扩展响应字段（`clusterId`/`clusterRole`），发现集群成员 |
| `ServerConfig.sq` / `ServerConfigService` | 存 `clusterId` / Coordinator 状态 |
| `HmacUtils` / `serverToken` | 复用做集群 API 鉴权（按需拉取 / 可选上报签名） |
| `PlatformFileStorage` | 增加"按磁盘/相对路径"读写、拉取 blob 能力 |
| `AssetRoutes` | 记录访问事件；上传后触发补副本 |
| `SyncRoutes`（当前 stub） | 可演进为集群元数据同步通道 |
| `BatteryMonitor` / `StorageMonitor` | 为迁移节流与空闲水位提供输入 |

---

## 17. 已知限制

- 逻辑 disk-id 在磁盘格式化后丢失。
- Android/iOS 获取真实硬件序列号受限，多依赖回退方案。
- 最终一致性下，副本数短时间可能不足（补副本有延迟）。
- 局域网带宽有限，大规模迁移需较长时间。
- 本设计聚焦局域网；跨公网 P2P 不在此范围。
