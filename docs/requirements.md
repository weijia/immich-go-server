# immich-go-server 需求与设计文档

> 本仓库用 Go 重写 `immich-android-server` 的服务端逻辑，并落地[分布式存储均衡设计](./distributed-storage-balancing.md)。
> 第 2 节的 FR1–FR11 为**移植需求**（来自 android 版 `需求文档.md`，标注"移植"），第 5 节为**新增分布式均衡需求**。

---

## 1. 产品概述

一个轻量、兼容 Immich 协议的照片 / 资产备份服务端，用 Go 实现，跨平台（Linux / macOS / Windows）编译为单二进制。在保留 android 版已有能力（认证、资产上传下载、UDP 局域网发现、server-info 等）的基础上，新增**多服务器存储均衡**：把局域网内多台服务器组成协作集群，实现空间分层、副本保证与目录级迁移。

设计原则（与均衡设计一致）：最终一致、去中心优先（Coordinator 可选举可漂移）、本地自治、数据安全第一（副本不足先补副本、迁移先复制后删除）、复用现有 `serverId` / `serverToken` / HMAC / UDP 发现。

---

## 2. 移植功能需求（FR，来自 immich-android-server）

### FR1 服务器启动与生命周期（移植）
- `main` 启动：初始化 SQLite → 生成/加载 `server_config` → 启动 HTTP 服务器（默认 `:2283`，`wait=false`）→ 启动 UDP 发现服务器（`:2284`）。
- 提供 `Start()` / `Stop()` / `IsRunning()` / `ServerURL()` / `ServerID()` / `ServerName()`。
- HTTP 中间件：JSON 协商（lenient、忽略未知键）、CORS（允许 `Authorization` / `Content-Type` / `x-api-key`）、异常转文本响应。

### FR2 服务配置（移植）
- 首次启动生成 `serverId`（UUID v4）与 32 字节（256-bit）`serverToken`，存 `server_config` 表。
- `serverName` 默认 `Immich Go Server`，可更新；支持 `regenerateServerToken()`。
- `serverToken` 永不在 UDP 上传输，仅经 HTTPS 的 `token-exchange` 下发。

### FR3 服务器信息接口（移植）
- `GET /api/server/ping` → `{ "res": "pong" }`
- `GET /api/server/version` → `{ "major": 3, "minor": 0, "patch": 0 }`
- `GET /api/server/features` → 特性开关
- `GET /api/server/config`
- Legacy 兼容：`GET /server-info`、`/server-info/ping`、`/server-info/version`、`/server-info/features`
- `GET /.well-known/immich`

### FR4 认证接口（移植，修正）
- `POST /api/auth/login`、`POST /api/auth/logout`
- `GET /api/auth/admin-sign-up`、`POST /api/auth/admin-sign-up`（首个用户为管理员）
- `GET /api/users/me`：**需实现为从 auth token 返回真实用户**（修正 android 版硬编码默认管理员）

### FR5 Token 交换（移植）
- `POST /api/auth/token-exchange`：校验 `Authorization: Bearer <accessToken>`，返回 `{ serverToken, serverId, expiresAt: null }`。

### FR6 同步接口（移植 stub）
- `syncRoutes()` 注册；后续演进为集群元数据同步通道。

### FR7 资产接口（移植 + 修正/新增）
- `POST /api/assets`：multipart 上传（`deviceAssetId` / `deviceId` / `fileCreatedAt` / `fileModifiedAt` / `isFavorite` / `duration` / `assetData`）。**修正**：上传时计算 SHA-256 写入 `asset.checksum`；`ownerId` 从 auth token 获取（修正硬编码）。
- `GET /api/assets/{id}`：元信息。
- `GET /api/assets/{id}/original`：下载原文件（正确 ContentType）。
- `GET /api/assets/{id}/thumbnail`：**需实现真实缩略图**（修正 android 占位返回原文件）。
- `DELETE /api/assets/{id}`：**用标准 REST DELETE**（修正 android 的 `GET /delete` 简化）。
- 新增 `asset.dir_key` 列（形如 `"2026/06"`）标记所属月份目录，供目录级迁移与清单使用。

### FR8 局域网发现协议（移植）
- 端口 `2284`，广播 `255.255.255.255`，三版本：
  - v1.0：`DISCOVER_IMMICH_SERVER` → 含 `serverUrl/version/serverName/timestamp`
  - v2.0：额外含 `serverId`
  - v3.0：额外含 `challengeNonce` + `signature = HMAC-SHA256(serverToken, serverId|serverUrl|timestamp|challengeNonce)`
- 响应 URL 自动补全 `/api` 后缀；客户端可优雅降级。

### FR9 数据存储（移植 + 扩展）
- 移植 `user` / `server_config` / `api_key` 表。
- 扩展见 §5.2（新增 `disk` / `node` / `replica` / `asset_access` / `directory` 及 `asset.dir_key` / `asset.checksum`）。

### FR10 平台抽象（Go 适配）
- `Storage` 接口：`SaveAsset` / `GetAssetPath` / `ReadFile` / `DeleteAsset` / `GetAssetSize` / `GetStoragePath`；增加"按磁盘/相对路径"读写、拉取 blob 能力。
- `Notifier` 接口（可选，桌面端系统通知）。
- SQLite 跨平台无需额外驱动抽象（纯 Go driver）。

### FR11 构建（Go）
- `go build -o immich-go-server ./cmd/server` 产出单二进制。
- 支持 `GOOS` / `GOARCH` 交叉编译。

---

## 3. 安全需求（SR，移植）

- **SR1 防服务器伪造**：v3.0 用 `serverToken` 的 HMAC 签名验证响应来源。
- **SR2 防重放**：v3.0 `challengeNonce` 必须与请求匹配。
- **SR3 防 MITM**：`serverToken` 仅经 token-exchange 下发，不经 UDP。
- **SR4 未授权访问防护**：`serverToken` 仅在用户认证后下发。
- **SR5 token 存储**：服务端存储于 SQLite；客户端应使用安全存储。

---

## 4. 非功能需求（NFR）

- **NFR1 跨平台单二进制**：核心逻辑全在 Go，无平台特定 CGO 依赖（可选 CGO SQLite）。
- **NFR2 低资源占用**：纯标准库 + 轻量 SQLite，可在树莓派 / 旧机运行。
- **NFR3 类型安全**：请求/响应用 `encoding/json` + 结构体。
- **NFR4 后台运行**：桌面端可注册 systemd / launchd / Windows Service。
- **NFR5 协议兼容**：对外声明 Immich 3.x 兼容。

---

## 5. 分布式存储均衡需求（新增）

| 编号 | 需求 | 设计目标 |
|------|------|----------|
| D-R1 | **空间分配** | 在线时长最长的磁盘尽量腾空（热层），在线时长短的承载久远/冷数据 |
| D-R2 | **文件迁移** | 高访问频率文件迁到高在线率服务器，低频下沉；**以目录为单位、仅上个月、迁移前空间预检** |
| D-R3 | **副本保证** | 任意文件在集群中至少保留 **2 个副本**，且不在同一物理磁盘 |
| D-R4 | **磁盘在线时长统计** | 由挂载磁盘的服务器**本地自治**统计，负载均衡时各节点各自上报；**去心跳**，按需拉取 |

完整设计（概念、架构、分层、复制、迁移、数据模型、协议、选举、边界、路线图、衔接点）见 [distributed-storage-balancing.md](./distributed-storage-balancing.md)。关键决策摘要：

- **R4 本地自治**：磁盘在线时长只由挂载它的服务器累加，节点间不互相更新；聚合只读、不回写。
- **去心跳**：删除持续 Gossip 心跳，改用按需 `GET /api/cluster/state` 拉取（拉不到即离线）+ 复用 UDP 发现维护成员；可选轻量心跳上报。
- **R2 目录为单元**：迁移最小单元是月份目录 `uploads/YYYY/MM/`，整目录迁或整目录不迁；默认仅评估"上个月"目录；迁移前对目标磁盘做空间预检（硬约束）。
- **R3 副本优先**：副本不足时先补副本再谈均衡；迁移"先复制后删除"，目录级原子回滚。

### 5.1 磁盘身份（D-R4 基础）
- 主键用磁盘序列号 `diskSerial`（跨节点唯一）；获取不到时用文件系统 UUID 或落盘 `.immich-disk-id` 文件兜底（§11.2）。

### 5.2 Go 数据模型（SQLite 建表）
与均衡设计 §8 一致，转换为 Go/SQLite DDL：

```sql
-- 现有 asset 表需新增：dir_key TEXT, checksum TEXT（上传时计算）
CREATE TABLE disk (
    disk_serial   TEXT PRIMARY KEY,
    label         TEXT NOT NULL DEFAULT '',
    capacity_bytes INTEGER NOT NULL DEFAULT 0,
    free_bytes    INTEGER NOT NULL DEFAULT 0,
    mounted_node_id TEXT,
    online_seconds INTEGER NOT NULL DEFAULT 0,
    first_seen_at INTEGER NOT NULL DEFAULT 0,
    last_seen_at  INTEGER NOT NULL DEFAULT 0,
    recent_uptime REAL NOT NULL DEFAULT 0,
    online_score  REAL NOT NULL DEFAULT 0,
    tier          TEXT NOT NULL DEFAULT 'WARM'
);

CREATE TABLE node (
    node_id        TEXT PRIMARY KEY,
    node_name      TEXT NOT NULL DEFAULT '',
    last_url       TEXT,
    last_seen_at   INTEGER NOT NULL DEFAULT 0,
    is_coordinator INTEGER NOT NULL DEFAULT 0,
    battery_level  INTEGER,
    is_online      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE replica (
    id           TEXT PRIMARY KEY,
    asset_id     TEXT NOT NULL,
    disk_serial  TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    checksum     TEXT,
    status       TEXT NOT NULL DEFAULT 'PENDING',
    created_at   INTEGER NOT NULL DEFAULT 0,
    verified_at  INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX idx_replica_asset_disk ON replica(asset_id, disk_serial);
CREATE INDEX idx_replica_asset ON replica(asset_id);
CREATE INDEX idx_replica_disk ON replica(disk_serial);

CREATE TABLE asset_access (
    asset_id       TEXT PRIMARY KEY,
    access_count   INTEGER NOT NULL DEFAULT 0,
    last_access_at INTEGER NOT NULL DEFAULT 0,
    access_score   REAL NOT NULL DEFAULT 0,
    temperature    REAL NOT NULL DEFAULT 0
);

CREATE TABLE directory (
    dir_key      TEXT PRIMARY KEY,
    node_id      TEXT NOT NULL,
    disk_serial  TEXT NOT NULL,
    tier         TEXT NOT NULL DEFAULT 'WARM',
    asset_count  INTEGER NOT NULL DEFAULT 0,
    total_bytes  INTEGER NOT NULL DEFAULT 0,
    access_score REAL NOT NULL DEFAULT 0,
    temperature  REAL NOT NULL DEFAULT 0,
    last_eval_at INTEGER NOT NULL DEFAULT 0,
    updated_at   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_directory_tier ON directory(tier);
CREATE INDEX idx_directory_disk ON directory(disk_serial);
```

- 磁盘级目录清单：每个 `uploads/YYYY/MM/.immich-dir.json` 记录本目录所有文件的 `assetId / dirKey / checksum / size / type / mimeType / fileCreatedAt / originalFileName / replicaOn`（§8.5.1）。

### 5.3 集群 API（新增 `/api/cluster/*`）
- `GET /api/cluster/state`：Coordinator 按需拉取节点状态（节点信息 + 本机磁盘统计 `onlineSeconds` 当前累计值 + 访问热度），拉不到即离线。
- `POST /api/cluster/heartbeat`（可选）：节点主动上报备选。
- `GET /api/cluster/blob/:assetId`：节点间拉取文件字节（带 checksum）。
- `POST /api/cluster/replica/register` / `verify`、`DELETE /api/cluster/replica/:id`、 `POST /api/cluster/task`：副本登记/校验/删除/任务下发。
- 鉴权复用 `serverToken` / HMAC。

---

## 6. 实现路线图

- **阶段零：移植单机功能**（FR1–FR11）：认证、资产上传/下载、UDP 发现、server-info；修正 `checksum` / `ownerId` / 缩略图 / 标准 DELETE。
- **阶段一：磁盘身份与在线统计（D-R4）**：序列号获取 + 回退 disk-id；`disk` 表 + 本机在线时长累加（自治）。
- **阶段二：集群成员与元数据**：`node` / `replica` / `asset_access` 表；`GET /api/cluster/state` 按需拉取；Coordinator 选举。
- **阶段三：副本保证（D-R3）**：副本健康检查、补副本任务、反亲和约束。
- **阶段四：均衡与迁移（D-R1 + D-R2）**：分层与空闲水位；目录温度；目录级迁移单元 + 仅"上个月"窗口；迁移前空间预检；上迁/下沉决策 + 节流。
- **阶段五：健壮性**：脑裂/掉线/回滚；磁盘物理迁移认领；监控面板。

---

## 7. 已知限制与待实现

| 项 | 状态 | 说明 |
|----|------|------|
| `/users/me` 真实用户 | 待实现 | 需基于 auth token 返回真实用户 |
| 缩略图生成 | 待实现 | 需生成真实缩略图（非返回原文件） |
| `checksum` | 待补 | 上传时需计算 SHA-256，否则副本/迁移校验无依据 |
| 资产 ownerId | 待修正 | 从 auth token 获取，非硬编码 |
| 发现 token 加密 | 待确认 | 服务端 SQLite 是否字段级加密 |
| 逻辑 disk-id | 已知限制 | 磁盘格式化后丢失 |
| 最终一致副本延迟 | 已知限制 | 副本数短时间可能不足 |
| 跨公网 P2P | 超出范围 | 本设计聚焦局域网 |
