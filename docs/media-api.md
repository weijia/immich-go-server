# 客户端媒体 API 设计（资产上传 / 列表 / 下载 / 删除）

> 配套实现：阶段零（FR1–FR11）中"资产接口"部分（见 `requirements.md` §FR7）。
> 本文档在已有发现 / 认证引导（`/api/server-info/ping`、`/api/server-info`、`/api/auth/login`、`/api/auth/token-exchange`）基础上，补齐可被官方 Immich 客户端真正"传图、看图、删图"的媒体接口，并与"**仓库即真相**"物理模型（`blob_root/<dir_key>/<assetID>` + `.meta.json` sidecar）保持一致。

## 1. 目标与非目标

**目标**
- 让官方 Immich 客户端完成一次完整备份闭环：发现 → 登录 → `bulk-upload-check` → 上传 → 时间线可见 → 下载原图。
- 上传资产按"仓库即真相"规则落盘（内容寻址 + 月份目录 + sidecar），与 `cmd/ingest` 摄入的产物**完全一致**，可被 `scanner` 重建、被均衡器迁移。
- 暴露标准 REST 的资产元信息 / 原图 / 缩略图 / 删除接口。

**非目标（本期不做，留痕）**
- 真实缩略图生成（本期 `/thumbnail` 返回原图字节，沿用 android 版 stub；后续接 `image/draw` 缩放）。
- 多用户 / 权限（ownerId 取服务器身份 `ServerID`；不做 token 强制校验）。
- 相册（albums）、搜索、人物、归档/回收站语义（仅保留 `isArchived/isTrashed` 字段默认 false）。
- `Range` 续传、EXIF 解析（上传时间取 `fileCreatedAt`/`fileModifiedAt`，解析失败回退当前时间）。

## 2. 端点总览

| 方法 | 路径 | 说明 | 鉴权 |
|---|---|---|---|
| POST | `/api/assets/bulk-upload-check` | 客户端上传前批量查重 | 无（MVP） |
| POST | `/api/assets` | multipart 上传单个资产 | 无（MVP） |
| GET  | `/api/assets` | 列出全部资产（时间线基础） | 无（MVP） |
| GET  | `/api/assets/{id}` | 单资产元信息 | 无（MVP） |
| GET  | `/api/assets/{id}/original` | 下载原文件（正确 Content-Type） | 无（MVP） |
| GET  | `/api/assets/{id}/thumbnail` | 缩略图（MVP：返回原图字节） | 无（MVP） |
| DELETE | `/api/assets/{id}` | 标准 REST 删除（字节 + 元数据） | 无（MVP） |

> 注：`requirements.md` FR3 曾列 `/api/server/ping`、`/api/server/version`，与 android 实现（`/api/server-info/ping`、`/api/server-info`）命名不一致；以 android 实现为准，避免客户端二次适配。

## 3. 数据模型

复用既有 `asset` 表（`asset_id, size_bytes, checksum, dir_key, original_path`），**新增两张表**承载 API 侧元信息，与仓库物理真相解耦：

```sql
-- 资产 API 侧元信息（仓库 .meta.json 只存 checksum/size/kind，这里存展示字段）
CREATE TABLE asset_meta (
  asset_id          TEXT PRIMARY KEY,
  device_asset_id   TEXT,
  device_id         TEXT,
  file_created_at   TEXT,
  file_modified_at  TEXT,
  is_favorite       INTEGER DEFAULT 0,
  duration          TEXT,
  type              TEXT,   -- IMAGE | VIDEO
  mime_type         TEXT,
  original_file_name TEXT,
  width             INTEGER,
  height            INTEGER
);

-- (device_id, device_asset_id) -> asset_id，供 bulk-upload-check 去重
CREATE TABLE device_asset (
  device_id       TEXT NOT NULL,
  device_asset_id TEXT NOT NULL,
  asset_id        TEXT NOT NULL,
  PRIMARY KEY (device_id, device_asset_id)
);
```

落盘与 `asset_id` 的内部生成规则（内容寻址、目录布局、文件名扩展名等存储实现细节）由存储设计文档规定，**不在本接口契约范围内**。上传成功返回 `AssetResponse`（含 `id`、`type`、`mimeType`、`fileSize`、`originalFileName` 等），客户端以 `id` 引用资产；内部 `asset_id` 与物理文件名对客户端不透明。

## 4. 关键流程

### 4.1 上传 `POST /api/assets`
1. 解析 multipart：`deviceAssetId`、`deviceId`、`fileCreatedAt`、`fileModifiedAt`、`isFavorite`、`duration`、`assetData`(文件)。必填缺失 → 400。
2. 计算 `sha256(bytes)` → `assetID`，`size = len(bytes)`。
3. **去重**：若 `(deviceId, deviceAssetId)` 已存在 → 返回已有 `id` + `duplicate:true`（200）。
4. 选盘：本节点已认领磁盘中 `free_bytes` 最大者；无则回退 `Handler.BlobRoot`（单根）。得到 `blobRoot` 与 `disk_serial`（无盘时为空）。
5. `dir_key` 由 `fileCreatedAt`（失败 `fileModifiedAt`，再失败 `now`）推导。
6. 写字节 `blobRoot/<dir_key>/<asset_id>`；写 `asset_meta`；合并写 `.meta.json`；写 `asset` + `replica(asset@disk, HEALTHY)` + `directory`（dir_key→disk，累加 total_bytes）；写 `device_asset` 映射。
7. 返回 `AssetResponse`（含 `id`、`type`、`mimeType`、`fileSize`、`originalFileName`、`fileCreatedAt` 等），201。

### 4.2 查重 `POST /api/assets/bulk-upload-check`
- 入参 `{deviceAssetIds:[...], deviceId}` → 对每项查 `device_asset`，返回 `{results:[{id, deviceAssetId, exists:bool}]}`。`exists` 为真时附带 `id`（已存在资产）。客户端据此跳过上传。

### 4.3 下载 / 缩略图
- `/original`：按 `asset.dir_key` + 副本定位 `blobRoot/<dir_key>/<asset_id>`，以扩展名推导 Content-Type 流式返回；缺失 → 404。
- `/thumbnail`：MVP 直接返回原图字节（Content-Type 取 `image/*` 或原文件类型），保证客户端时间线能渲染；真实缩略图为后续项。

### 4.4 删除 `DELETE /api/assets/{id}`
1. `asset` 不存在 → 404。
2. 由副本定位 `blobRoot`（无盘用 `Handler.BlobRoot`）；删除物理字节 `blobRoot/<dir_key>/<asset_id>`；从 `.meta.json` 移除该 `asset_id`。
3. 删 `asset` / `replica` 全部副本 / `asset_meta` / `device_asset`；若该 `dir_key` 下已无资产则删 `directory` 记录。
4. 返回 204。

## 5. 代码落点

- `internal/store/store.go`：新增 `asset_meta`/`device_asset` 表与读写方法（`SaveUploadedAsset`、`GetAssetMeta`、`DeleteAsset`、`SaveDeviceAsset`、`LookupDeviceAssets`、`ListMountedDisks`）+ 启动时补表（幂等 `CREATE TABLE IF NOT EXISTS`）。
- `internal/ingest/ingest.go`：导出 `FlushMeta`；新增 `RemoveAssetFromMeta(blobRoot, dirKey, assetID)`。
- `internal/clusterapi/clusterapi.go`：`Handler` 增加 `AssetStore AssetBackend` 接口字段与资产处理函数；`Mux()` 挂载路由（不经集群 HMAC）。
- `internal/server/server.go`：构造 `Handler` 后设 `h.AssetStore = st`。
- `cmd/server/main.go`：无需新增环境变量（复用 `DISK_DIRS`/`BLOB_ROOT`）。

## 6. 验证

- 单测：去重、选盘、落盘路径、sidecar 合并/移除、删除后元数据一致。
- 冒烟（PowerShell）：
  ```powershell
  curl.exe -s -X POST http://127.0.0.1:8081/api/auth/login -d '{}'
  curl.exe -s -F deviceAssetId=da1 -F deviceId=dev1 -F fileCreatedAt=2026-06-01T10:00:00Z `
    -F assetData=@photo.jpg http://127.0.0.1:8081/api/assets
  curl.exe -s http://127.0.0.1:8081/api/assets
  curl.exe -s http://127.0.0.1:8081/api/assets/<id>/original -o out.jpg
  curl.exe -s -X DELETE http://127.0.0.1:8081/api/assets/<id>
  ```
