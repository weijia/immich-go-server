# immich-go-server

Go (Golang) 实现的 [Immich](https://immich.app) 兼容自托管照片 / 视频备份服务端，并内置**分布式多服务器存储均衡**能力。

## 目标

- 兼容 Immich REST API，可被官方 Immich 客户端（移动 / Web）识别与连接。
- 跨平台：Linux / macOS / Windows，编译为单二进制，无原生依赖。
- 移植 `immich-android-server` 的**全部**核心功能（见 `docs/requirements.md`）。
- 新增**分布式存储均衡**：分层、副本保证、目录级迁移、本地自治在线时长、按需拉取（去心跳）。

## 与 immich-android-server 的关系

本项目从 Kotlin Multiplatform 的 `immich-android-server` 迁移而来，用 Go 重写其服务端逻辑，并落地已设计的分布式存储均衡方案。设计蓝本来自 `immich-android-server/docs/distributed-storage-balancing.md`，已并入本仓库 `docs/distributed-storage-balancing.md`。

## 技术栈（拟）

- HTTP：`net/http`（或 Gin / Echo）
- 数据库：纯 Go SQLite（`modernc.org/sqlite`，无 CGO）或 `mattn/go-sqlite3`
- 发现：UDP 广播 + HMAC-SHA256 签名（复用 `serverId` / `serverToken` 模型）
- 序列化：`encoding/json`
- 文件存储：抽象 `Storage` 接口（磁盘 / 相对路径）

## 文档

- `docs/requirements.md` —— 需求与设计总纲（功能需求 + 分布式均衡需求 + 数据模型 + API + 路线图）
- `docs/distributed-storage-balancing.md` —— 分布式存储均衡完整设计

## 许可证

AGPL-3.0
