# Changelog — oekit/registry/nacos

由 oekit/registry v0.5.0 拆分而来（Nacos 客户端门面独立成模块——nacos-sdk 重依赖
隔离在此，registry 根仓回归零第三方依赖）。

## [v0.2.0] — 2026-11-13

- 跟随 registry v0.6.0 发版（release_train 嵌套 train）（registry#177）

关联问题单：registry#177。

## [v0.1.14] — 2026-11-12

- 跟随 registry v0.5.15 发版（release_train 嵌套 train）（registry#139、registry#140、registry#141、registry#142、registry#143、registry#144）

关联问题单：registry#139、registry#140、registry#141、registry#142、registry#143、registry#144。

## [v0.1.13] — 2026-11-11

- fix(nacos): ConfigSource.Watch 的 ctx 取消后自动反注册 SDK 监听（#126）——此前 Watch 注册后无任何停止出口，注释谎称「SDK 无监听反注册 API」（与同矩阵 nacosimpl.StopListenConfig 直接矛盾，SDK 的 IConfigClient.CancelListenConfig 一直存在）：被取消的 watcher 留下永不反注册的 SDK 监听（cacheMap 条目、服务端数据监听、推送 goroutine 只增不减），排障时按注释以为「已无监听」被误导。修法：Watch 注册成功后挂 ctx 取消钩子，取消即调 CancelListenConfig 反注册（与 nacosimpl.StopListenConfig 同一 API），删除虚假注释

关联问题单：oekit/registry#126。

## [v0.1.12] — 2026-11-10

- 跟随 registry v0.5.12 发版（release_train 嵌套 train）（registry#104、registry#105）

关联问题单：registry#104、registry#105。

## [v0.1.11] — 2026-11-09

- 跟随 registry v0.5.11 发版（release_train 嵌套 train）（registry#95、registry#96）

关联问题单：registry#95、registry#96。

## [v0.1.10] — 2026-11-08

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.1.9] — 2026-11-07

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.1.8] — 2026-11-07

- 跟随 registry v0.5.5 发版（release_train 嵌套 train）（registry#63、registry#64、registry#65）

关联问题单：registry#63、registry#64、registry#65。

## [v0.1.7] — 2026-11-06

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）（registry#51、registry#53、registry#54）

关联问题单：registry#51、registry#53、registry#54。

## [v0.1.5] — 2026-11-06

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.1.4] — 2026-11-06

- 跟随 registry v0.5.4 发版（release_train 嵌套 train）

## [v0.1.3] — 2026-11-03

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）（registry#49）

关联问题单：registry#49。

## [v0.1.2] — 2026-11-03

- 跟随 registry v0.5.3 发版（release_train 嵌套 train）

## [v0.1.1] — 2026-11-03

- 跟随 registry v0.5.2 发版（release_train 嵌套 train）

## [v0.1.0] — 2026-10-21

- refactor: 自 registry v0.5.0 拆出为独立模块——**import 路径不变**
  （`github.com/oekit/registry/nacos`），变的只是「由哪个 module 提供」：
  消费方需在自己的 go.mod 里补 `github.com/oekit/registry/nacos`
- build: 依赖 nacos-sdk-go v2 + core/config + logx；父仓 registry 保持零第三方依赖
  （实测：nacos-sdk-go/v2 单独引入即拖 63 个第三方模块，不隔离时只要 SPI 接口的
  消费方也会被连带拖入）
