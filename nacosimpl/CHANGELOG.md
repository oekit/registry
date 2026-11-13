# Changelog — oekit/registry/nacosimpl

由 oekit/registry v0.5.0 拆分而来（Nacos SPI 实现独立成模块——nacos-sdk 重依赖
隔离在此，registry 根仓回归零第三方依赖）。

## [v0.3.0] — 2026-11-13

- 跟随 registry v0.6.0 发版（release_train 嵌套 train）（registry#177）

关联问题单：registry#177。

## [v0.2.1] — 2026-11-13

- 跟随 registry v0.5.17 发版（release_train 嵌套 train）（registry#157、registry#158、registry#159、registry#160、registry#161）

关联问题单：registry#157、registry#158、registry#159、registry#160、registry#161。

## [v0.2.0] — 2026-11-12

- 跟随 registry v0.5.15 发版（release_train 嵌套 train）（registry#139、registry#140、registry#141、registry#142、registry#143、registry#144）

关联问题单：registry#139、registry#140、registry#141、registry#142、registry#143、registry#144。

## [v0.1.14] — 2026-11-11

- fix(nacosimpl): ConfigStore 回调串行投递（#127）——SDK 对同一 dataID 每次推送各起一个新 goroutine 裸调 OnChange（config_client.go:103 `go listener(...)`），两次连更并发、乱序进入消费方 onChange，与 etcdx ConfigStore「回调串行执行、不会并发调用 onChange」的承诺分裂，快速连更可能旧配置最后生效（热更最终态停在过期配置）且与读侧构成数据竞争。修法：OnChange 闭包内投递互斥锁串行化（与 etcdx 口径对齐），registry.ConfigStore.ListenConfig 的 SPI 文档补「回调串行、不并发」契约，让两种后端实现行为可互换

关联问题单：oekit/registry#127。

- fix(nacosimpl): ConfigStore.ListenConfig 消费回调 panic 就地隔离（#125）——SDK config 派发链在独立 goroutine 上裸调 OnChange（config_client.go:103 `go listener(...)` 全程无 recover），onChange 解析配置 panic 会沿派发 goroutine 上抛打死宿主进程；与 #96 同构换道复发（naming Subscribe / nacos.Listen / nacos.ConfigSource.Watch 均已收编，唯独 config store 漏网）。修法：OnChange 闭包内 defer recover 转 Error 日志 + stack（与 nacos.Listen 的 #96 收编逐字同款），并给 registry.ConfigStore.ListenConfig 的 SPI 文档补 panic 隔离契约，堵住下一个实现面

关联问题单：oekit/registry#125。

- build: 依赖地板对齐——github.com/oekit/registry v0.5.14（随本 train 发布）。

## [v0.1.13] — 2026-11-10

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.1.12] — 2026-11-09

- 跟随 registry v0.5.11 发版（release_train 嵌套 train）（registry#95、registry#96）

关联问题单：registry#95、registry#96。

## [v0.1.11] — 2026-11-09

- 跟随 registry v0.5.10 发版（release_train 嵌套 train）（registry#94）

关联问题单：registry#94。

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

## [v0.1.6] — 2026-11-06

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.1.5] — 2026-11-06

- 跟随 registry v0.5.4 发版（release_train 嵌套 train）

## [v0.1.4] — 2026-11-03

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）（registry#49）

关联问题单：registry#49。

## [v0.1.3] — 2026-11-03

- 跟随 registry v0.5.3 发版（release_train 嵌套 train）

## [v0.1.2] — 2026-11-03

- 跟随 registry v0.5.2 发版（release_train 嵌套 train）

## [v0.1.1] — 2026-10-28

- build: 父模块依赖地板 v0.5.0 → v0.5.1（跟随 registry v0.5.1 发版）

## [v0.1.0] — 2026-10-21

- refactor: 自 registry v0.5.0 拆出为独立模块——**import 路径不变**
  （`github.com/oekit/registry/nacosimpl`），变的只是「由哪个 module 提供」：
  消费方需在自己的 go.mod 里补 `github.com/oekit/registry/nacosimpl`
- build: 依赖 registry（父仓接口，v0.5.0+）+ nacos-sdk-go v2 + core/network + logx；
  require 的 registry 版本必须 ≥ v0.5.0——v0.4.x 的 registry 仓里还带着
  `nacosimpl/` 子包目录，两个 module 同时提供同一包会让消费方撞上 ambiguous import
