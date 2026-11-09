# Changelog — oekit/registry/nacosimpl

由 oekit/registry v0.5.0 拆分而来（Nacos SPI 实现独立成模块——nacos-sdk 重依赖
隔离在此，registry 根仓回归零第三方依赖）。

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
