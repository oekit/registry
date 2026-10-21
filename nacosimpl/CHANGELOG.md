# Changelog — oekit/registry/nacosimpl

由 oekit/registry v0.5.0 拆分而来（Nacos SPI 实现独立成模块——nacos-sdk 重依赖
隔离在此，registry 根仓回归零第三方依赖）。

## [v0.1.0] — 2026-10-21

- refactor: 自 registry v0.5.0 拆出为独立模块——**import 路径不变**
  （`github.com/oekit/registry/nacosimpl`），变的只是「由哪个 module 提供」：
  消费方需在自己的 go.mod 里补 `github.com/oekit/registry/nacosimpl`
- build: 依赖 registry（父仓接口，v0.5.0+）+ nacos-sdk-go v2 + core/network + logx；
  require 的 registry 版本必须 ≥ v0.5.0——v0.4.x 的 registry 仓里还带着
  `nacosimpl/` 子包目录，两个 module 同时提供同一包会让消费方撞上 ambiguous import
