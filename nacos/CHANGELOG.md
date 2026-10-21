# Changelog — oekit/registry/nacos

由 oekit/registry v0.5.0 拆分而来（Nacos 客户端门面独立成模块——nacos-sdk 重依赖
隔离在此，registry 根仓回归零第三方依赖）。

## [v0.1.0] — 2026-10-21

- refactor: 自 registry v0.5.0 拆出为独立模块——**import 路径不变**
  （`github.com/oekit/registry/nacos`），变的只是「由哪个 module 提供」：
  消费方需在自己的 go.mod 里补 `github.com/oekit/registry/nacos`
- build: 依赖 nacos-sdk-go v2 + core/config + logx；父仓 registry 保持零第三方依赖
  （实测：nacos-sdk-go/v2 单独引入即拖 63 个第三方模块，不隔离时只要 SPI 接口的
  消费方也会被连带拖入）
