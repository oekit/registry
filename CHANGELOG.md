# Changelog — oekit/registry

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

## [v0.3.0] — 2026-10-16

### Breaking
- `config` 子包整体迁出至 [oekit/core](https://github.com/oekit/core) v0.4.0
  （`github.com/oekit/registry/config` → `github.com/oekit/core/config`，API 不变）。
  config 只依赖 yaml+logx，原先挂在 registry 下导致只需文件加载的服务被迫拖
  Nacos SDK 依赖树；本仓不再承担本地配置文件加载职责

### 保留
- `registry/nacos.ConfigSource`（实现 `core/config.ConfigSource`）——Nacos 配置源
  适配器，配合 `core/config.ConfigWatcher` 做远端配置热更新

### 迁移
- import 路径替换即可，无 API 变化

## [v0.2.6] — 2026-10-15

- v0.2.6

## [v0.1.0] — 2026-10-15

自 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来的初始公开版本，完成拆分稳定化：模块路径统一为 `github.com/oekit/registry`，多轮拆分/合并收敛至当前模块边界，组织基建（共享 CI workflow、golangci v2、Dependabot）就位。