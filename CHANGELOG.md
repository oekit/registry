## [nacosimpl/v0.1.6] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [nacos/v0.1.5] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [nacosimpl/v0.1.5] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [nacos/v0.1.4] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.5.4] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [nacosimpl/v0.1.4] — 2026-11-03

- build: 把父仓 registry v0.5.3 与 logx v0.8.2 的地板带进已发布产物（nacos/nacosimpl 原快照嵌的是更旧的父仓地板，消费方拿不到最新内容）（registry#49）

关联问题单：registry#49。

## [nacos/v0.1.3] — 2026-11-03

- build: 把父仓 registry v0.5.3 与 logx v0.8.2 的地板带进已发布产物（nacos/nacosimpl 原快照嵌的是更旧的父仓地板，消费方拿不到最新内容）（registry#49）

关联问题单：registry#49。

## [nacosimpl/v0.1.3] — 2026-11-03

- build: 矩阵内依赖地板对齐——把父仓 registry v0.5.2 与 logx v0.8.1 的地板带进已发布产物（nacosimpl 原快照嵌的是 registry v0.5.1 + logx v0.8.0，消费方拿不到新地板内容）

## [nacos/v0.1.2] — 2026-11-03

- build: 矩阵内依赖地板对齐——把父仓 registry v0.5.2 与 logx v0.8.1 的地板带进已发布产物（nacosimpl 原快照嵌的是 registry v0.5.1 + logx v0.8.0，消费方拿不到新地板内容）

## [v0.5.3] — 2026-11-03

- build: 矩阵内依赖地板对齐——把父仓 registry v0.5.2 与 logx v0.8.1 的地板带进已发布产物（nacosimpl 原快照嵌的是 registry v0.5.1 + logx v0.8.0，消费方拿不到新地板内容）

## [nacosimpl/v0.1.2] — 2026-11-03

- ci(dependabot): 补登记嵌套模块目录（/nacos、/nacosimpl）——未登记的嵌套模块依赖永远不会被提议升级且零报错（PR #36）：本仓曾漏登记两个嵌套模块 ⇒ 共 130 项间接依赖被静默冻结（形态是「直接依赖都是最新、落后全在间接依赖」⇒ 冻结落在下游的构建产物上）
- build: 矩阵内依赖地板对齐（align_floors 不动点）：core v0.9.2 → v0.12.1——registry 根模块的 go.mod **仍只 require core**（不直接引入第三方；消费方模块图里的 hertz / yaml.v3 来自 **core 的传递依赖**，本仓不承诺「模块图零第三方」），拆嵌套模块的隔离结论不变（消费方模块图不含 nacos-sdk）

## [nacos/v0.1.1] — 2026-11-03

- ci(dependabot): 补登记嵌套模块目录（/nacos、/nacosimpl）——未登记的嵌套模块依赖永远不会被提议升级且零报错（PR #36）：本仓曾漏登记两个嵌套模块 ⇒ 共 130 项间接依赖被静默冻结（形态是「直接依赖都是最新、落后全在间接依赖」⇒ 冻结落在下游的构建产物上）
- build: 矩阵内依赖地板对齐（align_floors 不动点）：core v0.9.2 → v0.12.1——registry 根模块的 go.mod **仍只 require core**（不直接引入第三方；消费方模块图里的 hertz / yaml.v3 来自 **core 的传递依赖**，本仓不承诺「模块图零第三方」），拆嵌套模块的隔离结论不变（消费方模块图不含 nacos-sdk）

## [v0.5.2] — 2026-11-03

- ci(dependabot): 补登记嵌套模块目录（/nacos、/nacosimpl）——未登记的嵌套模块依赖永远不会被提议升级且零报错（PR #36）：本仓曾漏登记两个嵌套模块 ⇒ 共 130 项间接依赖被静默冻结（形态是「直接依赖都是最新、落后全在间接依赖」⇒ 冻结落在下游的构建产物上）
- build: 矩阵内依赖地板对齐（align_floors 不动点）（#43）：core v0.9.2 → v0.12.1——registry 根模块的 go.mod **仍只 require core**（不直接引入第三方；消费方模块图里的 hertz / yaml.v3 来自 **core 的传递依赖**，本仓不承诺「模块图零第三方」），拆嵌套模块的隔离结论不变（消费方模块图不含 nacos-sdk）

## [nacosimpl/v0.1.1] — 2026-10-28

- build: 父模块依赖地板 v0.5.0 → v0.5.1（跟随 registry v0.5.1 发版）

## [v0.5.1] — 2026-10-28

- fix(registry): 包文档的示例**编译不过**——① `nacosimpl.WithGroup(...)` 该符号不存在（真实的是 `WithRegistrarGroup`/`WithDiscovererGroup`/`WithSubscriberGroup` 三个各自独立的选项）；② `NewConfigStore`/`NewRegistrar` 等返回 `(*T, error)`，示例漏了 error；③ 本包**目录名是 `nacosimpl`、包名却是 `nacos`**（与 `registry/nacos` 同名 ⇒ 同时 import 会 `nacos redeclared`），示例既没展示别名 import 又直接用 `nacosimpl.` 前缀。示例按真实签名改正，并补一条用例把文档承诺的调用形态逐一编译一遍 ⇒ 文档再漂移时当场红

## [nacosimpl/v0.1.0] — 2026-10-21

- Nacos 实现拆成嵌套模块 registry/nacosimpl 与 registry/nacos——根仓回归零第三方依赖（实测 nacos-sdk-go/v2 单独引入即拖 63 个第三方模块，不隔离时只要 SPI 接口的消费方也被连带拖入）；import 路径不变，消费方需在 go.mod 里补 go get github.com/oekit/registry/nacosimpl

## [nacos/v0.1.0] — 2026-10-21

- Nacos 实现拆成嵌套模块 registry/nacosimpl 与 registry/nacos——根仓回归零第三方依赖（实测 nacos-sdk-go/v2 单独引入即拖 63 个第三方模块，不隔离时只要 SPI 接口的消费方也被连带拖入）；import 路径不变，消费方需在 go.mod 里补 go get github.com/oekit/registry/nacosimpl

## [v0.5.0] — 2026-10-21

- Nacos 实现拆成嵌套模块 registry/nacosimpl 与 registry/nacos——根仓回归零第三方依赖（实测 nacos-sdk-go/v2 单独引入即拖 63 个第三方模块，不隔离时只要 SPI 接口的消费方也被连带拖入）；import 路径不变，消费方需在 go.mod 里补 go get github.com/oekit/registry/nacosimpl

## [v0.4.2] — 2026-10-19

- build: 矩阵内依赖地板对齐（align_floors 不动点）：core v0.7.0 → v0.9.2、logx v0.6.0 → v0.7.0

## [v0.4.1] — 2026-10-17

- fix(deps): core 地板 v0.6.0 → v0.7.0、logx 地板 v0.5.0 → v0.6.0（零代码改动）

# Changelog — oekit/registry

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

## [v0.4.0] — 2026-10-17

### Breaking
- **SPI 包提升至模块根**（与 bootstrap/gormx/hertz/httpx/middleware/network 收平先例一致）：
  - `github.com/oekit/registry/registry` → `github.com/oekit/registry`
  - `github.com/oekit/registry/registry/nacos` → `github.com/oekit/registry/nacosimpl`
  - `github.com/oekit/registry/registry/sdbridge` → `github.com/oekit/registry/sdbridge`
  仅移动目录与导入路径，代码零逻辑变化
- 删除 `Instance.GetWeight()`——负载均衡已迁至 httpx/loadbalance Selector，经 `Node.Weight()` 读取

## [v0.3.1] — 2026-10-17

- fix(deps): 内部依赖地板 core v0.4.0 → v0.6.0、logx v0.1.9 → v0.5.0；go 最低版本降至 1.24.0——x/net v0.50.0、grpc v1.80.0、genproto/rpc 20260120 锁 go1.24 兼容版

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