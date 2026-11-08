## [v0.5.8] — 2026-11-08

### fix(netutil): POD_IP 回环在回退网卡探测前显式拒绝——不被 GetPodIP 的非法值兜底静默掩盖（#77 纵深补丁）

POD_IP=::1 时 core#87 口径的 GetPodIP 会当非法值回退网卡探测，注册面拿到看似可拨通的地址、ErrLoopbackAddr 失效（CI 实测抓到）。

## [v0.5.7] — 2026-11-08

- fix(netutil): ParseAddr 判据收紧——「不是 IP 字面量」的主机一律拒绝（#77，ErrInvalidHostAddr 新错误，与 ErrLoopbackAddr 同族）——修复前判据是「解析成功且是回环 ⇒ 拒」（#64），解析失败分支未被覆盖：域名/垃圾串 host 原样放行且 err==nil，POD_IP 配错时实例以不可拨通地址注册、全程无告警（core#87 已在上游拦非法 POD_IP，本判据是注册面的纵深）；另明确「带 zone 的 IPv6（fe80::1%eth0）」语义并写进文档：地址合法但 zone 是本机接口名，消费方永远拨不通 ⇒ 同按 ErrInvalidHostAddr 拒绝

关联问题单：registry#77。

## [nacosimpl/v0.1.10] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [nacos/v0.1.10] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [v0.5.6] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [nacosimpl/v0.1.9] — 2026-11-07

- build: 父模块地板对齐（嵌套 go.mod 的父仓 require 跟随最新，audit 门禁项）

## [nacos/v0.1.9] — 2026-11-07

- build: 父模块地板对齐（嵌套 go.mod 的父仓 require 跟随最新，audit 门禁项）

## [nacosimpl/v0.1.8] — 2026-11-07

- fix(registry): EncodeTargetTags 的 tag key/value 经 QueryEscape——分隔符只由编码器产生，缓存 key 碰撞面关闭（#63）
- fix(registry): ParseAddr 对回环结果 fail-closed 拒绝注册（ErrLoopbackAddr 哨兵）——GetPodIP 静默回退 127.0.0.1 不再进入注册面（core 侧 GetPodIP 回退 ERROR 见 oekit#141/core v0.13.1）（#64）
- fix(nacosimpl): Registrar/Discoverer 的 ctx 真正下探（invokeWithCtx 竞速）——关停期反注册超时预算生效，SDK panic 转 error（#65）

关联问题单：registry#63、registry#64、registry#65。

## [nacos/v0.1.8] — 2026-11-07

- fix(registry): EncodeTargetTags 的 tag key/value 经 QueryEscape——分隔符只由编码器产生，缓存 key 碰撞面关闭（#63）
- fix(registry): ParseAddr 对回环结果 fail-closed 拒绝注册（ErrLoopbackAddr 哨兵）——GetPodIP 静默回退 127.0.0.1 不再进入注册面（core 侧 GetPodIP 回退 ERROR 见 oekit#141/core v0.13.1）（#64）
- fix(nacosimpl): Registrar/Discoverer 的 ctx 真正下探（invokeWithCtx 竞速）——关停期反注册超时预算生效，SDK panic 转 error（#65）

关联问题单：registry#63、registry#64、registry#65。

## [v0.5.5] — 2026-11-07

- fix(registry): EncodeTargetTags 的 tag key/value 经 QueryEscape——分隔符只由编码器产生，缓存 key 碰撞面关闭（#63）
- fix(registry): ParseAddr 对回环结果 fail-closed 拒绝注册（ErrLoopbackAddr 哨兵）——GetPodIP 静默回退 127.0.0.1 不再进入注册面（core 侧 GetPodIP 回退 ERROR 见 oekit#141/core v0.13.1）（#64）
- fix(nacosimpl): Registrar/Discoverer 的 ctx 真正下探（invokeWithCtx 竞速）——关停期反注册超时预算生效，SDK panic 转 error（#65）

关联问题单：registry#63、registry#64、registry#65。

## [nacos/v0.1.7] — 2026-11-06

- fix(nacos): Group 空值统一回填 `DEFAULT_GROUP` + Listen/Watch 失败可感知（[#51](https://github.com/oekit/registry/issues/51)、[#53](https://github.com/oekit/registry/issues/53)）——文档与 yaml 注释宣称的 Group 默认值此前从未被应用：GetConfig 对空 Group SDK 侧兜底（读路径侥幸工作），`ListenConfig` 对空 Group 直接报错 "Group can not be empty" 且被门面吞掉——YAML 省略 group 时热更从未注册，改配置永远不触发 onChange。修法：`Bootstrap.GroupOrDefault()` 统一回填（与 nacosimpl 同口径，全部 SDK 调用点接线）；**BREAKING** `Listen[T]` 改为返回 error（0.x 允许 breaking）——监听未挂上时调用方必须能感知，失败即 fail-fast；`ConfigSource.Watch` 因实现 core/config 接口（void 签名）不改签名，失败经新增的 **`ConfigSource.Err()`** 可查询（nil=监听已挂上），需 fail-fast 的调用方 Watch 后检查

关联问题单：registry#51、registry#53。

## [nacosimpl/v0.1.7] — 2026-11-06

- fix(nacosimpl): Subscribe 推送错误可感知面（[#54](https://github.com/oekit/registry/issues/54)）——SDK 推送失败此前只 log.Error 后 return：不更新状态、无重试，本地缓存（discover 侧视图）滞留旧版本直至下次推送，无任何可见信号。修法：新增 `Subscriber.LastErr()` 可查询（推送失败非 nil、成功推送清零），供健康检查/巡检暴露「视图可能过期」；日志点名明缓存可能滞后

关联问题单：registry#54。

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