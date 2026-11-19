module github.com/oekit/registry

go 1.26.0

// 根模块 = 注册发现**接口** + core（仅标准库 + core/network）。
//
// Nacos 实现已拆成嵌套模块（依赖隔离，职责同矩阵既有先例）：
//   github.com/oekit/registry/nacos       —— Nacos 配置源 / 客户端封装
//   github.com/oekit/registry/nacosimpl   —— 基于 nacos-sdk 的 Discoverer/ConfigStore 实现
//
// 拆分的理由（实测）：nacos-sdk-go/v2 单独引入即拖进 63 个第三方模块；
// 不隔离时，**只想要注册发现接口**的消费方也会被连带拖进去。
require github.com/oekit/core v0.17.1

// registrymocks 直接消费 pkg/errors.Recovered（#158 panic 收编单源）⇒ pkg 转直接依赖。
require github.com/oekit/pkg v0.55.1

// instancewatcher（#319 自 hertz/nns/health.go 迁入的实例轮询 watcher）依赖
// logx（日志门面，自身零 require）与 concurrency/async（GoSafe panic 收编）。
// 两者均为矩阵内零重依赖包，根仓「零第三方依赖」承诺不破——go.sum 仅新增
// 这两个模块自身，无任何 golang.org/... 等第三方代码进入根模块构建图。
require (
	github.com/oekit/concurrency v0.39.2
	github.com/oekit/logx v0.12.0
)
