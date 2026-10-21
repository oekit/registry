module github.com/oekit/registry

go 1.24.0

// 根模块 = 注册发现**接口** + core（仅标准库 + core/network）。
//
// Nacos 实现已拆成嵌套模块（依赖隔离，职责同矩阵既有先例）：
//   github.com/oekit/registry/nacos       —— Nacos 配置源 / 客户端封装
//   github.com/oekit/registry/nacosimpl   —— 基于 nacos-sdk 的 Discoverer/ConfigStore 实现
//
// 拆分的理由（实测）：nacos-sdk-go/v2 单独引入即拖进 63 个第三方模块；
// 不隔离时，**只想要注册发现接口**的消费方也会被连带拖进去。
require github.com/oekit/core v0.9.2
