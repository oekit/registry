// Package registrymocks 提供 registry 包 4 个接口（Registrar/Discoverer/Subscriber/ConfigStore）
// 的统一 mock 实现，供单元测试使用（原为 ekit internal 包，拆分后跨模块复用故公开）。
//
// 消除历史散落在 nns/client、kitex/nns、kitex/registry、hertz/registry、hertz/nns
// 等多个 _test.go 文件中的重复 mock 定义。
//
// 设计原则：
//   - 字段可配置：每个 mock 暴露必要的字段（错误、返回值）用于测试用例
//   - 线程安全：所有方法都加锁，支持并发测试场景
//   - 完整实现：覆盖接口的所有方法（不留 panic 默认）
package registrymocks
