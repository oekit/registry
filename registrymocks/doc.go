// Package registrymocks 提供 registry 包 4 个接口（Registrar/Discoverer/Subscriber/ConfigStore）
// 的统一 mock 实现，供单元测试使用（原为 ekit internal 包，拆分后跨模块复用故公开）。
//
// 消除历史散落在 nns/client、kitex/nns、kitex/registry、hertz/registry、hertz/nns
// 等多个 _test.go 文件中的重复 mock 定义。
//
// 设计原则：
//   - 字段可配置：每个 mock 暴露必要的字段（错误、返回值）用于测试用例——
//     构造期（未开始并发使用前）的字面量赋值不受限；并发场景下的变更
//     请走 SetXxx 系列锁内注入口与 SPI 方法，直接写导出字段会绕过 mu 锁
//     （map 是进程级 fatal error，标量是 DATA RACE，#160）
//   - 线程安全：所有方法都加锁，支持并发测试场景；SPI 派发链同契约落地
//     （#158）：回调串行投递、panic 就地隔离——与真实现（nacosimpl/etcdx）
//     同口径，mock 下测通的并发代码生产语义一致
//   - 完整实现：覆盖接口的所有方法（不留 panic 默认）
//   - 零值可用（#150）：结构体字面量构造（&Xxx{Err: ...}）后各方法可直接
//     调用——内部 map 字段在首次写入时惰性初始化，不会出现
//     「assignment to entry in nil map」这种不指向本包的 runtime panic
package registrymocks
