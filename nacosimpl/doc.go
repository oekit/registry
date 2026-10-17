// Package nacos 提供 [registry] 接口的 Nacos 实现。
//
// 本包封装 nacos-sdk-go 的 naming_client 和 config_client，
// 实现 registry 包定义的 Registrar、Discoverer、Subscriber 和 ConfigStore 接口。
//
// # 与 ekit/nacos 包的关系
//
//	ekit/nacos         SDK 薄封装：Bootstrap 配置、NewNamingClient/NewConfigClient 工厂
//	ekit/registry      provider 无关的抽象接口
//	ekit/registry/nacos（本包）  将 SDK client 适配为 registry 接口
//
// 典型用法：
//
//	namingClient, _ := nacos.NewNamingClient(cfg)
//	configClient, _ := nacos.NewConfigClient(cfg)
//
//	reg, _ := nacosimpl.NewRegistrar(namingClient, nacosimpl.WithGroup(cfg.Group))
//	disc, _ := nacosimpl.NewDiscoverer(namingClient, nacosimpl.WithGroup(cfg.Group))
//	sub, _ := nacosimpl.NewSubscriber(namingClient, nacosimpl.WithGroup(cfg.Group))
//	store := nacosimpl.NewConfigStore(configClient)
//
// 所有实现均满足并发安全要求（底层 SDK client 自身并发安全）。
package nacos
