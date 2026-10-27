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
// 本包**的目录名是 nacosimpl，但包名是 nacos**（与 registry/nacos 同名）⇒
// import 时必须起别名，否则与 registry/nacos 同时 import 时会「nacos redeclared」。
// org 内既有消费方一律按 `nacosimpl "github.com/oekit/registry/nacosimpl"` 引入。
//
//	import nacosimpl "github.com/oekit/registry/nacosimpl"
//
//	namingClient, _ := nacos.NewNamingClient(cfg)
//	configClient, _ := nacos.NewConfigClient(cfg)
//
//	reg, err := nacosimpl.NewRegistrar(namingClient, nacosimpl.WithRegistrarGroup(cfg.Group))
//	if err != nil {
//	    return err
//	}
//	disc, err := nacosimpl.NewDiscoverer(namingClient, nacosimpl.WithDiscovererGroup(cfg.Group))
//	if err != nil {
//	    return err
//	}
//	sub, err := nacosimpl.NewSubscriber(namingClient, nacosimpl.WithSubscriberGroup(cfg.Group))
//	if err != nil {
//	    return err
//	}
//	store, err := nacosimpl.NewConfigStore(configClient)
//	if err != nil {
//	    return err
//	}
//
// 所有实现均满足并发安全要求（底层 SDK client 自身并发安全）。
package nacos
