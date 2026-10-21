// Package registry 提供与服务注册中心无关的服务注册与发现抽象接口。
//
// 本包定义核心接口（Registrar、Discoverer、Subscriber、ConfigStore），
// 不依赖任何具体的注册中心 SDK。各注册中心（如 Nacos）的实现放在**嵌套模块**里
// （github.com/oekit/registry/nacos、github.com/oekit/registry/nacosimpl）——
// 它们各自把 Nacos SDK 的重依赖隔离在模块边界之外，父仓保持零第三方依赖。
//
// 注意：registry 是底层 SPI 和框架薄适配，只供 NNS、测试、极简接入使用。
// 不包含缓存、降级、singleflight 等治理能力。新服务推荐通过 nns 包接入。
//
// # 设计目标
//
//   - 解耦：上层（如 nns 包）依赖接口而非具体 SDK，便于测试和替换实现。
//   - 可测试：通过接口可以轻松注入 mock，解决上层核心逻辑难以测试的问题。
//   - 可扩展：未来新增 etcd/consul/k8s 等实现时，上层无需改动。
//
// # 三层架构
//
//	registry/nacos        Nacos 客户端封装（嵌套模块：Bootstrap 配置、client 工厂、config 加载）
//	registry              provider 无关的抽象接口（本包，根模块）
//	registry/nacosimpl    Nacos 实现（嵌套模块：封装 naming_client/config_client）
//
// 上层门面 nns 依赖 registry 接口，默认使用 registry/nacosimpl 实现，
// 也可通过依赖注入替换为 mock 或其他 provider。
//
// # 快速接入
//
// 使用 Nacos 实现：
//
//	reg, err := nacosimpl.NewRegistrar(namingClient, nacosimpl.WithGroup("DEFAULT_GROUP"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	err = reg.Register(ctx, registry.Instance{
//	    Service:  "my-service",
//	    IP:       "10.0.0.1",
//	    Port:     8080,
//	    Metadata: version.Metadata(),
//	})
//
// 发现服务实例：
//
//	disc, err := nacosimpl.NewDiscoverer(namingClient)
//	instances, err := disc.Discover(ctx, "user-service")
package registry
