// Package nacos 提供 Nacos 配置中心和服务注册发现客户端。
//
// 所有 enjoydream 后端服务共享此包，统一封装 Nacos SDK 操作。
//
// # 核心特性
//
//   - Bootstrap 配置结构体：嵌入服务配置即可连接 Nacos
//   - ConfigClient：远程配置加载和动态监听
//   - NamingClient：服务注册、发现、订阅
//   - Register/Deregister：服务实例注册和注销
//
// # 快速接入
//
// 在配置结构体中嵌入 Bootstrap：
//
//	type AppConfig struct {
//	    Port  int             `yaml:"port"`
//	    Nacos nacos.Bootstrap `yaml:"nacos"`
//	}
//
// 创建 ConfigClient：
//
//	client, err := nacos.NewConfigClient(cfg.Nacos)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// 加载远程配置并合并：
//
//	err = nacos.LoadAndMerge(client, cfg.Nacos, &cfg)
//
// 监听配置变更：
//
//	nacos.Listen(client, cfg.Nacos, &cfg, func(newCfg *AppConfig) {
//	    // 配置变更回调
//	})
//
// # 服务注册
//
// 使用 registry/nacos 包的 Registrar（推荐）或 nns 包的 Bootstrap：
//
//	import nacosimpl "github.com/oekit/registry/registry/nacos"
//
//	reg := nacosimpl.NewRegistrar(namingClient, nacosimpl.WithRegistrarGroup(cfg.Group))
//	err := reg.Register(ctx, registry.Instance{
//	    Service:  cfg.ServiceName,
//	    IP:       nacosimpl.GetPodIP(),
//	    Port:     port,
//	    Metadata: version.Metadata(),
//	})
//
// # Bootstrap 配置字段
//
//   - Addr: Nacos 地址，如 "nacos.enjoydream.svc.cluster.local:8848"
//   - NamespaceId: 命名空间，如 "dev"、"prod"
//   - Group: 配置分组，默认 "DEFAULT_GROUP"
//   - DataId: 配置 DataId
//   - Username/Password: 认证信息
//   - ServiceName: 服务注册名称
package nacos
