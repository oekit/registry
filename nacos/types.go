package nacos

// Bootstrap holds Nacos connection bootstrap configuration.
// Embed this in your service's config struct.
// 所有 yaml tag 统一使用 snake_case。
type Bootstrap struct {
	Addr        string `yaml:"addr"`         // host:port, e.g. nacos.enjoydream.svc.cluster.local:8848
	ContextPath string `yaml:"context_path"` // context path for reverse proxy, e.g. /nacos (default: /nacos)
	NamespaceId string `yaml:"namespace_id"` // dev / prod
	Group       string `yaml:"group"`        // DEFAULT_GROUP
	DataId      string `yaml:"data_id"`      // service config data id
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	ServiceName string `yaml:"service_name"` // service registration name
	GrpcPort    uint64 `yaml:"grpc_port"`    // gRPC port override (for NodePort: set to container port 9848)
}
