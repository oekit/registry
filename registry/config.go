package registry

import "context"

// ConfigStore 提供服务配置的拉取和动态监听能力。
//
// 这与服务发现（Discoverer/Subscriber）是正交关注点：
// 服务发现返回实例列表（IP/Port），而 ConfigStore 返回服务的运行时配置
// （如超时、重试、负载均衡策略等，通常以 JSON 等格式存储）。
//
// nns 包的 ServiceClient 同时使用 ConfigStore 和服务发现接口，
// 将配置和实例信息组合为完整的 ServiceBasic。
type ConfigStore interface {
	// GetConfig 拉取指定 dataID 的配置内容。
	//
	// 返回空字符串表示配置不存在（非错误）。
	GetConfig(ctx context.Context, dataID, group string) (string, error)

	// ListenConfig 监听指定 dataID 的配置变更，内容变化时回调 onChange。
	//
	// onChange 接收最新的配置内容字符串。实现应确保回调在独立 goroutine
	// 中执行或不会阻塞监听循环。
	ListenConfig(ctx context.Context, dataID, group string, onChange func(data string)) error

	// StopListenConfig 停止监听指定 dataID 的配置变更。
	StopListenConfig(ctx context.Context, dataID, group string) error
}
