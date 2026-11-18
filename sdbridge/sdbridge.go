package sdbridge

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/oekit/registry"
)

// FetchFunc 拉取指定服务的实例列表。
//
// 各框架适配层将自身的数据源（Discoverer.Discover、nns ServiceClient.GetInstances）
// 包装为本类型，交给 [ResolverCore] 统一调度。
type FetchFunc func(ctx context.Context, service string) ([]registry.Instance, error)

// ServiceNameFromDesc 去掉 Hertz 服务描述中的 "?tags" 后缀，返回纯服务名。
//
// Hertz 的缓存 key（由 registry.EncodeTargetTags 编码）形如 "host?k=v"，
// 而服务发现只认服务名，解析逻辑原先在 hertz/registry 与 hertz/nns 各拷贝一份。
func ServiceNameFromDesc(desc string) string {
	if idx := strings.Index(desc, "?"); idx >= 0 {
		return desc[:idx]
	}
	return desc
}

// Validate 校验实例列表：空列表返回包装了 [registry.ErrNoInstances] 的错误
// （可通过 errors.Is 判定），非空列表原样返回。
//
// 错误消息为 "no instances for %q"，不含前缀——由调用方（如 [ResolverCore.Fetch]）
// 按需附加各自的 resolver 前缀。
func Validate(instances []registry.Instance, service string) ([]registry.Instance, error) {
	if len(instances) == 0 {
		return nil, &noInstancesError{service: service}
	}
	return instances, nil
}

// noInstancesError 空实例列表错误。
//
// 消息保持 "no instances for %q"（与各适配器历史文案一致，前缀由调用方添加），
// 通过 Unwrap 关联 [registry.ErrNoInstances] 供 errors.Is 判定。
type noInstancesError struct {
	service string
}

func (e *noInstancesError) Error() string { return fmt.Sprintf("no instances for %q", e.service) }

func (e *noInstancesError) Unwrap() error { return registry.ErrNoInstances }

// ToInstance 从框架注册信息的通用字段构造 [registry.Instance]，
// weight <= 0 时使用 defaultWeight 兜底。内部委托 [registry.InfoFrom]，
// 作为 4 个 registry.go 适配文件权重兜底逻辑的唯一入口。
func ToInstance(name string, addr net.Addr, weight, defaultWeight int, tags map[string]string) (registry.Instance, error) {
	return registry.InfoFrom(name, addr, weight, defaultWeight, tags)
}

// NetworkTCP 是 [InstanceEndpoint] 返回的 network 值：四个 adapter 的实例
// 转换（InstancesToKitex / InstancesToHertz）均以 "tcp" 为 network，单源收口。
const NetworkTCP = "tcp"

// InstanceEndpoint 返回实例的框架无关网络端点：network 恒为 "tcp"
// （[NetworkTCP]），addr 为 IP:Port（net.JoinHostPort，IPv6 自动加方括号）。
//
// 消费方：kitex/registry 与 hertz/registry 的实例转换
// （InstancesToKitex / InstancesToHertz 此前各自 net.JoinHostPort 拷贝一份）
// 及 nns 系同类转换（后续接入）。
func InstanceEndpoint(inst registry.Instance) (network, addr string) {
	return NetworkTCP, inst.Addr()
}

// WeightOrDefault 回填非法权重：weight <= 0 时使用 defaultWeight 兜底；
// defaultWeight 也非法（<=0）时兜底到根包 [registry.DefaultInstanceWeight]
// （#214 常量单源）——保证返回值恒为正，转换产物不再把零/负权重透传给
// 框架负载均衡器（kitex weighted_balancer 判 "invalid weight" 视作无可用
// 实例 fail-closed 503，#92 教训）。
//
// 消费方：kitex/registry、hertz/registry 的 convert.go（后续接入，替换
// 各自内联的 weight <= 0 分支）。
func WeightOrDefault(weight, defaultWeight float64) float64 {
	if weight > 0 {
		return weight
	}
	if defaultWeight > 0 {
		return defaultWeight
	}
	return registry.DefaultInstanceWeight
}

// EnsureNotNil 对必填依赖做构造期 nil fail-fast：dep 为 nil（接口持 nil）
// 时 panic，消息形如 "<caller> requires non-nil <what>"。
//
// 口径与 hertz#219 一致：构造期配置错误启动即暴露，避免运行期空指针拖到
// 框架内部 goroutine 才炸出「未知来源 panic」。此前只有 hertz 侧两个
// Registry adapter 有该检查（且 panic 文案各写一份），kitex 侧与 watcher
// 构造无统一入口——本函数是唯一的口径单源。
//
// 用法（adapter 构造器内）：
//
//	sdbridge.EnsureNotNil(reg, "hertz: NewRegistry", "registrar")
//
// 注意：仅检接口层面的 nil；持有类型化 nil 指针（(*T)(nil) 装箱进接口）
// 不在本检查范围——与 hertz#219 既有口径一致，不引入反射超集。
//
// 消费方：hertz/registry、hertz/nns、kitex/registry、kitex/nns 四个
// Registry adapter 构造器与 instancewatcher.NewInstanceWatcher（后续接入）。
func EnsureNotNil(dep any, caller, what string) {
	if dep == nil {
		panic(fmt.Sprintf("%s requires non-nil %s", caller, what))
	}
}
