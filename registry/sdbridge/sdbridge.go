package sdbridge

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/oekit/registry/registry"
)

// FetchFunc 拉取指定服务的实例列表。
//
// 各框架适配层将自身的数据源（Discoverer.Discover、nns ServiceClient.GetInstances）
// 包装为本类型，交给 [ResolverCore] 统一调度。
type FetchFunc func(ctx context.Context, service string) ([]registry.Instance, error)

// ServiceNameFromDesc 去掉 Hertz 服务描述中的 "?tags" 后缀，返回纯服务名。
//
// Hertz 的缓存 key（由 registry.EncodeTargetTags 编码）形如 "host?k=v"，
// 而服务发现只认服务名，解析逻辑原先在 registry/hertz 与 nns/hertz 各拷贝一份。
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
