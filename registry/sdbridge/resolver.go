package sdbridge

import (
	"context"
	"fmt"

	"github.com/oekit/registry/registry"
)

// FetchErrFunc 包装实例拉取阶段的错误。
//
// errPrefix 是错误消息前缀，service 是（已剥离 "?tags" 的）服务名，
// err 是数据源返回的底层错误。
type FetchErrFunc func(errPrefix, service string, err error) error

// PlainFetchErr 返回 "<errPrefix>: <err>" 格式的错误（nns 系适配器风格，
// 数据源错误已自带服务上下文，不再重复服务名）。
func PlainFetchErr(errPrefix, _ string, err error) error {
	return fmt.Errorf("%s: %w", errPrefix, err)
}

// DiscoverFetchErr 返回 "<errPrefix>: discover <service>: <err>" 格式的错误
// （registry 系适配器风格，附加服务名与动词）。
func DiscoverFetchErr(errPrefix, service string, err error) error {
	return fmt.Errorf("%s: discover %q: %w", errPrefix, service, err)
}

// ResolverCoreOption 是 [ResolverCore] 的配置选项函数类型。
type ResolverCoreOption func(*ResolverCore)

// WithFetchErr 自定义实例拉取阶段的错误包装格式。
// fn 为 nil 时忽略（保持默认 [PlainFetchErr]）。
func WithFetchErr(fn FetchErrFunc) ResolverCoreOption {
	return func(c *ResolverCore) {
		if fn != nil {
			c.fetchErr = fn
		}
	}
}

// ResolverCore 服务发现解析器的框架无关核心骨架。
//
// 封装 registry/{kitex,hertz} 与 nns/{kitex,hertz} 四个 Resolver 适配器共用的
// "拉取 + 校验 + 错误包装" 逻辑；框架适配层只负责：
//  1. Target 的框架类型适配；
//  2. 实例列表 → 框架实例类型的转换（InstancesToKitex / InstancesToHertz）；
//  3. 框架 Result 的组装（Cacheable/CacheKey 等框架特有字段）。
type ResolverCore struct {
	fetch     FetchFunc // 实例拉取函数
	name      string    // resolver 名称（Name() 返回值）
	errPrefix string    // 错误消息前缀（如 "kitex resolver"）
	fetchErr  FetchErrFunc
}

// NewResolverCore 创建解析器核心。
//
// fetch 是实例拉取函数；name 是 resolver 名称（Name() 返回值）；
// errPrefix 是错误消息前缀（如 "kitex resolver"），错误格式为
// "<errPrefix> ..."（不含尾冒号，冒号由错误格式函数添加）。
func NewResolverCore(fetch FetchFunc, name, errPrefix string, opts ...ResolverCoreOption) *ResolverCore {
	c := &ResolverCore{
		fetch:     fetch,
		name:      name,
		errPrefix: errPrefix,
		fetchErr:  PlainFetchErr,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// Name 返回 resolver 名称。
func (c *ResolverCore) Name() string { return c.name }

// Cacheable 报告解析结果是否可被框架缓存。
//
// Kitex Result.Cacheable 恒为 true（lbcache 按 CacheKey 缓存增量 Diff）；
// Hertz 无此字段，忽略。
func (c *ResolverCore) Cacheable() bool { return true }

// Fetch 拉取并校验指定服务的实例列表（"拉取 + 校验" 公共骨架）。
//
// service 必须是纯服务名（Hertz 场景先用 [ServiceNameFromDesc] 剥离 "?tags"）。
// 拉取失败返回 fetchErr 定义的格式；实例列表为空返回
// "<errPrefix>: no instances for <service>"（errors.Is 可匹配
// [registry.ErrNoInstances]）。
func (c *ResolverCore) Fetch(ctx context.Context, service string) ([]registry.Instance, error) {
	insts, err := c.fetch(ctx, service)
	if err != nil {
		return nil, c.fetchErr(c.errPrefix, service, err)
	}
	insts, err = Validate(insts, service)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", c.errPrefix, err)
	}
	return insts, nil
}
