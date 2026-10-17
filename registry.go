package registry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
)

// ErrNoInstances 表示服务发现未找到任何实例。
var ErrNoInstances = errors.New("registry: no instances found")

// Instance 表示注册中心中的一个服务实例，与具体 provider 无关。
type Instance struct {
	// Service 是服务名称。
	Service string
	// IP 是实例的 IP 地址。
	IP string
	// Port 是实例的端口。
	Port int
	// Weight 是实例的权重，用于加权负载均衡。
	Weight float64
	// Metadata 是实例的元数据，可携带版本、颜色、区域等信息。
	Metadata map[string]string
	// Group 是分组名称。空值时使用 Registrar 的默认 Group。
	Group string
	// Cluster 是集群名称。空值时使用 Registrar 的默认 Cluster。
	Cluster string
}

// Addr 返回实例的地址字符串，格式为 "IP:Port"。
func (i Instance) Addr() string {
	return net.JoinHostPort(i.IP, strconv.Itoa(i.Port))
}

// Registrar 提供服务实例的注册和注销能力。
type Registrar interface {
	// Register 将服务实例注册到注册中心。
	Register(ctx context.Context, inst Instance) error
	// Deregister 从注册中心注销服务实例。
	Deregister(ctx context.Context, inst Instance) error
}

// Discoverer 提供服务实例的拉取发现能力。
type Discoverer interface {
	// Discover 根据服务名称返回健康实例列表。
	Discover(ctx context.Context, serviceName string) ([]Instance, error)

	// DiscoverAll 根据服务名称返回全部实例列表（包括不健康的实例）。
	// 用于调试、监控等场景；生产调用应优先使用 Discover。
	DiscoverAll(ctx context.Context, serviceName string) ([]Instance, error)
}

// Subscriber 提供服务实例变更的订阅推送能力。
type Subscriber interface {
	// Subscribe 订阅指定服务的实例变更，当实例列表变化时回调 cb。
	Subscribe(ctx context.Context, serviceName string, cb func([]Instance)) error
	// Unsubscribe 取消订阅指定服务的实例变更。
	Unsubscribe(ctx context.Context, serviceName string) error
}

// RegisterOption 是服务注册的可选配置，与具体 provider 无关。
//
// 各 provider 实现会将这些字段映射到自身 SDK 的参数。
// 如果某个字段在目标 provider 中不存在，实现应安全地忽略它。
type RegisterOption struct {
	// Weight 是实例权重，默认 10。
	Weight float64
	// Metadata 是实例元数据。
	Metadata map[string]string
	// Cluster 是集群名称（部分 provider 支持）。
	Cluster string
	// Group 是分组名称（部分 provider 支持）。
	Group string
}

// RegisterOptionFunc 是配置 RegisterOption 的函数选项类型。
type RegisterOptionFunc func(*RegisterOption)

// Apply 将多个选项函数应用到一个新的 RegisterOption 上，返回最终配置。
//
// 用法：
//
//	opt := Apply(registry.WithWeight(20), registry.WithMetadata(meta))
func Apply(opts ...RegisterOptionFunc) *RegisterOption {
	o := &RegisterOption{
		Weight: 10,
	}
	for _, fn := range opts {
		if fn != nil {
			fn(o)
		}
	}
	return o
}

// WithWeight 设置服务实例的权重。
func WithWeight(weight float64) RegisterOptionFunc {
	return func(o *RegisterOption) { o.Weight = weight }
}

// WithMetadata 设置服务实例的元数据。
//
// 多次调用会合并而非覆盖。
func WithMetadata(metadata map[string]string) RegisterOptionFunc {
	return func(o *RegisterOption) {
		if o.Metadata == nil {
			o.Metadata = make(map[string]string)
		}
		for k, v := range metadata {
			o.Metadata[k] = v
		}
	}
}

// WithCluster 设置服务实例所属的集群名称。
func WithCluster(cluster string) RegisterOptionFunc {
	return func(o *RegisterOption) { o.Cluster = cluster }
}

// WithGroup 设置服务实例所属的分组名称。
func WithGroup(group string) RegisterOptionFunc {
	return func(o *RegisterOption) { o.Group = group }
}

// InfoFrom 从通用字段构造 [Instance]，内部调用 [ParseAddr] 解析地址。
//
// name 是服务名称，addr 是注册地址（如 *net.TCPAddr），
// weight 是实例权重（<=0 时使用 defaultWeight），tags 是元数据。
// defaultWeight 用于 weight<=0 的回退值。
func InfoFrom(name string, addr net.Addr, weight, defaultWeight int, tags map[string]string) (Instance, error) {
	if name == "" {
		return Instance{}, fmt.Errorf("registry: service name is empty")
	}
	ip, port, err := ParseAddr(addr)
	if err != nil {
		return Instance{}, err
	}
	w := weight
	if w <= 0 {
		w = defaultWeight
	}
	return Instance{
		Service:  name,
		IP:       ip,
		Port:     port,
		Weight:   float64(w),
		Metadata: tags,
	}, nil
}

// EncodeTargetTags 将 host 和 tags 编码为确定性的缓存 key。
// tags 的 key 按字典序排序，确保相同 tags 产生相同结果（Go map 迭代顺序不确定）。
// 当 tags 为空时直接返回 host。
func EncodeTargetTags(host string, tags map[string]string) string {
	if len(tags) == 0 {
		return host
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString(host)
	sb.WriteString("?")
	for i, k := range keys {
		if i > 0 {
			sb.WriteString("&")
		}
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(tags[k])
	}
	return sb.String()
}

// 说明：Kitex/Hertz 的实例类型转换函数（原 InstancesToKitex/InstancesToHertz）
// 已迁至 kitex 仓的 kitex/registry 与 hertz 仓的 hertz/registry 包——
// 本包是 provider 无关的 SPI，不得 import 任何具体框架（oekit 平台契约 github.com/oekit/oekit.github.io/blob/main/docs/PLATFORM_CONTRACT.md）。
