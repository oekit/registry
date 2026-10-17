package nacos

import (
	"context"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/registry"
)

// Discoverer 实现 [registry.Discoverer] 接口，基于 Nacos naming client。
//
// 只返回健康实例（HealthyOnly=true）。
type Discoverer struct {
	client   naming_client.INamingClient
	group    string
	clusters []string
}

var _ registry.Discoverer = (*Discoverer)(nil)

// DiscovererOption 是 Discoverer 的配置选项函数类型。
type DiscovererOption func(*Discoverer)

// WithDiscovererGroup 设置发现器使用的默认服务分组。
// 默认值 "DEFAULT_GROUP"。
func WithDiscovererGroup(group string) DiscovererOption {
	return func(d *Discoverer) { d.group = group }
}

// WithDiscovererClusters 设置发现器查询的集群列表。
// 默认空（查询所有集群）。
func WithDiscovererClusters(clusters ...string) DiscovererOption {
	return func(d *Discoverer) { d.clusters = clusters }
}

// NewDiscoverer 创建基于 Nacos 的服务发现器。
//
// 必须传入非 nil 的 namingClient，否则返回 ErrNilClient。
func NewDiscoverer(client naming_client.INamingClient, opts ...DiscovererOption) (*Discoverer, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	d := &Discoverer{
		client: client,
		group:  "DEFAULT_GROUP",
	}
	for _, opt := range opts {
		opt(d)
	}
	return d, nil
}

// Discover 根据服务名称从 Nacos 拉取健康实例列表。
//
// Group 使用 Discoverer 的默认值，Clusters 使用 Discoverer 的默认值。
func (d *Discoverer) Discover(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	param := vo.SelectInstancesParam{
		ServiceName: serviceName,
		GroupName:   d.group,
		HealthyOnly: true,
	}
	if len(d.clusters) > 0 {
		param.Clusters = d.clusters
	}

	instances, err := d.client.SelectInstances(param)
	if err != nil {
		return nil, wrapErr("discover", serviceName, err)
	}

	return toInstances(serviceName, instances), nil
}

// DiscoverAll 根据服务名称从 Nacos 拉取全部实例列表（包括不健康的实例）。
//
// Group 和 Clusters 使用 Discoverer 的默认值。
func (d *Discoverer) DiscoverAll(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	param := vo.SelectAllInstancesParam{
		ServiceName: serviceName,
		GroupName:   d.group,
	}
	if len(d.clusters) > 0 {
		param.Clusters = d.clusters
	}

	instances, err := d.client.SelectAllInstances(param)
	if err != nil {
		return nil, wrapErr("discover all", serviceName, err)
	}

	return toInstances(serviceName, instances), nil
}
