package nacos

import (
	"context"
	"errors"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/registry"
)

// errRegisterReturnedFalse 表示 Nacos SDK 注册返回 false（非 error）。
var errRegisterReturnedFalse = errors.New("nacos returned false")

// errDeregisterReturnedFalse 表示 Nacos SDK 注销返回 false（非 error）。
var errDeregisterReturnedFalse = errors.New("nacos deregister returned false")

// ErrNilClient 表示构造时传入了 nil 的 SDK client。
var ErrNilClient = errors.New("registry/nacos: client must be non-nil")

// Registrar 实现 [registry.Registrar] 接口，基于 Nacos naming client。
//
// 实例以临时模式（Ephemeral=true，心跳维持）注册，适合 AP 场景。
// Group 和 Cluster 在构造时指定，作为该 Registrar 所有注册操作的默认值。
type Registrar struct {
	client  naming_client.INamingClient
	group   string
	cluster string
}

var _ registry.Registrar = (*Registrar)(nil)

// RegistrarOption 是 Registrar 的配置选项函数类型。
type RegistrarOption func(*Registrar)

// WithRegistrarGroup 设置注册器使用的默认服务分组。
// 默认值 "DEFAULT_GROUP"。
func WithRegistrarGroup(group string) RegistrarOption {
	return func(r *Registrar) { r.group = group }
}

// WithRegistrarCluster 设置注册器使用的默认集群名称。
// 默认值空字符串（Nacos SDK 内部使用 "DEFAULT"）。
func WithRegistrarCluster(cluster string) RegistrarOption {
	return func(r *Registrar) { r.cluster = cluster }
}

// NewRegistrar 创建基于 Nacos 的服务注册器。
//
// 必须传入非 nil 的 namingClient，否则返回 ErrNilClient。
func NewRegistrar(client naming_client.INamingClient, opts ...RegistrarOption) (*Registrar, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	r := &Registrar{
		client: client,
		group:  "DEFAULT_GROUP",
	}
	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

// Register 将服务实例注册到 Nacos。
//
// 从 inst 读取 Service、IP、Port、Weight、Metadata。
// inst.Group / inst.Cluster 非空时覆盖 Registrar 默认值。
func (r *Registrar) Register(ctx context.Context, inst registry.Instance) error {
	weight := inst.Weight
	if weight <= 0 {
		weight = 10
	}

	groupName := r.group
	if inst.Group != "" {
		groupName = inst.Group
	}
	clusterName := r.cluster
	if inst.Cluster != "" {
		clusterName = inst.Cluster
	}

	ok, err := r.client.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          inst.IP,
		Port:        uint64(inst.Port),
		ServiceName: inst.Service,
		GroupName:   groupName,
		ClusterName: clusterName,
		Weight:      weight,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    inst.Metadata,
	})
	if err != nil {
		return wrapErr("register", inst.Service, err)
	}
	if !ok {
		return wrapErr("register", inst.Service, errRegisterReturnedFalse)
	}
	log.Info("registered", "service", inst.Service, "ip", inst.IP, "port", inst.Port, "group", groupName)
	return nil
}

// Deregister 从 Nacos 注销服务实例。
//
// inst.Group / inst.Cluster 非空时覆盖 Registrar 默认值。
func (r *Registrar) Deregister(ctx context.Context, inst registry.Instance) error {
	groupName := r.group
	if inst.Group != "" {
		groupName = inst.Group
	}
	clusterName := r.cluster
	if inst.Cluster != "" {
		clusterName = inst.Cluster
	}

	ok, err := r.client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          inst.IP,
		Port:        uint64(inst.Port),
		ServiceName: inst.Service,
		GroupName:   groupName,
		Cluster:     clusterName,
		Ephemeral:   true,
	})
	if err != nil {
		return wrapErr("deregister", inst.Service, err)
	}
	if !ok {
		return wrapErr("deregister", inst.Service, errDeregisterReturnedFalse)
	}
	log.Info("deregistered", "service", inst.Service, "ip", inst.IP, "port", inst.Port, "group", groupName)
	return nil
}
