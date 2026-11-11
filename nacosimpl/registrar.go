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

// resolveGroupCluster 解析本次注册/反注册生效的 Group 与 Cluster：
// inst.Group / inst.Cluster 非空时覆盖 Registrar 构造时指定的默认值
// （#140：Register/Deregister 曾各写一遍逐字相同的覆盖逻辑，收敛为单源）。
func (r *Registrar) resolveGroupCluster(inst registry.Instance) (string, string) {
	groupName := r.group
	if inst.Group != "" {
		groupName = inst.Group
	}
	clusterName := r.cluster
	if inst.Cluster != "" {
		clusterName = inst.Cluster
	}
	return groupName, clusterName
}

// Register 将服务实例注册到 Nacos。
//
// 从 inst 读取 Service、IP、Port、Weight、Metadata。
// inst.Group / inst.Cluster 非空时覆盖 Registrar 默认值。
//
// ctx 语义（#65）：SDK API 不接收 context，无法真正中断底层调用。这里以
// 竞速方式兜底——ctx 已取消/超时时快速失败返回 ctx.Err()（不发起或不再等待
// SDK 调用）；SDK 调用若已在进行会在后台自行完成（结果丢弃）。
func (r *Registrar) Register(ctx context.Context, inst registry.Instance) error {
	weight := inst.Weight
	if weight <= 0 {
		weight = 10
	}

	groupName, clusterName := r.resolveGroupCluster(inst)

	ok, err := invokeWithCtx(ctx, func() (bool, error) {
		return r.client.RegisterInstance(vo.RegisterInstanceParam{
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
//
// ctx 语义（#65）：与 [Registrar.Register] 相同——关停期给本调用挂
// context.WithTimeout 的超时预算会生效（ctx 到点立即返回 ctx.Err()），
// 但底层 SDK 反注册无法被中断，会在后台自行完成。Nacos 临时实例
// （Ephemeral=true）心跳停止后也会被服务端摘除，反注册是加速手段。
func (r *Registrar) Deregister(ctx context.Context, inst registry.Instance) error {
	groupName, clusterName := r.resolveGroupCluster(inst)

	ok, err := invokeWithCtx(ctx, func() (bool, error) {
		return r.client.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip:          inst.IP,
			Port:        uint64(inst.Port),
			ServiceName: inst.Service,
			GroupName:   groupName,
			Cluster:     clusterName,
			Ephemeral:   true,
		})
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
