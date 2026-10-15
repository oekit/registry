package nacos

import (
	"context"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/registry/registry"
)

// Subscriber 实现 [registry.Subscriber] 接口，基于 Nacos naming client。
//
// 利用 Nacos 的推送机制，当服务实例列表变化时回调注册的 callback。
type Subscriber struct {
	client naming_client.INamingClient
	group  string
}

var _ registry.Subscriber = (*Subscriber)(nil)

// SubscriberOption 是 Subscriber 的配置选项函数类型。
type SubscriberOption func(*Subscriber)

// WithSubscriberGroup 设置订阅器使用的默认服务分组。
// 默认值 "DEFAULT_GROUP"。
func WithSubscriberGroup(group string) SubscriberOption {
	return func(s *Subscriber) { s.group = group }
}

// NewSubscriber 创建基于 Nacos 的服务订阅器。
//
// 必须传入非 nil 的 namingClient，否则返回 ErrNilClient。
func NewSubscriber(client naming_client.INamingClient, opts ...SubscriberOption) (*Subscriber, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	s := &Subscriber{
		client: client,
		group:  "DEFAULT_GROUP",
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Subscribe 订阅指定服务的实例变更。
//
// 当 Nacos 推送实例变更时，将 SDK 的 model.Instance 列表转换为
// provider 无关的 registry.Instance 列表后回调 cb。
func (s *Subscriber) Subscribe(ctx context.Context, serviceName string, cb func([]registry.Instance)) error {
	err := s.client.Subscribe(&vo.SubscribeParam{
		ServiceName: serviceName,
		GroupName:   s.group,
		SubscribeCallback: func(services []model.Instance, err error) {
			if err != nil {
				log.Error("subscribe callback error", "service", serviceName, "err", err)
				return
			}
			cb(toInstances(serviceName, services))
		},
	})
	if err != nil {
		return wrapErr("subscribe", serviceName, err)
	}
	log.Info("subscribed", "service", serviceName, "group", s.group)
	return nil
}

// Unsubscribe 取消订阅指定服务的实例变更。
func (s *Subscriber) Unsubscribe(ctx context.Context, serviceName string) error {
	err := s.client.Unsubscribe(&vo.SubscribeParam{
		ServiceName: serviceName,
		GroupName:   s.group,
	})
	if err != nil {
		return wrapErr("unsubscribe", serviceName, err)
	}
	log.Info("unsubscribed", "service", serviceName, "group", s.group)
	return nil
}
