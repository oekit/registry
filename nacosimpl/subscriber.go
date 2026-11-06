package nacos

import (
	"context"
	"fmt"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/registry"
)

// Subscriber 实现 [registry.Subscriber] 接口，基于 Nacos naming client。
//
// 利用 Nacos 的推送机制，当服务实例列表变化时回调注册的 callback。
type Subscriber struct {
	client naming_client.INamingClient
	group  string

	mu sync.Mutex
	// lastErr 记录最近一次 SDK 推送错误（#54 可观测面）：非 nil = 本地
	// 缓存视图可能是旧版本（直至下一次成功推送清零）。nil = 视图新鲜。
	lastErr error
}

// LastErr 返回最近一次 SDK 推送错误（nil = 本地缓存视图新鲜）。
// 供健康检查/巡检暴露「视图可能过期」——此前推送失败只记日志，
// discover 返回过期实例列表而无任何可见信号（#54）。
func (s *Subscriber) LastErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
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
				// #54：SDK 推送失败 ⇒ 本地缓存视图滞留旧版本直至下次推送。
				// 此前只 log.Error：过期视图零可见信号。补错误标记（LastErr
				// 可查询）+ SDK 会持续重推（nacos 客户端有自己的容错循环），
				// 恢复后 LastErr 清零——消费方健康检查可据此暴露「视图过期」。
				log.Error("subscribe callback error (cached view may be stale until next push)",
					"service", serviceName, "err", err)
				s.mu.Lock()
				s.lastErr = fmt.Errorf("nacosimpl: subscribe push for %s: %w", serviceName, err)
				s.mu.Unlock()
				return
			}
			s.mu.Lock()
			s.lastErr = nil
			s.mu.Unlock()
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
