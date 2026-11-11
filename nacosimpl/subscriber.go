package nacos

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"slices"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/registry"
)

// Subscriber 实现 [registry.Subscriber] 接口，基于 Nacos naming client。
//
// 利用 Nacos 的推送机制，当服务实例列表变化时回调注册的 callback。
//
// 回调的 registry.Instance 会还原 Cluster（#94），但不带 Group：
// nacos-sdk-go v2 把 Group 放在 Service 层而不在 model.Instance 上，
// 推送结果无法归属分组（Nacos provider 的 Subscribe 不返回 Group）。
type Subscriber struct {
	client naming_client.INamingClient
	group  string

	mu sync.Mutex
	// lastErrs 按服务名记录最近一次 SDK 推送错误（#54 可观测面；#142 起按
	// 服务分账）：条目存在 = 该服务本地缓存视图可能是旧版本（直至**该服务
	// 自己**的下一次成功推送清除）。无条目 = 视图新鲜（或从未订阅）。
	// 此前是 Subscriber 全局单值 lastErr：任一服务失败置位、任一服务成功
	// 即清零——多服务订阅下服务 A 的失败会被 B 的成功掩盖（陈旧视图伪装成
	// 健康），反之 B 的错误也会让巡检误判 A。
	lastErrs map[string]error
	// subs 记录 Subscribe 成功注册的那个 *vo.SubscribeParam（#95）。
	//
	// nacos-sdk-go v2 移除回调按 **指针身份** 匹配：Unsubscribe 时 SDK
	// 用 NewSubscribeCallbackFuncWrapper(&param.SubscribeCallback) 构造
	// wrapper，再与注册时的 wrapper 逐个比较 CallbackFunc 指针
	// （naming_cache.RemoveCallbackFunc）。若 Unsubscribe 重新构造 param，
	// 新的 &param.SubscribeCallback 与注册时的地址必然不同 ⇒ wrapper
	// 永不移除 ⇒ 回调 ghost 不死，且 IsSubscribed 恒 true 连 gRPC 层
	// 退订（serviceProxy.Unsubscribe）也被跳过。因此必须原样复用注册时
	// 的同一个 param——顺带保证 GroupName/Clusters 语义天然对齐。
	// 同一服务允许重复 Subscribe（SDK 支持同服务多回调），故按服务名
	// 存列表；Unsubscribe 全部移除。
	subs map[string][]*vo.SubscribeParam
}

// LastErr 返回最近一次 SDK 推送错误的**全量聚合**（nil = 所有订阅服务的
// 本地缓存视图都新鲜）。多服务订阅时按服务名聚合（[errors.Join]，服务名
// 字典序，错误文本各自携带服务名可判定归属）。
// 供健康检查/巡检暴露「视图可能过期」——此前推送失败只记日志，
// discover 返回过期实例列表而无任何可见信号（#54）。
//
// 只关心单个服务时用 [Subscriber.LastErrFor]：任一服务的失败不会稀释
// 其他服务的新鲜状态，也不会反向误报（#142）。
func (s *Subscriber) LastErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(s.lastErrs)) {
		if err := s.lastErrs[name]; err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// LastErrFor 返回指定服务最近一次 SDK 推送错误（nil = 该服务本地缓存视图
// 新鲜，或从未订阅/已退订）。
// 多服务订阅时按服务取账（#142）：全局聚合 [Subscriber.LastErr] 无法区分
// 「哪个服务的视图过期」，按服务读取才能把「视图过期」信号归到正确的服务。
func (s *Subscriber) LastErrFor(serviceName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErrs[serviceName]
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
		client:   client,
		group:    "DEFAULT_GROUP",
		subs:     make(map[string][]*vo.SubscribeParam),
		lastErrs: make(map[string]error),
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
//
// cb 在 SDK 的推送链路上被调用（naming 侧为 gRPC 双向流收包 goroutine，
// 全程无 recover）：本包就地 recover 隔离 cb 的 panic（转错误日志），
// 消费方回调异常不得炸穿 SDK 派发链打死宿主进程（#96，SPI 契约见
// [registry.Subscriber]）。
//
// 同一服务重复 Subscribe 会叠加注册多个回调（SDK 支持同服务多回调），
// Unsubscribe 时一并移除。
func (s *Subscriber) Subscribe(ctx context.Context, serviceName string, cb func([]registry.Instance)) error {
	param := &vo.SubscribeParam{
		ServiceName: serviceName,
		GroupName:   s.group,
		SubscribeCallback: func(services []model.Instance, err error) {
			// #96：SDK 派发链全程无 recover，回调 panic 会沿 gRPC 收包
			// goroutine 上抛打死整个进程。消费方回调质量不可控（nil map/
			// 类型断言/下游透传），就地隔离转为错误日志——不传播、不改变
			// 同步推送语义。
			defer func() {
				if r := recover(); r != nil {
					log.Error("subscribe callback panicked (isolated)",
						"service", serviceName, "err", r, "stack", string(debug.Stack()))
				}
			}()
			if err != nil {
				// #54：SDK 推送失败 ⇒ 本地缓存视图滞留旧版本直至下次推送。
				// 此前只 log.Error：过期视图零可见信号。补错误标记（LastErr/
				// LastErrFor 可查询）+ SDK 会持续重推（nacos 客户端有自己的
				// 容错循环），该服务恢复后其账目清除——消费方健康检查可据此
				// 暴露「视图过期」。按服务分账（#142）：A 的失败只记 A 的账，
				// 不被 B 的成功推送冲掉。
				log.Error("subscribe callback error (cached view may be stale until next push)",
					"service", serviceName, "err", err)
				s.mu.Lock()
				s.lastErrs[serviceName] = fmt.Errorf("nacosimpl: subscribe push for %s: %w", serviceName, err)
				s.mu.Unlock()
				return
			}
			s.mu.Lock()
			delete(s.lastErrs, serviceName) // 该服务自己的成功推送清它自己的账（#142）
			s.mu.Unlock()
			cb(toInstances(serviceName, services))
		},
	}
	if err := s.client.Subscribe(param); err != nil {
		return wrapErr("subscribe", serviceName, err)
	}
	// #95：留存注册时的 param——Unsubscribe 必须复用同一指针（&param.
	// SubscribeCallback 地址同一），否则 SDK 按指针身份移除永远匹配不上。
	// 注册成功才入账；失败则不留无法退订的残账。
	s.mu.Lock()
	s.subs[serviceName] = append(s.subs[serviceName], param)
	s.mu.Unlock()
	log.Info("subscribed", "service", serviceName, "group", s.group)
	return nil
}

// Unsubscribe 取消订阅指定服务的实例变更。
//
// 复用 Subscribe 注册时的同一 *vo.SubscribeParam（#95）：SDK 按
// &param.SubscribeCallback 指针身份移除回调，新构造的 param 永远匹配
// 不上 ⇒ 回调 ghost 不死，且 IsSubscribed 恒 true 连 gRPC 层退订也被
// 跳过。移除全部 wrapper 后 IsSubscribed=false，SDK 自会触发 gRPC 层
// 退订（serviceProxy.Unsubscribe）。
//
// 无注册记录（从未 Subscribe 或已退订）时仍向 SDK 发起一次退订（保持
// 旧行为）：SDK 侧无可移除的 wrapper，IsSubscribed=false 直接走 gRPC 层
// 退订。退订失败的 param 会重新入账——重试时仍复用同一指针，换新指针
// 将再次变成静默 no-op。
func (s *Subscriber) Unsubscribe(ctx context.Context, serviceName string) error {
	s.mu.Lock()
	params := s.subs[serviceName]
	delete(s.subs, serviceName)
	s.mu.Unlock()
	if len(params) == 0 {
		params = []*vo.SubscribeParam{{ServiceName: serviceName, GroupName: s.group}}
	}
	for i, param := range params {
		if err := s.client.Unsubscribe(param); err != nil {
			// 失败的（含未处理的）param 放回：重试时仍复用同一指针。
			s.mu.Lock()
			s.subs[serviceName] = append(s.subs[serviceName], params[i:]...)
			s.mu.Unlock()
			return wrapErr("unsubscribe", serviceName, err)
		}
	}
	// #142：全部退订成功后该服务不再有推送，其 per-service 错误账目随之
	// 移除——否则一次 unclean 推送留下的错误会在聚合面（LastErr）永久滞留，
	// 健康检查对已退订服务误报「视图过期」。退订失败（订阅仍在）则保留账目。
	s.mu.Lock()
	delete(s.lastErrs, serviceName)
	s.mu.Unlock()
	log.Info("unsubscribed", "service", serviceName, "group", s.group)
	return nil
}
