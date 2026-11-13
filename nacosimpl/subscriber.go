package nacos

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/logx"
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
	//
	// ⚠️ 送达面事实（#161）：钉死的 nacos-sdk-go v2.3.6 对 SubscribeCallback
	// 恒传 err=nil（SDK 全仓唯一调用点 naming_cache/subscribe_callback_
	// wrapper.go:59），本账目在当前版本的生产路径上不会被置位——出口语义
	// 见 [Subscriber.LastErr]。
	lastErrs map[string]error
	// pushNano 按服务名记录最近一次**成功**推送（回调无 err）的 UnixNano
	// （#161 真实信号源）：err 账目在钉死的 SDK 版本上不可达（见 lastErrs），
	// 「视图过期」的唯一可达判据是「该服务最近一次成功回调时刻 vs 当前时刻」
	// ——SDK 后台刷新失败（service_info_updater.go updateServiceNow）连续
	// 不达时本信号停走，消费方按业务容忍度取阈值（#54 的真实可达形态）。
	pushNano map[string]int64
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
// 错误账目为空）。多服务订阅时按服务名聚合（[errors.Join]，服务名字典序，
// 错误文本各自携带服务名可判定归属）。
//
// ⚠️ 送达面事实（#161）：钉死的 nacos-sdk-go v2.3.6 对 SubscribeCallback 的
// 唯一调用点（naming_cache/subscribe_callback_wrapper.go:59 notifyListener）
// 恒以 err=nil 调用——本出口在当前钉死版本的生产路径上**永远返回 nil**
// （只有测试桩手动造非 nil err 能置位账目），不可用作健康检查的「视图
// 过期」判据：SDK 后台刷新失败（service_info_updater.go updateServiceNow）
// 只记日志，连回调都不触发。本出口保留作 SPI 契约面（SDK 升级到会把 err
// 送达回调的版本后自动生效）与同 SPI 其它实现的语义对齐；「视图新鲜度」
// 的真实信号源用 [Subscriber.LastPushFor]（当前版本可达，#161）。
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

// LastErrFor 返回指定服务最近一次 SDK 推送错误（nil = 该服务错误账目为空，
// 或从未订阅/已退订）。
//
// ⚠️ 送达面事实同 [Subscriber.LastErr]（#161）：钉死的 SDK v2.3.6 上本出口
// 生产路径恒 nil；「视图新鲜度」判据用 [Subscriber.LastPushFor]。
//
// 多服务订阅时按服务取账（#142）：全局聚合 [Subscriber.LastErr] 无法区分
// 「哪个服务的视图过期」，按服务读取才能把「视图过期」信号归到正确的服务。
func (s *Subscriber) LastErrFor(serviceName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErrs[serviceName]
}

// LastPushFor 返回指定服务最近一次**成功**推送（回调无 err）的时刻
// （#161 真实信号源）。第二返回值 false = 该服务从未成功推送（或已退订）。
//
// 这是「视图过期」在当前钉死 SDK（v2.3.6）上唯一可达的判据：SDK 不把推送
// 失败传进回调（err 恒 nil，见 [Subscriber.LastErr]），但成功回调本身是
// 真实事件——消费方按业务容忍度取阈值：
//
//	if ts, ok := sub.LastPushFor("svc"); !ok || time.Since(ts) > 5*time.Minute {
//	    // 视图可能过期：SDK 后台刷新失败（updateServiceNow）连续不达时
//	    // 本信号停走，巡检可据此告警（#54 的真实可达形态）
//	}
func (s *Subscriber) LastPushFor(serviceName string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nano, ok := s.pushNano[serviceName]
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(0, nano), true
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
		pushNano: make(map[string]int64),
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
						"service", serviceName, logx.ErrKey, r, "stack", string(debug.Stack()))
				}
			}()
			if err != nil {
				// #54/#142：per-service 错误账目（SPI 契约面）。⚠️ 送达面
				// 事实（#161）：钉死的 nacos-sdk-go v2.3.6 对回调恒传
				// err=nil（唯一调用点 subscribe_callback_wrapper.go:59），
				// 本分支在当前版本的生产路径不可达——保留作 SPI 契约面
				// （SDK 升级后自动生效）与测试桩语义；「视图过期」的真实
				// 可达信号源见 LastPushFor（成功回调时刻）。
				log.Error("subscribe callback error (cached view may be stale until next push)",
					"service", serviceName, logx.ErrKey, err)
				s.mu.Lock()
				s.lastErrs[serviceName] = fmt.Errorf("nacosimpl: subscribe push for %s: %w", serviceName, err)
				s.mu.Unlock()
				return
			}
			s.mu.Lock()
			delete(s.lastErrs, serviceName)                 // 该服务自己的成功推送清它自己的账（#142）
			s.pushNano[serviceName] = time.Now().UnixNano() // #161：真实信号源
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
	// pushNano 同步移除（#161）：已退订服务不再有成功推送，时刻账目失去
	// 意义，保留会让 LastPushFor 误报「曾经推送过」。
	s.mu.Lock()
	delete(s.lastErrs, serviceName)
	delete(s.pushNano, serviceName)
	s.mu.Unlock()
	log.Info("unsubscribed", "service", serviceName, "group", s.group)
	return nil
}
