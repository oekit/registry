package registrymocks

import (
	"context"
	"log"
	"sync"

	"github.com/oekit/pkg/errors"

	"github.com/oekit/registry"
)

// Subscriber 是 [registry.Subscriber] 的可配置 mock 实现。
//
// 支持订阅后通过 Push 手动触发回调，模拟注册中心的推送行为。
//
// 零值可用（#150）：结构体字面量构造（如 &Subscriber{}）后各方法可直接
// 调用——map 字段在首次写入时惰性初始化，与 NewSubscriber() 等价。
type Subscriber struct {
	mu   sync.Mutex
	subs map[string][]func([]registry.Instance)

	// deliverMu 串行化 cb 投递（#158，SPI 契约「实现必须就地隔离 cb 的
	// panic + 一个服务不稳定的消费代码不能拖崩同进程其他订阅」）：Push 在
	// 释放 mu 后才调回调，多个 goroutine 并发 Push 时 cb 被并发调用——
	// 真实现（nacosimpl 在 SDK 推送 goroutine 上串行投递）与 mock 语义
	// 分裂会让消费方「读-改-写」共享状态代码测试通过、生产才有竞态。
	// 本锁不得与 mu 混用（回调可能回查 Subscribe/CallbackCount 重入 mu）。
	deliverMu sync.Mutex
}

// NewSubscriber 创建一个空的 Subscriber。
func NewSubscriber() *Subscriber {
	return &Subscriber{subs: make(map[string][]func([]registry.Instance))}
}

// Subscribe 注册回调。
func (m *Subscriber) Subscribe(_ context.Context, serviceName string, cb func([]registry.Instance)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 零值可用（#150）：字面量构造后 subs 为 nil，直接写会 panic
	// （assignment to entry in nil map，报错不指向 registrymocks，难归因）
	if m.subs == nil {
		m.subs = make(map[string][]func([]registry.Instance))
	}
	m.subs[serviceName] = append(m.subs[serviceName], cb)
	return nil
}

// Unsubscribe 注销回调。
func (m *Subscriber) Unsubscribe(_ context.Context, serviceName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.subs, serviceName)
	return nil
}

// Push 主动触发指定服务的所有回调，模拟注册中心的实例变更推送。
//
// SPI 契约落地（#158，根包 registry.go 对所有实现的硬性要求，nacosimpl
// 同款）：cb 逐个串行投递（deliverMu 保证并发 Push 不并发调用 cb），每个
// cb 的 panic 经 errors.Recovered 收编转错误日志——一个坏回调不得让同服务
// 的其他回调收不到事件、更不得沿派发链上抛打死宿主测试进程。
func (m *Subscriber) Push(serviceName string, instances []registry.Instance) {
	m.mu.Lock()
	cbs := append([]func([]registry.Instance){}, m.subs[serviceName]...)
	m.mu.Unlock()
	m.deliverMu.Lock()
	defer m.deliverMu.Unlock()
	for _, cb := range cbs {
		if cb == nil {
			continue
		}
		deliverOne(serviceName, cb, instances)
	}
}

// deliverOne 单回调隔离投递（#158）：panic 就地收编转错误日志，后续回调
// 照常投递（SPI：一个服务不稳定的消费代码不能拖崩同进程其他订阅）。
func deliverOne(serviceName string, cb func([]registry.Instance), instances []registry.Instance) {
	defer func() {
		if perr := errors.Recovered(recover()); perr != nil {
			log.Printf("[registrymocks] subscribe callback panicked (isolated): service=%s err=%v stack=%s",
				serviceName, perr, perr.(*errors.PanicError).Stack)
		}
	}()
	cb(instances)
}

// CallbackCount 返回指定服务注册的回调数量（测试用）。
func (m *Subscriber) CallbackCount(serviceName string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.subs[serviceName])
}
