package registrymocks

import (
	"context"
	"sync"

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
func (m *Subscriber) Push(serviceName string, instances []registry.Instance) {
	m.mu.Lock()
	cbs := append([]func([]registry.Instance){}, m.subs[serviceName]...)
	m.mu.Unlock()
	for _, cb := range cbs {
		cb(instances)
	}
}

// CallbackCount 返回指定服务注册的回调数量（测试用）。
func (m *Subscriber) CallbackCount(serviceName string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.subs[serviceName])
}
