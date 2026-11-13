// Package configbridge 提供配置监听的 per-client 多播桥（registry#177 治本单源）。
//
// 为什么需要：nacos-sdk 的 ListenConfig 按 (dataID, group) **单槽**——
// 同 key 第二次注册复用旧 cacheData、新传入的 OnChange 在整个函数体内无
// 消费点（静默丢弃且返回 nil）；CancelListenConfig 按 key 整删，任一订阅
// 者停止会把同 key 所有订阅者一并互杀（config_client.go:297-360 实测）。
// 同一封装内多组件共听同一配置（nns 的 ConfigStore、nacos 的
// config_loader + config_source）即触发：后订阅者零信号永久收不到变更，
// 任一取消其余静默失联——「配置中心改了下不去」的故障无从排查。
//
// 本包把「单槽」抬升为「多播」：按 client 身份共享一个桥，底层 SDK 只在
// 首个订阅者进入时注册一次桥接回调；之后 Subscribe/Unsubscribe 只增删本
// 桥的本地表项，订阅句柄互不影响，最后一个订阅者离开才 Cancel 底层。
//
// 依赖注入而非 import SDK：本包零第三方依赖（父仓红线），Listen/Cancel
// 由各模块（nacosimpl/nacos）以闭包注入其 SDK 调用——与父仓
// 「实现依赖接口」的分层纪律一致。
//
// 派发口径（与 etcdx ConfigStore 对齐）：
//   - 同一桥的回调**串行执行**（不会并发调用 onChange），快速连更按到达序投递；
//   - 单个订阅者的 panic 就地隔离（不影响同 key 其他订阅者，进程存活）。
package configbridge

import (
	"fmt"
	"runtime/debug"
	"sync"
)

// ChangeFunc 与 nacos-sdk 的 OnChange 同签名（namespace, group, dataId, data），
// 由桥接回调向底层注册时透传给 SDK。
type ChangeFunc func(namespace, group, dataID, data string)

// Hooks 是底层监听后端的注入面：各模块用自己的 SDK 调用闭包实现。
//
// Listen 在**该桥该 key 的首个订阅者**进入时被调用一次；Cancel 在**最后一个**
// 订阅者离开时被调用一次。Listen 失败会回滚本地表项（桥对调用方呈现
// 「注册失败」而非半开状态）。
//
// OnPanic 是订阅者回调 panic 的隔离上报口（可为 nil：只吞不报）。注入方
// 在此接自己的日志器（父仓零 logx 依赖，日志面不进本包）。
type Hooks struct {
	Listen  func(dataID, group string, onChange ChangeFunc) error
	Cancel  func(dataID, group string) error
	OnPanic func(dataID, group string, r any, stack string)
}

// Handle 是订阅句柄：Unsubscribe 按句柄精确摘除，不受其他订阅者进出影响。
type Handle uint64

// Bridge 是按 client 身份共享的多播桥（见 [For]）。
type Bridge struct {
	hooks Hooks

	// regMu 串行化注册路径（Subscribe 的 Listen 调用 / Unsubscribe 的
	// Cancel 调用）：底层 SDK 调用可能含网络首拉，期间不得有并发 Subscribe
	// 追加表项——否则 Listen 失败的回滚无法呈现干净失败（并发者已拿到
	// 「成功」句柄而底层没注册）。派发路径只拿 mu（快锁），不受 regMu 阻塞。
	regMu sync.Mutex

	mu      sync.Mutex       // 保护 subs/bridged/dispatches/nextID（快路径）
	subs    map[string][]sub // key → 订阅栈（LIFO 摘除）
	bridged map[string]bool  // key → 底层是否已注册
	// dispatches 按 key 的派发锁（#127：同 key 回调不并发、不乱序）。
	// per-key 而非整桥：某 key 的订阅者慢/背压（如 config_loader 的队列
	// 入队）只拖住本 key 的推送，不跨 key 耦合——与原 per-Listen 交付锁
	// 的隔离面等价。
	dispatches map[string]*sync.Mutex
	nextID     Handle
}

type sub struct {
	id Handle
	cb func(namespace, group, dataID, data string)
}

func keyOf(dataID, group string) string { return group + "\x00" + dataID }

var bridges sync.Map // client 身份键 → *Bridge

// For 返回 client 对应的桥；首次调用用注入的 hooks 构造。
//
// client 用作身份键（通常为 SDK client 指针，可比较）——同一 client 的全部
// 封装（ConfigStore/ConfigSource/config_loader）拿到**同一个桥**，跨封装同
// key 也走同一张多播表，这正是 registry#177 互杀场景的闭合点。
//
// 同一 client 先后以不同 hooks 调用时，**首个 hooks 生效**（后到者忽略）：
// hooks 是构造参数而非订阅参数，同 client 的底层后端必然一致。
func For(client any, hooks Hooks) *Bridge {
	if v, ok := bridges.Load(client); ok {
		return v.(*Bridge)
	}
	b := newBridge(hooks)
	actual, _ := bridges.LoadOrStore(client, b)
	return actual.(*Bridge)
}

// New 单独构造一个桥（不进全局表）——给无共享身份的场景与测试用。
func New(hooks Hooks) *Bridge { return newBridge(hooks) }

func newBridge(hooks Hooks) *Bridge {
	return &Bridge{
		hooks:      hooks,
		subs:       make(map[string][]sub),
		bridged:    make(map[string]bool),
		dispatches: make(map[string]*sync.Mutex),
	}
}

// lockFor 返回 key 的派发锁（懒初始化，调用方须持 b.mu）。
func (b *Bridge) lockFor(k string) *sync.Mutex {
	m, ok := b.dispatches[k]
	if !ok {
		m = &sync.Mutex{}
		b.dispatches[k] = m
	}
	return m
}

// Subscribe 登记一个回调（SDK OnChange 四元组保真透传）并返回句柄。
//
// 同 key 已有底层注册时只追加本地表项（不再次调 hooks.Listen——SDK 单槽
// 语义由此被多播覆盖）；首个订阅者进入才调 hooks.Listen。
func (b *Bridge) Subscribe(dataID, group string, cb func(namespace, group, dataID, data string)) (Handle, error) {
	if cb == nil {
		return 0, fmt.Errorf("configbridge: nil onChange for %s/%s", group, dataID)
	}
	k := keyOf(dataID, group)

	b.regMu.Lock()
	defer b.regMu.Unlock()

	b.mu.Lock()
	b.nextID++
	id := b.nextID
	first := !b.bridged[k]
	if first {
		b.bridged[k] = true // 先占位（regMu 已串行化，无并发 Subscribe 竞争）
	}
	b.subs[k] = append(b.subs[k], sub{id: id, cb: cb})
	b.mu.Unlock()

	if !first {
		return id, nil
	}
	if err := b.hooks.Listen(dataID, group, b.makeRelay(dataID, group, k)); err != nil {
		// 注册失败：回滚本订阅与占位（含 bridged 复位，否则后续 Subscribe
		// 会误判「已注册」跳过 Listen，呈现假成功），桥对调用方呈现干净的失败。
		b.mu.Lock()
		b.removeLocked(k, id)
		if len(b.subs[k]) == 0 {
			delete(b.subs, k)
			b.bridged[k] = false
		}
		b.mu.Unlock()
		return 0, fmt.Errorf("configbridge: listen %s/%s: %w", group, dataID, err)
	}
	return id, nil
}

// Unsubscribe 按句柄摘除订阅；该 key 最后一个订阅者离开时调 hooks.Cancel。
//
// 句柄已摘除（重复调用/并发双摘）时幂等返回 nil——停止路径不该因竞态报错。
func (b *Bridge) Unsubscribe(dataID, group string, id Handle) error {
	k := keyOf(dataID, group)

	b.regMu.Lock()
	defer b.regMu.Unlock()

	b.mu.Lock()
	if !b.removeLocked(k, id) {
		b.mu.Unlock()
		return nil // 幂等：句柄不在（已摘/不存在）
	}
	last := len(b.subs[k]) == 0
	if last {
		delete(b.subs, k)
		b.bridged[k] = false
	}
	b.mu.Unlock()

	if !last {
		return nil
	}
	if err := b.hooks.Cancel(dataID, group); err != nil {
		return fmt.Errorf("configbridge: cancel %s/%s: %w", group, dataID, err)
	}
	return nil
}

// removeLocked 摘除指定句柄；返回是否摘到（调用方须持 b.mu）。
func (b *Bridge) removeLocked(k string, id Handle) bool {
	list := b.subs[k]
	for i, s := range list {
		if s.id == id {
			b.subs[k] = append(list[:i], list[i+1:]...)
			return true
		}
	}
	return false
}

// makeRelay 构造注册给底层的桥接回调：读订阅快照后串行逐个投递，
// 单订阅者 panic 就地隔离。
func (b *Bridge) makeRelay(dataID, group, k string) ChangeFunc {
	return func(namespace, _, _, data string) {
		b.mu.Lock()
		list := make([]func(namespace, group, dataID, data string), len(b.subs[k]))
		for i, s := range b.subs[k] {
			list[i] = s.cb
		}
		b.mu.Unlock()
		if len(list) == 0 {
			return // 全部已摘（Cancel 在途窗口）：丢弃本次推送
		}

		b.mu.Lock()
		dl := b.lockFor(k)
		b.mu.Unlock()
		dl.Lock()
		defer dl.Unlock() // #127：同 key 串行，快速连更按到达序
		for _, cb := range list {
			func(cb func(namespace, group, dataID, data string)) {
				defer func() {
					if r := recover(); r != nil {
						// #96/#125 同口径：SDK 派发链无 recover，消费方回调
						// panic 会打死进程；多播下还要互不影响——逐订阅隔离。
						if b.hooks.OnPanic != nil {
							b.hooks.OnPanic(dataID, group, r, string(debug.Stack()))
						}
					}
				}()
				cb(namespace, group, dataID, data)
			}(cb)
		}
	}
}

// ResetFor 清除指定 client 的桥注册（测试专用：避免用例间串表）。
func ResetFor(client any) { bridges.Delete(client) }
