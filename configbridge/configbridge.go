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
	"runtime/debug"
	"sync"

	"github.com/oekit/pkg/errors"
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

	// mu 保护表状态（快路径，绝不跨网络调用持有，#250）。
	mu      sync.Mutex
	subs    map[string][]sub // key → 订阅栈（LIFO 摘除）
	bridged map[string]bool  // key → 底层是否已注册
	// pending 是 key → 在途底层调用完成信号（#250）：
	//   - 首订阅者先在本表占位（bridged 仍为 false、自己的 sub 先入栈），
	//     释放 mu 后在锁外调 hooks.Listen；同 key 的其他 Subscribe 等到
	//     close 后重试（成功则直接追加、失败则接管重试），不再像 regMu
	//     时代那样全桥串行——一个 key 的首拉挂住只阻塞同 key 的注册，
	//     其他 key 的 Subscribe/Unsubscribe 照常进行；
	//   - 最后一个订阅者离开时先清本地表项并占位，锁外调 hooks.Cancel。
	// 判据：pending 存在 ⇔ 该 key 有在途 Listen/Cancel；表状态在调用前
	// 已落定，调用方只在 close 后重读状态，不会看到半开视图。
	pending map[string]chan struct{}
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
		pending:    make(map[string]chan struct{}),
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
		return 0, errors.Newf("configbridge: nil onChange for %s/%s", group, dataID)
	}
	k := keyOf(dataID, group)

	for {
		b.mu.Lock()
		if b.bridged[k] {
			// 已注册：只追加本地表项（快路径，锁外无调用）
			b.nextID++
			id := b.nextID
			b.subs[k] = append(b.subs[k], sub{id: id, cb: cb})
			b.mu.Unlock()
			return id, nil
		}
		if ch, ok := b.pending[k]; ok {
			// 同 key 有在途 Listen/Cancel（#250）：等它完成后按最新状态重试。
			// 释放 mu 等待——其他 key 的注册不受影响（regMu 时代全桥串行）。
			b.mu.Unlock()
			<-ch
			continue
		}

		// 首订阅者：占位 + 自己的 sub 先入栈（table 状态先行），锁外做
		// 可能含网络首拉的 hooks.Listen。等待者看到 pending 即等待，不会
		// 在 Listen 结果未定时追加表项。
		ch := make(chan struct{})
		b.pending[k] = ch
		b.nextID++
		id := b.nextID
		b.subs[k] = append(b.subs[k], sub{id: id, cb: cb})
		b.mu.Unlock()

		err := b.hooks.Listen(dataID, group, b.makeRelay(dataID, group, k))

		b.mu.Lock()
		delete(b.pending, k)
		if err != nil {
			// 注册失败：回滚本订阅（等待者从未入栈）。bridged 保持 false，
			// 后续 Subscribe 会重新接管 Listen，桥对调用方呈现干净的失败。
			b.removeLocked(k, id)
			if len(b.subs[k]) == 0 {
				delete(b.subs, k)
			}
		} else {
			b.bridged[k] = true
		}
		b.mu.Unlock()
		close(ch) // 唤醒等待者（成功 → 直接追加；失败 → 接管重试）

		if err != nil {
			return 0, errors.Wrapf(err, "configbridge: listen %s/%s", group, dataID)
		}
		return id, nil
	}
}

// Unsubscribe 按句柄摘除订阅；该 key 最后一个订阅者离开时调 hooks.Cancel。
//
// 句柄已摘除（重复调用/并发双摘）时幂等返回 nil——停止路径不该因竞态报错。
func (b *Bridge) Unsubscribe(dataID, group string, id Handle) error {
	k := keyOf(dataID, group)

	for {
		b.mu.Lock()
		if !b.hasSubLocked(k, id) {
			b.mu.Unlock()
			return nil // 幂等：句柄不在（已摘/不存在），不等待无关的在途调用
		}
		if ch, ok := b.pending[k]; ok {
			// 同 key 有在途 Listen/Cancel：等它完成后按最新状态重试
			// （在途 Listen 期间本句柄尚不可能存在，此分支主要覆盖
			// Cancel 在途时的并发双摘——重试后幂等返回）。
			b.mu.Unlock()
			<-ch
			continue
		}
		b.removeLocked(k, id)
		if len(b.subs[k]) > 0 {
			b.mu.Unlock()
			return nil
		}

		// 最后一个订阅者：本地表项先落定（key 清空、bridged 复位），占位后
		// 锁外调可能含网络调用的 hooks.Cancel（#250）。等待者不会在 Cancel
		// 在途时抢先重新注册（pending 挡在 Listen 之前）。
		ch := make(chan struct{})
		b.pending[k] = ch
		delete(b.subs, k)
		b.bridged[k] = false
		b.mu.Unlock()

		err := b.hooks.Cancel(dataID, group)

		b.mu.Lock()
		delete(b.pending, k)
		b.mu.Unlock()
		close(ch)

		if err != nil {
			return errors.Wrapf(err, "configbridge: cancel %s/%s", group, dataID)
		}
		return nil
	}
}

// hasSubLocked 判断 key 的订阅栈里是否有指定句柄（调用方须持 b.mu）。
func (b *Bridge) hasSubLocked(k string, id Handle) bool {
	for _, s := range b.subs[k] {
		if s.id == id {
			return true
		}
	}
	return false
}

// removeLocked 摘除指定句柄（调用方须持 b.mu 且已用 hasSubLocked 确认存在）。
func (b *Bridge) removeLocked(k string, id Handle) {
	list := b.subs[k]
	for i, s := range list {
		if s.id == id {
			b.subs[k] = append(list[:i], list[i+1:]...)
			return
		}
	}
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
