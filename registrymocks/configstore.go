package registrymocks

import (
	"context"
	"log"
	"sync"

	"github.com/oekit/pkg/errors"
)

// ConfigStore 是 [registry.ConfigStore] 的可配置 mock 实现。
//
// 通过 Configs map 配置 dataID/group 对应的配置内容；
// 通过 Listening map 跟踪已注册的监听器，可通过 FireChange 模拟配置变更推送。
//
// 零值可用（#150）：结构体字面量构造（如 &ConfigStore{}）后各方法可直接
// 调用——map 字段在首次写入时惰性初始化，与 NewConfigStore() 等价。
//
// 并发注入口（#160）：直接写导出字段（Configs/Listening/Err）会绕过 mu 锁
// ——并发场景下是进程级 fatal error（map）或 DATA RACE（标量字段）；并发
// 测试请走锁内注入口 SetConfig/SetErr 与 SPI 方法 ListenConfig/
// StopListenConfig，构造期（未开始并发使用前）的字面量赋值不受限。
type ConfigStore struct {
	mu sync.Mutex

	// dataID/group → 配置内容
	Configs map[string]string
	// 注入的故障错误（#151）：非 nil 时 GetConfig 与 ListenConfig 一律返回
	// 它——模拟「配置中心不可用」是全量故障：读不到配置、监听也挂不上
	// （监听照常挂上会造成故障注入用例假绿：用例以为注册失败已处理，
	// 实际监听已挂上、配置变更照常投递）。nil（默认）时 GetConfig 按
	// Configs 未命中返回空值、ListenConfig 正常登记。
	Err error
	// dataID/group → onChange 回调
	Listening map[string]func(string)

	// deliverMu 串行化 onChange 投递（#158，SPI 契约「回调必须串行投递、
	// 不并发调用 onChange」）：FireChange 在释放 mu 后才调回调，多个
	// goroutine 并发 FireChange 时 onChange 被并发调用——真实现
	// （nacosimpl/etcdx）串行投递，mock 并发投递会让消费方「读-改-写」
	// 共享状态代码测试通过、生产才有竞态。本锁不得与 mu 混用（回调可能
	// 回查 GetConfig/ListenConfig 重入 mu，持 mu 调回调 = 死锁）。
	deliverMu sync.Mutex
}

// NewConfigStore 创建一个空的 ConfigStore。
func NewConfigStore() *ConfigStore {
	return &ConfigStore{
		Configs:   make(map[string]string),
		Listening: make(map[string]func(string)),
	}
}

func keyOf(dataID, group string) string {
	return dataID + "/" + group
}

// GetConfig 返回配置内容。
func (m *ConfigStore) GetConfig(_ context.Context, dataID, group string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return "", m.Err
	}
	return m.Configs[keyOf(dataID, group)], nil
}

// ListenConfig 注册变更监听器。
func (m *ConfigStore) ListenConfig(_ context.Context, dataID, group string, onChange func(string)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 故障注入生效（#151）：SetErr 的文档承诺 ListenConfig 同受影响——
	// 模拟「配置中心不可用」时监听挂不上，故障注入用例不再假绿。
	if m.Err != nil {
		return m.Err
	}
	// 零值可用（#150）：字面量构造后 Listening 为 nil，直接写会 panic
	// （assignment to entry in nil map，报错不指向 registrymocks，难归因）
	if m.Listening == nil {
		m.Listening = make(map[string]func(string))
	}
	m.Listening[keyOf(dataID, group)] = onChange
	return nil
}

// StopListenConfig 注销监听器。
func (m *ConfigStore) StopListenConfig(_ context.Context, dataID, group string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.Listening, keyOf(dataID, group))
	return nil
}

// SetErr 并发安全地更新 GetConfig/ListenConfig 的注入错误（模拟故障注入
// 与恢复）：传 error 即两方法一律返回它，传 nil 即恢复。故障期间监听
// 不登记（ListenConfig 返回错误，Listening 不写入）。
func (m *ConfigStore) SetErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Err = err
}

// SetConfig 并发安全地写入指定 dataID/group 的配置内容（#160 锁内注入口）：
// 直接写 Configs map 会绕过 mu 锁，并发场景下是进程级 fatal error
// （concurrent map read and map write）——整个测试二进制终止而非单测失败。
func (m *ConfigStore) SetConfig(dataID, group, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Configs == nil {
		m.Configs = make(map[string]string)
	}
	m.Configs[keyOf(dataID, group)] = value
}

// ListeningCountFor 返回指定 dataID/group 当前注册的监听器数量（1 或 0）。
// 供测试轮询等待 ListenConfig 完成注册。
func (m *ConfigStore) ListeningCountFor(dataID, group string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.Listening[keyOf(dataID, group)]; ok {
		return 1
	}
	return 0
}

// FireChange 主动触发指定 dataID/group 的 onChange 回调，模拟配置中心推送。
//
// SPI 契约落地（#158，根包 config.go 对所有实现的硬性要求，nacosimpl 同款）：
//   - 串行投递：deliverMu 保证并发 FireChange 不并发调用 onChange（快速连更
//     时旧配置不得后到先生效，与 etcdx/nacosimpl 口径对齐）；
//   - panic 就地隔离：onChange panic 经 errors.Recovered 收编转错误日志——
//     消费方回调质量不可控，坏回调不得沿派发链上抛打死宿主测试进程，
//     同服务的后续推送照常投递。
func (m *ConfigStore) FireChange(dataID, group, newValue string) {
	m.mu.Lock()
	cb, ok := m.Listening[keyOf(dataID, group)]
	m.mu.Unlock()
	if !ok || cb == nil {
		return
	}
	m.deliverMu.Lock()
	defer m.deliverMu.Unlock()
	defer func() {
		if perr := errors.Recovered(recover()); perr != nil {
			log.Printf("[registrymocks] config change callback panicked (isolated): dataID=%s group=%s err=%v stack=%s",
				dataID, group, perr, perr.(*errors.PanicError).Stack)
		}
	}()
	cb(newValue)
}
