package registrymocks

import (
	"context"
	"sync"
)

// ConfigStore 是 [registry.ConfigStore] 的可配置 mock 实现。
//
// 通过 configs map 配置 dataID/group 对应的配置内容；
// 通过 listening map 跟踪已注册的监听器，可通过 FireChange 模拟配置变更推送。
//
// 零值可用（#150）：结构体字面量构造（如 &ConfigStore{}）后各方法可直接
// 调用——map 字段在首次写入时惰性初始化，与 NewConfigStore() 等价。
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
func (m *ConfigStore) FireChange(dataID, group, newValue string) {
	m.mu.Lock()
	cb, ok := m.Listening[keyOf(dataID, group)]
	m.mu.Unlock()
	if ok && cb != nil {
		cb(newValue)
	}
}
