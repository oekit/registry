package registrymocks

import (
	"context"
	"sync"
)

// ConfigStore 是 [registry.ConfigStore] 的可配置 mock 实现。
//
// 通过 configs map 配置 dataID/group 对应的配置内容；
// 通过 listening map 跟踪已注册的监听器，可通过 FireChange 模拟配置变更推送。
type ConfigStore struct {
	mu sync.Mutex

	// dataID/group → 配置内容
	Configs map[string]string
	// GetConfig 默认错误（未命中 Configs 时返回）
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

// SetErr 并发安全地更新 GetConfig/ListenConfig 的默认错误（模拟故障注入与恢复）。
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
	cb := m.Listening[keyOf(dataID, group)]
	m.mu.Unlock()
	if cb != nil {
		cb(newValue)
	}
}
