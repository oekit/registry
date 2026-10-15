package registrymocks

import (
	"context"
	"sync"

	"github.com/oekit/registry/registry"
)

// Discoverer 是 [registry.Discoverer] 的可配置 mock 实现。
//
// 支持两种使用模式：
//  1. 简单模式：所有服务返回相同的 Instances / Err
//  2. Per-service 模式：在 InstancesMap 中按服务名配置不同返回值
type Discoverer struct {
	mu sync.Mutex

	// 默认返回（所有服务都返回这些实例，除非 InstancesMap 覆盖）
	Instances []registry.Instance
	Err       error

	// AllInstances / AllErr 是 DiscoverAll 的返回
	AllInstances []registry.Instance
	AllErr       error

	// Per-service 覆盖：serviceName → instances
	InstancesMap map[string][]registry.Instance
	// Per-service 错误：serviceName → error
	ErrMap map[string]error
}

// Discover 返回配置的实例列表。
func (m *Discoverer) Discover(_ context.Context, serviceName string) ([]registry.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.ErrMap[serviceName]; ok {
		return nil, err
	}
	if insts, ok := m.InstancesMap[serviceName]; ok {
		return insts, nil
	}
	return m.Instances, m.Err
}

// DiscoverAll 返回配置的全部实例列表。
func (m *Discoverer) DiscoverAll(_ context.Context, serviceName string) ([]registry.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.AllInstances != nil || m.AllErr != nil {
		return m.AllInstances, m.AllErr
	}
	return m.Instances, m.Err
}
