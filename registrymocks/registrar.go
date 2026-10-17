package registrymocks

import (
	"context"
	"sync"

	"github.com/oekit/registry"
)

// Registrar 是 [registry.Registrar] 的可配置 mock 实现。
//
// 用法：
//
//	reg := &registrymocks.Registrar{RegErr: errors.New("register failed")}
//	err := reg.Register(ctx, registry.Instance{...})
//	if err == nil { t.Error("expected error") }
type Registrar struct {
	mu           sync.Mutex
	Registered   []registry.Instance
	Deregistered []registry.Instance
	RegErr       error // Register 返回的错误
	DeregErr     error // Deregister 返回的错误
}

// Register 记录注册实例并返回配置的 RegErr。
func (m *Registrar) Register(_ context.Context, inst registry.Instance) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.RegErr != nil {
		return m.RegErr
	}
	m.Registered = append(m.Registered, inst)
	return nil
}

// Deregister 记录注销实例并返回配置的 DeregErr。
func (m *Registrar) Deregister(_ context.Context, inst registry.Instance) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.DeregErr != nil {
		return m.DeregErr
	}
	m.Deregistered = append(m.Deregistered, inst)
	return nil
}

// Reset 清空记录（保留错误配置）。
func (m *Registrar) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Registered = nil
	m.Deregistered = nil
}
