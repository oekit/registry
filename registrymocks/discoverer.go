package registrymocks

import (
	"context"
	"sync"

	"github.com/oekit/registry"
)

// Discoverer 是 [registry.Discoverer] 的可配置 mock 实现。
//
// 支持两种使用模式：
//  1. 简单模式：所有服务返回相同的 Instances / Err
//  2. Per-service 模式：在 InstancesMap 中按服务名配置不同返回值
//
// 并发注入口（#160）：直接写导出字段（Instances/Err/AllInstances/AllErr/
// InstancesMap/ErrMap）会绕过 mu 锁——并发场景下是进程级 fatal error（map）
// 或 DATA RACE（标量字段）；并发测试请走锁内注入口 SetInstances/SetErrMap/
// SetAll/SetAllErr，构造期（未开始并发使用前）的字面量赋值不受限。
type Discoverer struct {
	mu sync.Mutex

	// 默认返回（所有服务都返回这些实例，除非 InstancesMap 覆盖）。
	// Err 非 nil 时 Discover/DiscoverAll 的缺省回落返回 (nil, Err)——
	// 与 ErrMap 路径同形状（#198）：此前返回 (Instances, Err)，两者
	// 同时非 nil，按 err 短路则 Instances 是死数据、按 len 断言则 Err
	// 是死数据，断言随注入方式漂移。
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
//
// 错误形状（#198）：任何错误注入路径都返回 (nil, err)——ErrMap 与全局 Err
// 同形状，消费方按 `if err != nil` 短路时实例面恒为 nil，不再出现
// 「Instances 与 Err 同时非 nil」的分裂。
func (m *Discoverer) Discover(_ context.Context, serviceName string) ([]registry.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.ErrMap[serviceName]; ok {
		return nil, err
	}
	if insts, ok := m.InstancesMap[serviceName]; ok {
		return insts, nil
	}
	if m.Err != nil {
		return nil, m.Err
	}
	return m.Instances, nil
}

// DiscoverAll 返回配置的全部实例列表（含不健康实例——根包 SPI 口径）。
//
// #159：按服务名取值，与 Discover 同口径——先查 per-service 覆盖
// （ErrMap/InstancesMap），未命中回落 DiscoverAll 专用覆盖（AllInstances/
// AllErr），最后回落全局默认（Instances/Err）。原实现完全忽略 serviceName
// 形参，per-service 模式在 DiscoverAll 上整条失效：Discover 返回服务专属
// 实例集、DiscoverAll 却返回「所有人的默认实例」，ErrMap 故障注入被静默
// 吞掉（全量巡检看不到「该服务挂了」），消费方测试通过、生产语义分裂。
func (m *Discoverer) DiscoverAll(_ context.Context, serviceName string) ([]registry.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.ErrMap[serviceName]; ok {
		return nil, err
	}
	if insts, ok := m.InstancesMap[serviceName]; ok {
		return insts, nil
	}
	if m.AllInstances != nil || m.AllErr != nil {
		return m.AllInstances, m.AllErr
	}
	// #198：缺省回落与 Discover 同形状（nil 实例 + Err）。
	if m.Err != nil {
		return nil, m.Err
	}
	return m.Instances, nil
}

// SetInstances 并发安全地写入指定服务的 per-service 实例覆盖（#160 锁内
// 注入口）：直接写 InstancesMap map 会绕过 mu 锁，并发场景下是进程级
// fatal error——整个测试二进制终止而非单测失败。传 nil 表示该服务回退
// 默认返回。
func (m *Discoverer) SetInstances(serviceName string, instances []registry.Instance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if instances == nil {
		// nil = 清除覆盖（Discover 的命中判定是 map 存在性，存 nil 值仍命中
		// 空列表——语义必须是回退默认返回）。
		delete(m.InstancesMap, serviceName)
		return
	}
	if m.InstancesMap == nil {
		m.InstancesMap = make(map[string][]registry.Instance)
	}
	m.InstancesMap[serviceName] = instances
}

// SetErrMap 并发安全地写入指定服务的 per-service 故障注入（#160 锁内
// 注入口）：直接写 ErrMap map 会绕过 mu 锁，并发场景下是进程级 fatal
// error。传 nil 清除该服务的故障注入。
func (m *Discoverer) SetErrMap(serviceName string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		// nil = 清除故障注入（Discover/DiscoverAll 的命中判定是 map 存在性）。
		delete(m.ErrMap, serviceName)
		return
	}
	if m.ErrMap == nil {
		m.ErrMap = make(map[string]error)
	}
	m.ErrMap[serviceName] = err
}

// SetAll 并发安全地更新 DiscoverAll 的全局专用返回（#160 锁内注入口）。
func (m *Discoverer) SetAll(instances []registry.Instance, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.AllInstances = instances
	m.AllErr = err
}

// SetDefault 并发安全地更新全局默认返回（Discover/DiscoverAll 的最后回落，
// #160 锁内注入口）。
func (m *Discoverer) SetDefault(instances []registry.Instance, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Instances = instances
	m.Err = err
}
