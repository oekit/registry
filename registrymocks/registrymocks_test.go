package registrymocks

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/registry"
)

func TestRegistrar_BasicFlow(t *testing.T) {
	r := &Registrar{}
	ctx := context.Background()
	inst := registry.Instance{Service: "svc", IP: "1.2.3.4", Port: 8080}
	if err := r.Register(ctx, inst); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := r.Deregister(ctx, inst); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if len(r.Registered) != 1 || len(r.Deregistered) != 1 {
		t.Errorf("expected 1 registered + 1 deregistered, got %d/%d", len(r.Registered), len(r.Deregistered))
	}
}

func TestRegistrar_ErrorInjection(t *testing.T) {
	r := &Registrar{RegErr: errors.New("boom")}
	if err := r.Register(context.Background(), registry.Instance{}); err == nil {
		t.Error("expected Register error")
	}
	if len(r.Registered) != 0 {
		t.Error("expected nothing recorded when RegErr set")
	}
}

func TestDiscoverer_PerService(t *testing.T) {
	d := &Discoverer{
		InstancesMap: map[string][]registry.Instance{
			"svc-a": {{IP: "1.1.1.1"}},
			"svc-b": {{IP: "2.2.2.2"}},
		},
	}
	a, _ := d.Discover(context.Background(), "svc-a")
	if len(a) != 1 || a[0].IP != "1.1.1.1" {
		t.Errorf("svc-a = %+v", a)
	}
	b, _ := d.Discover(context.Background(), "svc-b")
	if len(b) != 1 || b[0].IP != "2.2.2.2" {
		t.Errorf("svc-b = %+v", b)
	}
}

func TestSubscriber_PushCallback(t *testing.T) {
	sub := NewSubscriber()
	called := 0
	err := sub.Subscribe(context.Background(), "svc", func([]registry.Instance) {
		called++
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sub.Push("svc", []registry.Instance{{IP: "1.2.3.4"}})
	sub.Push("svc", []registry.Instance{{IP: "2.3.4.5"}})
	if called != 2 {
		t.Errorf("expected 2 callback invocations, got %d", called)
	}
}

func TestConfigStore_FireChange(t *testing.T) {
	cs := NewConfigStore()
	received := ""
	err := cs.ListenConfig(context.Background(), "dataID", "GROUP", func(s string) {
		received = s
	})
	if err != nil {
		t.Fatalf("ListenConfig: %v", err)
	}
	cs.FireChange("dataID", "GROUP", "new-value")
	if received != "new-value" {
		t.Errorf("received = %q, want %q", received, "new-value")
	}
}

// #150 回归：ConfigStore/Subscriber 零值（结构体字面量构造）必须可用——
// 此前字面量构造后在 ListenConfig/Subscribe 写 nil map 直接 panic
// （assignment to entry in nil map，报错不指向 registrymocks，崩的是整个
// 测试二进制），与包文档「不留 panic 默认」的承诺冲突。
func TestMocksZeroValueUsable(t *testing.T) {
	ctx := context.Background()

	// 与包内 Registrar 文档给出的字面量用法示例同形态
	cs := &ConfigStore{}
	got := ""
	if err := cs.ListenConfig(ctx, "d", "g", func(s string) { got = s }); err != nil {
		t.Fatalf("zero-value ListenConfig: %v", err)
	}
	if err := cs.StopListenConfig(ctx, "d", "g"); err != nil {
		t.Fatalf("zero-value StopListenConfig: %v", err)
	}
	if cs.ListeningCountFor("d", "g") != 0 {
		t.Error("expected 0 listener after stop")
	}
	// 重复 ListenConfig 后 FireChange 正常投递（惰性初始化的 map 可用）
	if err := cs.ListenConfig(ctx, "d", "g", func(s string) { got = s }); err != nil {
		t.Fatalf("second ListenConfig: %v", err)
	}
	cs.FireChange("d", "g", "v")
	if got != "v" {
		t.Errorf("FireChange delivered %q, want %q", got, "v")
	}

	sub := &Subscriber{}
	if err := sub.Subscribe(ctx, "svc", func([]registry.Instance) {}); err != nil {
		t.Fatalf("zero-value Subscribe: %v", err)
	}
	if err := sub.Unsubscribe(ctx, "svc"); err != nil {
		t.Fatalf("zero-value Unsubscribe: %v", err)
	}
	// Push/CallbackCount 对 nil map 只读，本就安全——一并钉住
	sub.Push("svc", nil)
	if sub.CallbackCount("svc") != 0 {
		t.Error("expected 0 callbacks after unsubscribe")
	}
}

// #151 回归：SetErr 必须同时作用于 GetConfig 与 ListenConfig——此前实现
// 只作用于 GetConfig，用 SetErr 模拟「配置中心不可用」的故障注入用例拿到
// 假绿：监听注册照样成功返回 nil，实际监听已挂上、配置变更照常投递。
func TestConfigStoreSetErrAffectsListenConfig(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("config center down")
	cs := NewConfigStore()
	cs.SetErr(boom)

	if _, err := cs.GetConfig(ctx, "d", "g"); !errors.Is(err, boom) {
		t.Fatalf("GetConfig err = %v; want config center down", err)
	}
	if err := cs.ListenConfig(ctx, "d", "g", func(string) {}); !errors.Is(err, boom) {
		t.Fatalf("ListenConfig err = %v; want config center down（#151：SetErr 同时作用于 ListenConfig）", err)
	}
	// 故障期间监听不得登记（假绿根源：用例以为注册失败，实际监听挂上了）
	if cs.ListeningCountFor("d", "g") != 0 {
		t.Error("listener registered during injected fault; want none（#151）")
	}

	// SetErr(nil) 恢复：GetConfig 正常返回、ListenConfig 正常登记
	cs.SetErr(nil)
	if err := cs.ListenConfig(ctx, "d", "g", func(string) {}); err != nil {
		t.Fatalf("ListenConfig after recovery: %v", err)
	}
	if cs.ListeningCountFor("d", "g") != 1 {
		t.Error("expected 1 listener after recovery")
	}
}

// —— #158/#159/#160 契约回归钉 ——

// TestConfigStore_FireChange_PanicIsolated 回归钉（#158，SPI 契约）：onChange
// panic 必须就地收编——不逃逸到 FireChange 调用方（无 recover 的真实测试
// 二进制里会终止整个测试进程），且后续推送照常投递。
func TestConfigStore_FireChange_PanicIsolated(t *testing.T) {
	m := NewConfigStore()
	var delivered []string
	if err := m.ListenConfig(context.Background(), "app", "DEFAULT_GROUP", func(v string) {
		delivered = append(delivered, v)
		if v == "boom" {
			panic("boom in onChange")
		}
	}); err != nil {
		t.Fatalf("ListenConfig: %v", err)
	}

	m.FireChange("app", "DEFAULT_GROUP", "boom") // panic 不得逃逸
	m.FireChange("app", "DEFAULT_GROUP", "next")
	if len(delivered) != 2 || delivered[1] != "next" {
		t.Fatalf("★ 坏回调后同服务的后续推送必须照常投递: got %v", delivered)
	}
}

// TestConfigStore_FireChange_SerialDelivery 回归钉（#158，SPI 契约）：并发
// FireChange 的 onChange 并发度必须恒为 1（回调必须串行投递、不并发调用
// onChange——真实现 nacosimpl/etcdx 串行，mock 并发会让「读-改-写」共享
// 状态代码测试通过、生产才有竞态）。
func TestConfigStore_FireChange_SerialDelivery(t *testing.T) {
	m := NewConfigStore()
	var inFlight atomic.Int32
	var maxConcurrent atomic.Int32
	var wg sync.WaitGroup
	if err := m.ListenConfig(context.Background(), "app", "DEFAULT_GROUP", func(string) {
		n := inFlight.Add(1)
		for {
			old := maxConcurrent.Load()
			if n <= old || maxConcurrent.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		inFlight.Add(-1)
	}); err != nil {
		t.Fatalf("ListenConfig: %v", err)
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.FireChange("app", "DEFAULT_GROUP", "v")
		}()
	}
	wg.Wait()
	if got := maxConcurrent.Load(); got != 1 {
		t.Fatalf("★ 并发 FireChange 的 onChange 最大并发数=%d, want 1（SPI 契约串行投递，#158）", got)
	}
}

// TestSubscriber_Push_PanicIsolatedAndSerial 回归钉（#158，SPI 契约）：一个
// 坏回调不得让同服务的其他回调收不到事件（原实现 panic 逃逸且后续回调
// 静默丢失）；并发 Push 的回调并发度恒为 1。
func TestSubscriber_Push_PanicIsolatedAndSerial(t *testing.T) {
	m := NewSubscriber()
	var goodGot atomic.Bool
	if err := m.Subscribe(context.Background(), "svc", func([]registry.Instance) {
		panic("boom in cb")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := m.Subscribe(context.Background(), "svc", func([]registry.Instance) {
		goodGot.Store(true)
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	m.Push("svc", []registry.Instance{{IP: "1.1.1.1"}}) // 坏回调 panic 不得逃逸
	if !goodGot.Load() {
		t.Fatal("★ 坏回调之后同服务的第二个回调必须照常投递（#158 EXP-C 面）")
	}

	// 并发 Push 串行投递。
	var inFlight, maxConcurrent atomic.Int32
	if err := m.Subscribe(context.Background(), "svc2", func([]registry.Instance) {
		n := inFlight.Add(1)
		for {
			old := maxConcurrent.Load()
			if n <= old || maxConcurrent.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		inFlight.Add(-1)
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Push("svc2", nil)
		}()
	}
	wg.Wait()
	if got := maxConcurrent.Load(); got != 1 {
		t.Fatalf("★ 并发 Push 的 cb 最大并发数=%d, want 1（SPI 契约串行投递，#158）", got)
	}
}

// TestDiscoverer_DiscoverAll_PerService 回归钉（#159）：per-service 配置对
// Discover 与 DiscoverAll 同时生效——同一服务两个方法给出一致视图，ErrMap
// 故障注入对全量巡检同样有效（不再被静默吞掉）。
func TestDiscoverer_DiscoverAll_PerService(t *testing.T) {
	d := &Discoverer{
		Instances:    []registry.Instance{{IP: "9.9.9.9"}},
		AllInstances: []registry.Instance{{IP: "8.8.8.8"}},
		InstancesMap: map[string][]registry.Instance{
			"svc-a": {{IP: "1.1.1.1"}},
		},
		ErrMap: map[string]error{
			"svc-err": errors.New("boom"),
		},
	}
	ctx := context.Background()

	// svc-a：per-service 覆盖对两个方法同时生效（原先 DiscoverAll 返回默认）。
	for _, insts := range [][]registry.Instance{
		func() []registry.Instance { i, _ := d.Discover(ctx, "svc-a"); return i }(),
		func() []registry.Instance { i, _ := d.DiscoverAll(ctx, "svc-a"); return i }(),
	} {
		if len(insts) != 1 || insts[0].IP != "1.1.1.1" {
			t.Fatalf("★ per-service 覆盖必须对 Discover/DiscoverAll 同时生效: got %v (#159)", insts)
		}
	}

	// svc-err：故障注入对 DiscoverAll 同样有效（原先返回健康实例 + nil）。
	if _, err := d.Discover(ctx, "svc-err"); err == nil {
		t.Fatal("Discover(svc-err) should fail")
	}
	if _, err := d.DiscoverAll(ctx, "svc-err"); err == nil {
		t.Fatal("★ ErrMap 故障注入不得被 DiscoverAll 静默吞掉(#159)")
	}

	// 未命中 per-service：回落 DiscoverAll 专用覆盖（AllInstances）。
	if insts, _ := d.DiscoverAll(ctx, "svc-b"); len(insts) != 1 || insts[0].IP != "8.8.8.8" {
		t.Fatalf("★ AllInstances 专用覆盖失效: got %v", insts)
	}
	// 未命中 per-service：Discover 回落全局默认（不受 AllInstances 影响）。
	if insts, _ := d.Discover(ctx, "svc-b"); len(insts) != 1 || insts[0].IP != "9.9.9.9" {
		t.Fatalf("★ Discover 全局默认回落失效: got %v", insts)
	}
}

// TestDiscoverer_DiscoverAll_AllErrOnly 专用覆盖的单边形态（#159 语义补充）：
// 只配 AllErr（AllInstances 为 nil）时 DiscoverAll 全量视图注入故障，Discover
// 不受牵连。
func TestDiscoverer_DiscoverAll_AllErrOnly(t *testing.T) {
	d := &Discoverer{
		Instances: []registry.Instance{{IP: "9.9.9.9"}},
		AllErr:    errors.New("all down"),
	}
	if _, err := d.DiscoverAll(context.Background(), "any"); err == nil {
		t.Fatal("★ AllErr 单边覆盖必须对 DiscoverAll 生效(#159)")
	}
	insts, err := d.Discover(context.Background(), "any")
	if err != nil || len(insts) != 1 {
		t.Fatalf("★ Discover 不受 AllErr 牵连: got (%v, %v)", insts, err)
	}
}

// TestMocks_SettersConcurrentNoRace 回归钉（#160，-race）：并发走锁内注入口
// + 并发方法读——无 DATA RACE、无 fatal error；SetInstances 传 nil 即回退
// 默认返回。
func TestMocks_SettersConcurrentNoRace(t *testing.T) {
	d := &Discoverer{Instances: []registry.Instance{{IP: "9.9.9.9"}}}
	cs := &ConfigStore{}
	r := &Registrar{RegErr: errors.New("reg boom")}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				d.SetInstances("svc", []registry.Instance{{IP: "1.1.1.1"}})
				d.SetErrMap("svc", nil)
				d.SetErrMap("svc", errors.New("boom"))
				d.SetAll([]registry.Instance{{IP: "8.8.8.8"}}, nil)
				d.SetDefault([]registry.Instance{{IP: "7.7.7.7"}}, nil)
				cs.SetConfig("app", "DEFAULT_GROUP", "v")
				cs.SetErr(nil)
				cs.SetErr(errors.New("cfg boom"))
				r.SetRegErr(nil)
				r.SetRegErr(errors.New("reg boom"))
				r.SetDeregErr(errors.New("dereg boom"))
				_, _ = d.Discover(context.Background(), "svc")
				_, _ = d.DiscoverAll(context.Background(), "svc")
				_, _ = cs.GetConfig(context.Background(), "app", "DEFAULT_GROUP")
				_ = r.Register(context.Background(), registry.Instance{})
			}
		}()
	}
	wg.Wait()

	// 行为面：SetInstances(nil) 回退默认；SetErrMap(nil) 清除故障注入。
	// 并发循环的最后一次写入不定序，先复位再断言。
	d.SetInstances("svc", nil)
	d.SetErrMap("svc", nil)
	d.SetDefault([]registry.Instance{{IP: "7.7.7.7"}}, nil)
	d.SetAll(nil, nil)
	cs.SetErr(nil)
	cs.SetConfig("app", "DEFAULT_GROUP", "v1")
	r.SetRegErr(nil)
	r.SetDeregErr(nil)
	if insts, err := d.Discover(context.Background(), "svc"); err != nil || len(insts) != 1 || insts[0].IP != "7.7.7.7" {
		t.Fatalf("★ SetInstances(nil) 必须回退默认: got (%v, %v)", insts, err)
	}
	if v, err := cs.GetConfig(context.Background(), "app", "DEFAULT_GROUP"); err != nil || v != "v1" {
		t.Fatalf("★ SetConfig 必须生效: got (%q, %v)", v, err)
	}
	if err := r.Register(context.Background(), registry.Instance{}); err != nil {
		t.Fatalf("★ SetRegErr(nil) 必须恢复注册: %v", err)
	}
}
