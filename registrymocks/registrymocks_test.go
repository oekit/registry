package registrymocks

import (
	"context"
	"errors"
	"testing"

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
