package configbridge

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeBackend 记录 hooks 调用并可手动推送。
type fakeBackend struct {
	mu        sync.Mutex
	listenN   int
	cancelN   int
	listenErr error
	lastRelay ChangeFunc
}

func (f *fakeBackend) hooks() Hooks {
	return Hooks{
		Listen: func(dataID, group string, onChange ChangeFunc) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.listenN++
			if f.listenErr != nil {
				return f.listenErr
			}
			f.lastRelay = onChange
			return nil
		},
		Cancel: func(dataID, group string) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.cancelN++
			return nil
		},
	}
}

func (f *fakeBackend) push(data string) {
	f.mu.Lock()
	relay := f.lastRelay
	f.mu.Unlock()
	if relay != nil {
		relay("ns", "g", "d", data)
	}
}

func (f *fakeBackend) counts() (listen, cancel int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listenN, f.cancelN
}

// TestMulticastSameKey 多播核心：同 key 多订阅者全部收到，底层只注册一次。
func TestMulticastSameKey(t *testing.T) {
	fk := &fakeBackend{}
	b := New(fk.hooks())

	var gotA, gotB []string
	h1, err := b.Subscribe("d1", "g", func(_, _, _, s string) { gotA = append(gotA, s) })
	if err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	h2, err := b.Subscribe("d1", "g", func(_, _, _, s string) { gotB = append(gotB, s) })
	if err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}
	if listen, _ := fk.counts(); listen != 1 {
		t.Fatalf("底层应只注册一次, got %d", listen)
	}

	fk.push("v1")
	if len(gotA) != 1 || gotA[0] != "v1" || len(gotB) != 1 || gotB[0] != "v1" {
		t.Fatalf("两订阅者都应收到 v1: A=%v B=%v", gotA, gotB)
	}

	// 摘 A 不影响 B（句柄独立）。
	if err := b.Unsubscribe("d1", "g", h1); err != nil {
		t.Fatalf("Unsubscribe A: %v", err)
	}
	fk.push("v2")
	if len(gotA) != 1 || len(gotB) != 2 || gotB[1] != "v2" {
		t.Fatalf("摘 A 后仅 B 收到 v2: A=%v B=%v", gotA, gotB)
	}
	if listen, cancel := fk.counts(); listen != 1 || cancel != 0 {
		t.Fatalf("尚有订阅者：不应 Cancel: listen=%d cancel=%d", listen, cancel)
	}

	// 最后一个离开才 Cancel。
	if err := b.Unsubscribe("d1", "g", h2); err != nil {
		t.Fatalf("Unsubscribe B: %v", err)
	}
	if _, cancel := fk.counts(); cancel != 1 {
		t.Fatalf("最后一个订阅者离开应 Cancel, got %d", cancel)
	}
}

// TestUnsubscribeIdempotent 幂等：重复摘、摘不存在的句柄都不报错。
func TestUnsubscribeIdempotent(t *testing.T) {
	fk := &fakeBackend{}
	b := New(fk.hooks())
	h, _ := b.Subscribe("d", "g", func(_, _, _, _ string) {})
	if err := b.Unsubscribe("d", "g", h); err != nil {
		t.Fatalf("首次摘: %v", err)
	}
	if err := b.Unsubscribe("d", "g", h); err != nil {
		t.Fatalf("重复摘应幂等 nil, got %v", err)
	}
	if err := b.Unsubscribe("d", "g", 999); err != nil {
		t.Fatalf("摘不存在句柄应幂等 nil, got %v", err)
	}
	if _, cancel := fk.counts(); cancel != 1 {
		t.Fatalf("Cancel 只应一次, got %d", cancel)
	}
}

// TestListenFailureRollback 注册失败：回滚表项，后续 Subscribe 可重试。
func TestListenFailureRollback(t *testing.T) {
	fk := &fakeBackend{listenErr: errors.New("boom")}
	b := New(fk.hooks())

	if _, err := b.Subscribe("d", "g", func(_, _, _, _ string) {}); err == nil {
		t.Fatal("Subscribe 应透传 Listen 错误")
	}
	// 回滚后重试（清掉错误）应成功且重新走 Listen。
	fk.mu.Lock()
	fk.listenErr = nil
	fk.mu.Unlock()
	h, err := b.Subscribe("d", "g", func(_, _, _, _ string) {})
	if err != nil {
		t.Fatalf("回滚后重试应成功: %v", err)
	}
	if listen, _ := fk.counts(); listen != 2 {
		t.Fatalf("回滚后应重新注册, listen=%d", listen)
	}
	b.Unsubscribe("d", "g", h)
}

// TestPanicIsolation 单订阅者 panic 不影响同 key 其他订阅者，且经 OnPanic 上报。
func TestPanicIsolation(t *testing.T) {
	var reported atomic.Value
	fk := &fakeBackend{}
	h := fk.hooks()
	h.OnPanic = func(dataID, group string, r any, stack string) {
		reported.Store(r.(string))
	}
	b := New(h)

	var got []string
	_, _ = b.Subscribe("d", "g", func(_, _, _, _ string) { panic("poison") })
	_, _ = b.Subscribe("d", "g", func(_, _, _, s string) { got = append(got, s) })

	fk.push("v1") // 不得 panic 逃逸
	if len(got) != 1 {
		t.Fatalf("健康订阅者应收到且不被毒订阅者影响, got %v", got)
	}
	if rep, _ := reported.Load().(string); rep != "poison" {
		t.Fatalf("OnPanic 应上报 poison, got %v", reported.Load())
	}
}

// TestSerialDispatch 派发串行：并发推送不并发进入订阅者回调。
func TestSerialDispatch(t *testing.T) {
	fk := &fakeBackend{}
	b := New(fk.hooks())

	var inCRITICAL atomic.Int32
	var maxSeen atomic.Int32
	_, _ = b.Subscribe("d", "g", func(_, _, _, _ string) {
		n := inCRITICAL.Add(1)
		if n > maxSeen.Load() {
			maxSeen.Store(n)
		}
		time.Sleep(2 * time.Millisecond)
		inCRITICAL.Add(-1)
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fk.push("x")
		}()
	}
	wg.Wait()
	if maxSeen.Load() > 1 {
		t.Fatalf("同桥回调必须串行, 并发度峰值=%d", maxSeen.Load())
	}
}

// TestForSharedByClient 同 client 身份共享同一桥（跨封装闭合点）。
func TestForSharedByClient(t *testing.T) {
	type fakeClient struct{ name string }
	c := &fakeClient{"shared"}
	defer ResetFor(c)

	fk := &fakeBackend{}
	b1 := For(c, fk.hooks())
	b2 := For(c, fk.hooks())
	if b1 != b2 {
		t.Fatal("同一 client 应拿到同一桥")
	}

	var got []string
	_, _ = b1.Subscribe("d", "g", func(_, _, _, s string) { got = append(got, s) })
	// 第二个「封装」经 b2 订阅：不应触发第二次底层注册。
	_, _ = b2.Subscribe("d", "g", func(_, _, _, _ string) {})
	if listen, _ := fk.counts(); listen != 1 {
		t.Fatalf("跨封装同 key 应共享底层注册, listen=%d", listen)
	}
	fk.push("v")
	if len(got) != 1 {
		t.Fatalf("b1 订阅者应收到推送, got %v", got)
	}
}
