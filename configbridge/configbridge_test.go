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
	cancelErr error
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
			return f.cancelErr
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

// TestSlowListenDoesNotBlockOtherKeys 回归（registry#250）：hooks.Listen
// 可能含网络首拉（SDK ListenConfig），此前 Subscribe 全程持 regMu——一个
// key 的首拉挂住会串行阻塞该 client 下**所有 key** 的 Subscribe/
// Unsubscribe。修复后只在 b.mu 内占位、网络调用锁外执行，其他 key 不受
// 影响（同 key 仍由 pending 串行化）。
//
// 变异性负对照：恢复「regMu 覆盖 Listen」后 fast 订阅在 2s 内无法返回。
func TestSlowListenDoesNotBlockOtherKeys(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	b := New(Hooks{
		Listen: func(dataID, _ string, _ ChangeFunc) error {
			if dataID == "slow" {
				once.Do(func() { close(entered) })
				<-release // 模拟网络首拉挂住
			}
			return nil
		},
		Cancel: func(_, _ string) error { return nil },
	})

	slowDone := make(chan error, 1)
	go func() {
		_, err := b.Subscribe("slow", "g", func(_, _, _, _ string) {})
		slowDone <- err
	}()
	<-entered // slow 的 Listen 已在途

	fastDone := make(chan error, 1)
	go func() {
		_, err := b.Subscribe("fast", "g", func(_, _, _, _ string) {})
		fastDone <- err
	}()
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatalf("fast Subscribe: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("#250：另一 key 的 Subscribe 被在途 Listen 阻塞（regMu 覆盖网络调用）")
	}

	close(release)
	if err := <-slowDone; err != nil {
		t.Fatalf("slow Subscribe: %v", err)
	}
}

// TestSlowCancelDoesNotBlockOtherKeys 同 #250 的 Cancel 面：最后一个订阅者
// 离开时的 hooks.Cancel（SDK CancelListenConfig）同样可能阻塞，不得拖住
// 其他 key 的注册；同 key 的重新订阅则必须等 Cancel 完成后再注册，
// 避免「Cancel 在途时抢先 Listen」被随后的 Cancel 一并取消。
func TestSlowCancelDoesNotBlockOtherKeys(t *testing.T) {
	cancelEntered := make(chan struct{})
	cancelRelease := make(chan struct{})
	var once sync.Once
	slowListenN := atomic.Int32{}
	b := New(Hooks{
		Listen: func(dataID, _ string, _ ChangeFunc) error {
			if dataID == "slow" {
				slowListenN.Add(1)
			}
			return nil
		},
		Cancel: func(dataID, _ string) error {
			if dataID == "slow" {
				once.Do(func() { close(cancelEntered) })
				<-cancelRelease
			}
			return nil
		},
	})

	h, err := b.Subscribe("slow", "g", func(_, _, _, _ string) {})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancelDone := make(chan error, 1)
	go func() { cancelDone <- b.Unsubscribe("slow", "g", h) }()
	<-cancelEntered // slow 的 Cancel 已在途

	// 其他 key 不得被 Cancel 阻塞
	fastDone := make(chan error, 1)
	go func() {
		_, err := b.Subscribe("fast", "g", func(_, _, _, _ string) {})
		fastDone <- err
	}()
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatalf("fast Subscribe: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("#250：另一 key 的 Subscribe 被在途 Cancel 阻塞")
	}

	// 同 key 重新订阅必须等 Cancel 完成（pending 串行化）
	sameKeyDone := make(chan error, 1)
	go func() {
		_, err := b.Subscribe("slow", "g", func(_, _, _, _ string) {})
		sameKeyDone <- err
	}()
	select {
	case err := <-sameKeyDone:
		t.Fatalf("同 key 订阅在 Cancel 在途时提前返回（err=%v）：可能与 Cancel 交错", err)
	case <-time.After(100 * time.Millisecond):
		// 预期：等待 Cancel
	}

	close(cancelRelease)
	if err := <-cancelDone; err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if err := <-sameKeyDone; err != nil {
		t.Fatalf("同 key 重订阅: %v", err)
	}
	if got := slowListenN.Load(); got != 2 {
		t.Fatalf("同 key 重订阅应重新 Listen 一次（slow 总 2 次）, got %d", got)
	}
}

// TestConcurrentSubscribeSameKeySingleListen 并发首订阅同 key：只有一次
// Listen，全部订阅者拿到句柄并收到后续推送（#250 的占位/等待逻辑）。
func TestConcurrentSubscribeSameKeySingleListen(t *testing.T) {
	listenEntered := make(chan struct{})
	listenRelease := make(chan struct{})
	relayCh := make(chan ChangeFunc, 1)
	var listenN atomic.Int32
	b := New(Hooks{
		Listen: func(_ string, _ string, onChange ChangeFunc) error {
			if listenN.Add(1) == 1 {
				close(listenEntered)
				<-listenRelease // 拉长注册窗口，逼出并发等待路径
			}
			relayCh <- onChange
			return nil
		},
		Cancel: func(_, _ string) error { return nil },
	})

	const n = 4
	var received atomic.Int32
	var wg sync.WaitGroup
	handles := make([]Handle, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			handles[i], errs[i] = b.Subscribe("d", "g", func(_, _, _, _ string) {
				received.Add(1)
			})
		}(i)
	}
	<-listenEntered
	close(listenRelease)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Subscribe[%d]: %v", i, err)
		}
	}
	if got := listenN.Load(); got != 1 {
		t.Fatalf("并发首订阅应只 Listen 一次, got %d", got)
	}
	(<-relayCh)("ns", "g", "d", "v")
	if got := received.Load(); got != n {
		t.Fatalf("全部 %d 个订阅者都应收到推送, got %d", n, got)
	}
	for _, h := range handles {
		if err := b.Unsubscribe("d", "g", h); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
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

// TestUnsubscribeCancelFailureRollsBack 是 #251 的回归钉：Unsubscribe 先摘
// 句柄再锁外 Cancel，Cancel 失败时不回滚——句柄从表里永久消失而底层监听
// 仍活着，订阅成为谁也无法再摘的无主泄漏。修复后失败必须恢复表项：
// 推送仍投递、bridged 保持 true（新订阅不重复 Listen）、同句柄可重试成功。
func TestUnsubscribeCancelFailureRollsBack(t *testing.T) {
	fk := &fakeBackend{}
	b := New(fk.hooks())

	var got []string
	h, err := b.Subscribe("d", "g", func(_, _, _, s string) { got = append(got, s) })
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if listen, _ := fk.counts(); listen != 1 {
		t.Fatalf("首次订阅应 Listen 一次, got %d", listen)
	}

	// Cancel 失败：必须返回错误，且本地表项回滚（订阅仍有效）。
	fk.mu.Lock()
	fk.cancelErr = errors.New("cancel boom")
	fk.mu.Unlock()
	if uerr := b.Unsubscribe("d", "g", h); uerr == nil {
		t.Fatal("Cancel 失败必须上抛错误")
	}
	fk.push("v1")
	if len(got) != 1 || got[0] != "v1" {
		t.Fatalf("#251: Cancel 失败后订阅应回滚并继续投递, got=%v", got)
	}

	// bridged 已恢复：同 key 新订阅只追加本地表项，不得重复 Listen。
	h2, err := b.Subscribe("d", "g", func(_, _, _, _ string) {})
	if err != nil {
		t.Fatalf("回滚后 Subscribe: %v", err)
	}
	if listen, _ := fk.counts(); listen != 1 {
		t.Fatalf("#251: 回滚后 bridged 应保持 true（不得重复 Listen）, got %d", listen)
	}

	// 同句柄重试（Cancel 恢复）：h 不是最后一个（h2 在），不触 Cancel。
	fk.mu.Lock()
	fk.cancelErr = nil
	fk.mu.Unlock()
	if uerr := b.Unsubscribe("d", "g", h); uerr != nil {
		t.Fatalf("重试 Unsubscribe(h): %v", uerr)
	}
	if _, cancel := fk.counts(); cancel != 1 {
		t.Fatalf("尚有订阅者时不应再次 Cancel, got %d", cancel)
	}

	// 最后一个订阅者离开：Cancel 成功，表项清空、不再投递。
	if uerr := b.Unsubscribe("d", "g", h2); uerr != nil {
		t.Fatalf("Unsubscribe(h2): %v", uerr)
	}
	if _, cancel := fk.counts(); cancel != 2 {
		t.Fatalf("最后订阅者离开应 Cancel（累计 2 次：失败尝试 + 成功）, got %d", cancel)
	}
	fk.push("v2")
	if len(got) != 1 {
		t.Fatalf("取消成功后不应再投递, got=%v", got)
	}
}

// TestUnsubscribeCancelFailureDirectRetry 覆盖「唯一订阅者」路径：Cancel
// 失败回滚后，同句柄重试必须能重新走 Cancel 并成功收尾（#251）。
func TestUnsubscribeCancelFailureDirectRetry(t *testing.T) {
	fk := &fakeBackend{}
	b := New(fk.hooks())
	h, err := b.Subscribe("d", "g", func(_, _, _, _ string) {})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	fk.mu.Lock()
	fk.cancelErr = errors.New("cancel boom")
	fk.mu.Unlock()
	if uerr := b.Unsubscribe("d", "g", h); uerr == nil {
		t.Fatal("Cancel 失败必须上抛错误")
	}

	fk.mu.Lock()
	fk.cancelErr = nil
	fk.mu.Unlock()
	if uerr := b.Unsubscribe("d", "g", h); uerr != nil {
		t.Fatalf("Cancel 恢复后重试应成功: %v", uerr)
	}
	if _, cancel := fk.counts(); cancel != 2 {
		t.Fatalf("重试应再次调用 Cancel（累计 2 次）, got %d", cancel)
	}
	// 收尾后句柄幂等且不再触发 Cancel。
	if uerr := b.Unsubscribe("d", "g", h); uerr != nil {
		t.Fatalf("重复摘应幂等 nil, got %v", uerr)
	}
	if _, cancel := fk.counts(); cancel != 2 {
		t.Fatalf("已收尾句柄不应再 Cancel, got %d", cancel)
	}
}

// TestRemoveDropsBridge 覆盖 #251 新增的生产清理入口：Remove 后同一 client
// 的 For 重建新桥（全局表不再永久持有 client 与订阅表）。
func TestRemoveDropsBridge(t *testing.T) {
	type fakeClient struct{ id int }
	c := &fakeClient{id: 1}
	defer Remove(c)

	fk := &fakeBackend{}
	b1 := For(c, fk.hooks())
	Remove(c)
	b2 := For(c, fk.hooks())
	if b1 == b2 {
		t.Fatal("#251: Remove 后 For 应重建桥")
	}
	if _, err := b2.Subscribe("d", "g", func(_, _, _, _ string) {}); err != nil {
		t.Fatalf("新桥 Subscribe: %v", err)
	}
	if listen, _ := fk.counts(); listen != 1 {
		t.Fatalf("新桥应独立注册底层监听, got %d", listen)
	}
}
