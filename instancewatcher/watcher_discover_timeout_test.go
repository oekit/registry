package instancewatcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oekit/registry"
)

// blockingDiscoverer 模拟不主动返回、只等 ctx 的 Discoverer（#231 的挂死面：
// 插件式 Nacos/etcd 客户端在网络分区时可能长时间不返回）。
type blockingDiscoverer struct {
	mu    sync.Mutex
	calls int
}

func (d *blockingDiscoverer) Discover(ctx context.Context, _ string) ([]registry.Instance, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (d *blockingDiscoverer) DiscoverAll(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	return d.Discover(ctx, serviceName)
}

func (d *blockingDiscoverer) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// TestInstanceWatcher231_DiscoverTimeoutUnblocksRound 钉住 core#231(d)：
// 挂死的 Discover 必须被单次预算截断，不能卡住 checkAll（进而卡住轮询
// 主循环）。没有 WithDiscoverTimeout 时该测试会一直挂到超时。
func TestInstanceWatcher231_DiscoverTimeoutUnblocksRound(t *testing.T) {
	disc := &blockingDiscoverer{}
	h := NewInstanceWatcher(disc,
		WithWatchServices("svc-a", "svc-b"),
		WithDiscoverTimeout(40*time.Millisecond),
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.checkAll(context.Background())
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("checkAll blocked on hung Discover: WithDiscoverTimeout not enforced (#231)")
	}
	if got := disc.callCount(); got != 2 {
		t.Errorf("Discover calls = %d, want 2 (timeout must not abort the whole round)", got)
	}
}

// TestInstanceWatcher231_PerCallBudgetIndependent 钉住「每轮 Discover 独立
// 预算」：第一单发现耗尽自己的预算后，第二单仍拿满新预算（共享预算会让
// 后续服务的 ctx 一进函数就已到期）。
func TestInstanceWatcher231_PerCallBudgetIndependent(t *testing.T) {
	const budget = 80 * time.Millisecond
	disc := &deadlineRecorder{}
	h := NewInstanceWatcher(disc,
		WithWatchServices("svc-a", "svc-b"),
		WithDiscoverTimeout(budget),
	)

	h.checkAll(context.Background())

	remaining := disc.remainingBudgets()
	if len(remaining) != 2 {
		t.Fatalf("recorded %d deadlines, want 2", len(remaining))
	}
	// 第二单在首单耗尽 ~80ms 之后才开始；独立预算下它仍有接近满额的
	// 剩余时间（阈值取一半，容忍调度抖动）。
	if remaining[1] < budget/2 {
		t.Errorf("second Discover budget remaining = %v, want >= %v (per-call budgets must be independent, #231)",
			remaining[1], budget/2)
	}
}

// TestInstanceWatcher231_NegativeDisablesBudget 钉住负数语义：显式禁用单次
// 预算时 Discover 直接拿调用方 ctx（此处无 deadline），不自行加超时。
func TestInstanceWatcher231_NegativeDisablesBudget(t *testing.T) {
	disc := &deadlineRecorder{}
	h := NewInstanceWatcher(disc,
		WithWatchServices("svc-a"),
		WithDiscoverTimeout(-time.Second),
	)

	h.checkAll(context.Background())

	if disc.hasDeadline() {
		t.Error("negative WithDiscoverTimeout must disable per-call budget (#231)")
	}
}

// TestInstanceWatcher231_ZeroUsesDefault 钉住 0 = 回默认（不是禁用）。
func TestInstanceWatcher231_ZeroUsesDefault(t *testing.T) {
	disc := &blockingDiscoverer{}
	h := NewInstanceWatcher(disc, WithDiscoverTimeout(0))
	if h.discoverTimeout != DefaultDiscoverTimeout {
		t.Errorf("discoverTimeout = %v, want default %v (#231)", h.discoverTimeout, DefaultDiscoverTimeout)
	}
}

// TestInstanceWatcher231_StartExitOnCancel 集成路径：挂死的 Discover 不再
// 阻塞 Start 响应 ctx 取消（预算由调用方 ctx 派生）。
func TestInstanceWatcher231_StartExitOnCancel(t *testing.T) {
	disc := &blockingDiscoverer{}
	h := NewInstanceWatcher(disc,
		WithWatchServices("svc-a"),
		WithDiscoverTimeout(time.Second), // 预算远大于取消时延，靠 ctx 取消退出
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Start(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after ctx cancellation: hung Discover blocks the loop (#231)")
	}
	h.Stop()
}

// deadlineRecorder 记录每次 Discover 的剩余预算；等 ctx 到期后返回。
type deadlineRecorder struct {
	mu        sync.Mutex
	remaining []time.Duration
	deadline  bool
}

func (d *deadlineRecorder) Discover(ctx context.Context, _ string) ([]registry.Instance, error) {
	dl, ok := ctx.Deadline()
	d.mu.Lock()
	if ok {
		d.remaining = append(d.remaining, time.Until(dl))
	}
	d.deadline = d.deadline || ok
	d.mu.Unlock()

	if ok {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, nil
}

func (d *deadlineRecorder) DiscoverAll(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	return d.Discover(ctx, serviceName)
}

func (d *deadlineRecorder) remainingBudgets() []time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]time.Duration(nil), d.remaining...)
}

func (d *deadlineRecorder) hasDeadline() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deadline
}
