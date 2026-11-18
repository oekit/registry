package sdbridge

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/oekit/registry"
)

// recorderRegistrar 记录派发进来的操作、实例与 ctx 截止时间（预算口径探针）。
type recorderRegistrar struct {
	mu          sync.Mutex
	calls       []string
	instances   []registry.Instance
	deadline    time.Time
	hasDeadline bool
	err         error
}

func (r *recorderRegistrar) Register(ctx context.Context, inst registry.Instance) error {
	return r.record(ctx, "Register", inst)
}

func (r *recorderRegistrar) Deregister(ctx context.Context, inst registry.Instance) error {
	return r.record(ctx, "Deregister", inst)
}

func (r *recorderRegistrar) record(ctx context.Context, op string, inst registry.Instance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, op)
	r.instances = append(r.instances, inst)
	r.deadline, r.hasDeadline = ctx.Deadline()
	return r.err
}

func (r *recorderRegistrar) snapshot() (calls []string, insts []registry.Instance, deadline time.Time, hasDeadline bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...), append([]registry.Instance(nil), r.instances...), r.deadline, r.hasDeadline
}

// blockingRegistrar 阻塞到 ctx 取消/超时，返回 ctx.Err()（预算竞速探针）。
type blockingRegistrar struct{}

func (blockingRegistrar) Register(ctx context.Context, _ registry.Instance) error {
	<-ctx.Done()
	return ctx.Err()
}

func (blockingRegistrar) Deregister(ctx context.Context, _ registry.Instance) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestDefaultRegisterBudget 钉住缺省预算 = 3s（kitex/registry #191 与
// kitex/nns #212 的统一口径，hertz 孪生 #317/#318 同值）。
func TestDefaultRegisterBudget(t *testing.T) {
	if DefaultRegisterBudget != 3*time.Second {
		t.Fatalf("DefaultRegisterBudget = %v, want 3s (kitex #191 口径)", DefaultRegisterBudget)
	}
}

var testInstance = registry.Instance{Service: "echo", IP: "10.0.0.1", Port: 8080, Weight: 10}

// TestRegisterWithBudget_ExplicitBudget 显式预算生效：deadline 落在预算窗口内。
func TestRegisterWithBudget_ExplicitBudget(t *testing.T) {
	rec := &recorderRegistrar{}
	budget := 100 * time.Millisecond
	before := time.Now()
	if err := RegisterWithBudget(context.Background(), rec, testInstance, budget); err != nil {
		t.Fatalf("RegisterWithBudget() error = %v", err)
	}

	calls, insts, deadline, hasDeadline := rec.snapshot()
	if len(calls) != 1 || calls[0] != "Register" {
		t.Fatalf("calls = %v, want [Register]", calls)
	}
	if len(insts) != 1 || !reflect.DeepEqual(insts[0], testInstance) {
		t.Fatalf("instances = %v, want passthrough %+v", insts, testInstance)
	}
	if !hasDeadline {
		t.Fatal("derived ctx has no deadline")
	}
	elapsed := deadline.Sub(before)
	if elapsed < budget-10*time.Millisecond || elapsed > budget+100*time.Millisecond {
		t.Fatalf("deadline in %v, want ~%v", elapsed, budget)
	}
}

// TestRegisterWithBudget_DefaultBudget 非正预算回退 DefaultRegisterBudget：
// 0 与负值均得 ~3s 截止时间（nil 参数 = 显式回退默认值，不算非法）。
func TestRegisterWithBudget_DefaultBudget(t *testing.T) {
	for name, budget := range map[string]time.Duration{"zero": 0, "negative": -time.Second} {
		t.Run(name, func(t *testing.T) {
			rec := &recorderRegistrar{}
			before := time.Now()
			if err := RegisterWithBudget(context.Background(), rec, testInstance, budget); err != nil {
				t.Fatalf("RegisterWithBudget(budget=%v) error = %v", budget, err)
			}
			_, _, deadline, hasDeadline := rec.snapshot()
			if !hasDeadline {
				t.Fatal("derived ctx has no deadline")
			}
			elapsed := deadline.Sub(before)
			if elapsed < DefaultRegisterBudget-100*time.Millisecond || elapsed > DefaultRegisterBudget+100*time.Millisecond {
				t.Fatalf("deadline in %v, want ~%v (DefaultRegisterBudget)", elapsed, DefaultRegisterBudget)
			}
		})
	}
}

// TestRegisterWithBudget_NilCtx ctx 为 nil 按 Background 处理（仍有预算）。
func TestRegisterWithBudget_NilCtx(t *testing.T) {
	rec := &recorderRegistrar{}
	//nolint:staticcheck // nil ctx 是帮助函数显式声明的回退路径
	if err := RegisterWithBudget(nil, rec, testInstance, time.Second); err != nil {
		t.Fatalf("RegisterWithBudget(nil ctx) error = %v", err)
	}
	if _, _, deadline, hasDeadline := rec.snapshot(); !hasDeadline {
		t.Fatal("nil ctx must fall back to Background and still derive a deadline")
	} else if deadline.IsZero() {
		t.Fatal("deadline is zero")
	}
}

// TestRegisterWithBudget_RespectsEarlierDeadline 调用方 ctx 自带更早 deadline
// 时不放宽：阻塞 Registrar 在外层 30ms deadline 处返回 DeadlineExceeded，
// 而不是等满 10s 预算。
func TestRegisterWithBudget_RespectsEarlierDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := RegisterWithBudget(ctx, blockingRegistrar{}, testInstance, 10*time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("blocked %v, want ~30ms (caller deadline must win)", elapsed)
	}
}

// TestRegisterWithBudget_ErrorPassthrough 底层错误原样透传（前缀由调用方加）。
func TestRegisterWithBudget_ErrorPassthrough(t *testing.T) {
	boom := errors.New("nacos down")
	rec := &recorderRegistrar{err: boom}
	err := RegisterWithBudget(context.Background(), rec, testInstance, time.Second)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want passthrough %v", err, boom)
	}
	if err.Error() != boom.Error() {
		t.Errorf("error message = %q, want unmodified %q", err.Error(), boom.Error())
	}
}

// TestRegisterWithBudget_NilRegistrar registrar 为 nil 返回错误而非 panic
// （运行期帮助函数显式上抛；构造期 nil 用 EnsureNotNil fail-fast）。
func TestRegisterWithBudget_NilRegistrar(t *testing.T) {
	err := RegisterWithBudget(context.Background(), nil, testInstance, time.Second)
	if err == nil {
		t.Fatal("error = nil, want error for nil registrar")
	}
	if want := "sdbridge: RegisterWithBudget requires non-nil registrar"; err.Error() != want {
		t.Errorf("error = %q, want %q", err.Error(), want)
	}
}

// TestDeregisterWithBudget_ExplicitBudget 注销路径镜像：操作名、实例与预算均正确。
func TestDeregisterWithBudget_ExplicitBudget(t *testing.T) {
	rec := &recorderRegistrar{}
	budget := 250 * time.Millisecond
	before := time.Now()
	if err := DeregisterWithBudget(context.Background(), rec, testInstance, budget); err != nil {
		t.Fatalf("DeregisterWithBudget() error = %v", err)
	}

	calls, insts, deadline, hasDeadline := rec.snapshot()
	if len(calls) != 1 || calls[0] != "Deregister" {
		t.Fatalf("calls = %v, want [Deregister]", calls)
	}
	if len(insts) != 1 || !reflect.DeepEqual(insts[0], testInstance) {
		t.Fatalf("instances = %v, want passthrough %+v", insts, testInstance)
	}
	if !hasDeadline {
		t.Fatal("derived ctx has no deadline")
	}
	if elapsed := deadline.Sub(before); elapsed < budget-10*time.Millisecond || elapsed > budget+100*time.Millisecond {
		t.Fatalf("deadline in %v, want ~%v", elapsed, budget)
	}
}

// TestDeregisterWithBudget_NilRegistrar 与注册路径对称的 nil 防护。
func TestDeregisterWithBudget_NilRegistrar(t *testing.T) {
	err := DeregisterWithBudget(context.Background(), nil, testInstance, 0)
	if err == nil {
		t.Fatal("error = nil, want error for nil registrar")
	}
	if want := "sdbridge: DeregisterWithBudget requires non-nil registrar"; err.Error() != want {
		t.Errorf("error = %q, want %q", err.Error(), want)
	}
}
