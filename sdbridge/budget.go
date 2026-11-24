package sdbridge

import (
	"context"
	"time"

	"github.com/oekit/pkg/contextx"
	"github.com/oekit/pkg/errors"
	"github.com/oekit/registry"
)

// DefaultRegisterBudget 是 (反)注册单次操作的缺省预算（3s）：kitex/registry
// #191 与 kitex/nns #212 的统一口径（hertz 孪生 #317/#318 同值）。
//
// kitex 与 hertz 的 server registry 接口均无 ctx 形参，框架在启动路径
// 自动 Register、优雅关停路径 Deregister——裸 context.Background() 则底层
// Registrar（如 nacosimpl）拿到的 ctx 永无截止时间，注册中心不可达时
// （反）注册退化为 SDK 内部阻塞时长，拖住整个优雅关停序列。缺省 3s，应
// 显著小于 bootstrap 关停 closer 的预算，给错误处理与后续 closer 留余量。
const DefaultRegisterBudget = 3 * time.Second

// RegisterWithBudget 带单次预算注册实例：「context.WithTimeout + Registrar
// 调用」的共享收口。
//
// budget <= 0 时回退 [DefaultRegisterBudget]（nil 参数 = 显式回退默认值，
// 不算非法）；ctx 为 nil 时按 context.Background() 处理。ctx 自带更早的
// deadline/cancel 时以调用方为准（context.WithTimeout 语义，不放宽）。
//
// registrar 为 nil 返回错误（运行期帮助函数不 panic；构造期 nil 请用
// [EnsureNotNil] fail-fast）。错误原样透传不加前缀——错误文案前缀由
// 调用方（各框架 adapter）按自身口径添加，与 [ResolverCore.Fetch] 的
// errPrefix 口径一致。
//
// 消费方：hertz/registry、hertz/nns、kitex/registry、kitex/nns 四个
// adapter 的 registry.go（各自内联的 context.WithTimeout 拷贝已收口至此）。
func RegisterWithBudget(ctx context.Context, reg registry.Registrar, inst registry.Instance, budget time.Duration) error {
	if reg == nil {
		return errors.New("sdbridge: RegisterWithBudget requires non-nil registrar")
	}
	ctx, cancel := withBudget(ctx, budget)
	defer cancel()
	return reg.Register(ctx, inst)
}

// DeregisterWithBudget 带单次预算注销实例：语义与约束同 [RegisterWithBudget]，
// 预算覆盖优雅关停路径的 Deregister——注册中心不可达时最多阻塞 budget
// （缺省 [DefaultRegisterBudget]），不再拖死关停序列。
//
// 消费方：同 [RegisterWithBudget]（hertz/registry、hertz/nns、
// kitex/registry、kitex/nns 四个 adapter 的 registry.go）。
func DeregisterWithBudget(ctx context.Context, reg registry.Registrar, inst registry.Instance, budget time.Duration) error {
	if reg == nil {
		return errors.New("sdbridge: DeregisterWithBudget requires non-nil registrar")
	}
	ctx, cancel := withBudget(ctx, budget)
	defer cancel()
	return reg.Deregister(ctx, inst)
}

// withBudget 规范化 ctx 与预算并派生带截止时间的 ctx（Register/Deregister
// WithBudget 的共同前段）：nil ctx 回退 Background（单源收敛到
// pkg/contextx.OrBackground，#231），非正预算回退 [DefaultRegisterBudget]。
func withBudget(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	ctx = contextx.OrBackground(ctx)
	if budget <= 0 {
		budget = DefaultRegisterBudget
	}
	return context.WithTimeout(ctx, budget)
}
