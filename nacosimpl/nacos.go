package nacos

import (
	"context"
	"fmt"
	"maps"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/oekit/logx"
	"github.com/oekit/registry"
)

var log = logx.Delegating("registry/nacosimpl")

// toInstance 将 Nacos SDK 的 model.Instance 转换为 provider 无关的 registry.Instance。
// metadata 做深拷贝：SDK 会在推送更新时原地修改内部实例对象，共享 map
// 会引发脏读与数据竞争。
//
// Cluster 还原自 model.Instance.ClusterName（#94）：Register 写入的集群名
// 在读取面原样带回，多集群同服务（灰度/单元化/就近路由）场景下消费方
// 可以从结果直接归属集群。
//
// Group 无法还原：nacos-sdk-go v2 把 Group 放在 Service 层，不在
// model.Instance 上——因此本包的 Discover/DiscoverAll/Subscribe 返回的
// registry.Instance 一律不带 Group（写侧可经 Register 的 inst.Group
// 或构造选项指定，读侧需调用方自行记忆）。这一限制是 provider 语义，
// 见 [Discoverer] 与 [Subscriber] 的文档。
func toInstance(serviceName string, m model.Instance) registry.Instance {
	return registry.Instance{
		Service:  serviceName,
		IP:       m.Ip,
		Port:     int(m.Port),
		Weight:   m.Weight,
		Cluster:  m.ClusterName,
		Metadata: copyMetadata(m.Metadata),
	}
}

// copyMetadata 返回 metadata 的浅层拷贝（值为 string，一层即深拷贝）；nil 透传 nil。
func copyMetadata(src map[string]string) map[string]string {
	return maps.Clone(src)
}

// toInstances 批量转换。
func toInstances(serviceName string, src []model.Instance) []registry.Instance {
	result := make([]registry.Instance, 0, len(src))
	for _, m := range src {
		result = append(result, toInstance(serviceName, m))
	}
	return result
}

// wrapErr 包装错误并附带 Nacos 上下文信息。
func wrapErr(op, service string, err error) error {
	return fmt.Errorf("registry/nacos: %s [%s]: %w", op, service, err)
}

// invokeWithCtx 以 ctx 感知方式执行一次无 ctx 入参的 SDK 调用（#65）。
//
// nacos-sdk-go 的 API（RegisterInstance/SelectInstances 等）不接收
// context.Context，底层 HTTP 请求与内部重试无法被外部中断——此前
// Registrar/Discoverer 签名里的 ctx 被完全忽略，bootstrap 给关停期反注册
// 的独立超时预算对它完全无效：Nacos 不可达时 SDK 内部阻塞多久，关停就拖多久。
//
// 本函数用 goroutine + select 竞速兜住这条 SDK 限制：
//   - 进入前 ctx 已取消/超时 ⇒ 不发起 SDK 调用，直接返回 ctx.Err()；
//   - SDK 调用进行中 ctx 取消/超时 ⇒ 立即返回 ctx.Err()（调用方的超时预算
//     生效）；底层 SDK 调用无法中断，会在后台自行完成，结果被丢弃——
//     这是 SDK 的限制，注册/反注册幂等性由 Nacos 侧保证；
//   - SDK 调用 panic 被就地 recover 并转为 error 返回（不打死进程，
//     也不让 select 悬挂）。
//
// channel 带缓冲（容量 1）：ctx 先返回时后台调用仍能完成发送，goroutine 不泄漏。
func invokeWithCtx[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	type result struct {
		val T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{zero, fmt.Errorf("nacos sdk call panicked: %v", r)}
			}
		}()
		v, err := fn()
		ch <- result{v, err}
	}()

	select {
	case r := <-ch:
		return r.val, r.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// invokeWithCtxErr 是 [invokeWithCtx] 的 error-only 便捷变体：SDK 调用只返回
// error（如 CancelListenConfig）时免去 struct{} 占位类型。语义与 invokeWithCtx
// 完全一致（预取消快速失败 / 竞速 / panic 转 error，#143）。
func invokeWithCtxErr(ctx context.Context, fn func() error) error {
	_, err := invokeWithCtx(ctx, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}
