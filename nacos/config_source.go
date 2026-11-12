package nacos

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"

	"github.com/oekit/core/config"
	"github.com/oekit/logx"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// ConfigSource 将 Nacos 配置项适配为 config.ConfigSource（读 + 监听），
// 使 config.ConfigWatcher 可以直接消费 Nacos，而无需再手写一份监听逻辑。
//
// 与 Listen 的区别：Listen 自带「拉取 + YAML 解析 + OverlayMerge + 回调」
// 的合并语义；ConfigSource 只负责原始内容的读取与变更通知，
// 解析和合并策略由 ConfigWatcher 的 parser/回调决定。
type ConfigSource struct {
	client config_client.IConfigClient
	cfg    Bootstrap
	// listenErr 记录最近一次 ListenConfig 注册失败（#53 可查询状态面）：
	// nil = 监听已挂上。经 Err() 出口读取。
	listenErr atomic.Pointer[error]
}

// Err 返回 ListenConfig 的注册失败（nil = 监听已挂上）。
// Watch 本身实现 core/config.ConfigSource（void 签名），失败无法经返回值
// 上抛——需要 fail-fast 的调用方在 Watch 之后读本方法。
func (s *ConfigSource) Err() error {
	if p := s.listenErr.Load(); p != nil {
		return *p
	}
	return nil
}

var _ config.ConfigSource = (*ConfigSource)(nil)

// NewConfigSource 创建基于 Nacos 的配置源。
func NewConfigSource(client config_client.IConfigClient, cfg Bootstrap) *ConfigSource {
	return &ConfigSource{client: client, cfg: cfg}
}

// Read 读取当前配置内容。
func (s *ConfigSource) Read() ([]byte, error) {
	content, err := s.client.GetConfig(vo.ConfigParam{
		DataId: s.cfg.DataId,
		Group:  s.cfg.GroupOrDefault(),
	})
	if err != nil {
		return nil, fmt.Errorf("get nacos config [%s/%s]: %w", s.cfg.Group, s.cfg.DataId, err)
	}
	return []byte(content), nil
}

// Watch 监听配置变更，变更时调用 onChange（异步，立即返回）。
//
// ctx 取消后：onChange 不再向调用方投递，且自动调用 SDK CancelListenConfig
// 反注册监听（#126）——SDK 有反注册 API（同矩阵 nacosimpl.StopListenConfig
// 即调它实现），不反注册会让被取消的 watcher 留下永不释放的 SDK 监听
// （cacheMap 条目、服务端数据监听、推送 goroutine 只增不减）。
//
// onChange 的 panic 被就地隔离（recover 转错误日志，#96）：SDK config 侧
// 在独立 goroutine 上裸调 OnChange（config_client.go 无 recover），回调
// 异常不得打死宿主进程。ctx 收尾 goroutine 里的 SDK 反注册调用（#126）同样
// 就地隔离（#144）——SDK 在 CancelListenConfig 内部 panic 不得打死宿主进程。
//
// ⚠️ 本方法实现 core/config.ConfigSource 接口（void 签名不可改）：注册失败
// 经 **Err()** 可查询（#53）——ListenConfig 失败仅靠一条 ERROR 日志不可见，
// 「配置中心改了下不去」的故障排查时不会先想到看启动日志。失败即需要
// fail-fast 的调用方应在 Watch 后检查 Err()。
func (s *ConfigSource) Watch(ctx context.Context, onChange func()) {
	err := s.client.ListenConfig(vo.ConfigParam{
		DataId: s.cfg.DataId,
		Group:  s.cfg.GroupOrDefault(),
		OnChange: func(namespace, group, dataId, data string) {
			// #96：SDK config 侧派发链无 recover（config_client.go 裸
			// go listener(...)），onChange panic 会沿该 goroutine 上抛打死
			// 进程。消费方回调质量不可控，就地隔离转为错误日志。
			defer func() {
				if r := recover(); r != nil {
					log.Error("config change callback panicked (isolated)",
						"dataId", dataId, logx.ErrKey, r, "stack", string(debug.Stack()))
				}
			}()
			select {
			case <-ctx.Done():
				return
			default:
			}
			log.Info("config changed", "namespace", namespace, "group", group, "dataId", dataId)
			onChange()
		},
	})
	if err != nil {
		log.Error("listen config failed", "dataId", s.cfg.DataId, logx.ErrKey, err)
		s.listenErr.Store(&err)
		return
	}
	// #126：ctx 取消即向 SDK 反注册监听（CancelListenConfig，与
	// nacosimpl.StopListenConfig 同一 API）。此前 Watch 无任何停止出口，
	// 被取消的 watcher 的 SDK 监听永不释放（泄漏 + 推送空跑）。
	//
	// #144：收尾 goroutine 会跑进 provider SDK 的 CancelListenConfig——
	// SDK 内部断言/空指针等 panic 会沿 goroutine 逃逸打死宿主进程，与
	// onChange 的就地隔离（#96，上方）是同一故障的两个面。照仓内先例
	// （本方法 onChange、nacosimpl invokeWithCtx）加内联 recover：panic 转
	// 错误日志，反注册失败只损失一次清理，不炸进程。
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("cancel listen config panicked (isolated)",
					"dataId", s.cfg.DataId, logx.ErrKey, r, "stack", string(debug.Stack()))
			}
		}()
		<-ctx.Done()
		if err := s.client.CancelListenConfig(vo.ConfigParam{
			DataId: s.cfg.DataId,
			Group:  s.cfg.GroupOrDefault(),
		}); err != nil {
			log.Error("cancel listen config failed", "dataId", s.cfg.DataId, logx.ErrKey, err)
		}
	}()
}
