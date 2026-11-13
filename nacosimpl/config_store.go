package nacos

import (
	"context"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/logx"
	"github.com/oekit/registry"
	"github.com/oekit/registry/configbridge"
)

// ConfigStore 实现 [registry.ConfigStore] 接口，基于 Nacos config client。
//
// 从 Nacos 配置中心拉取和监听服务运行时配置（如超时、重试、LB 策略）。
//
// 监听经 [configbridge] 多播桥（registry#177）：SDK 的 ListenConfig 按
// (dataID, group) 单槽——同 key 二次注册新回调被静默丢弃、任一取消按 key
// 整删互杀。桥把单槽抬升为多播：同 key 多订阅者共存全部收到、句柄互不影响、
// 最后一个订阅者离开才 Cancel；同 client 的其他封装（nacos 的
// config_source/config_loader）共享同一张表。
//
// 接口层无订阅句柄（registry.ConfigStore 签名固定），本封装以 LIFO 句柄栈
// 适配 StopListenConfig：后进先出配对（与 etcdx 替换语义的「停当前生效者」
// 同向）；配对式 Listen/Stop 使用下精确，交错配对下至少不再整 key 互杀。
type ConfigStore struct {
	client config_client.IConfigClient
	bridge *configbridge.Bridge

	hMu     sync.Mutex
	handles map[string][]configbridge.Handle // key(group\x00dataID) → 句柄栈
}

var _ registry.ConfigStore = (*ConfigStore)(nil)

// NewConfigStore 创建基于 Nacos 的配置存储。
//
// 必须传入非 nil 的 configClient，否则返回 ErrNilClient。
func NewConfigStore(client config_client.IConfigClient) (*ConfigStore, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	bridge := configbridge.For(client, configbridge.Hooks{
		Listen: func(dataID, group string, onChange configbridge.ChangeFunc) error {
			return client.ListenConfig(vo.ConfigParam{
				DataId:   dataID,
				Group:    group,
				OnChange: onChange,
			})
		},
		Cancel: func(dataID, group string) error {
			return client.CancelListenConfig(vo.ConfigParam{
				DataId: dataID,
				Group:  group,
			})
		},
		OnPanic: func(dataID, group string, r any, stack string) {
			// #96/#125 同口径：SDK 派发链无 recover，消费方回调 panic 会打死
			// 进程；桥逐订阅隔离，此处接本仓日志面（父仓 configbridge 零 logx）。
			log.Error("config change callback panicked (isolated)",
				"dataId", dataID, "group", group, logx.ErrKey, r, "stack", stack)
		},
	})
	return &ConfigStore{
		client:  client,
		bridge:  bridge,
		handles: make(map[string][]configbridge.Handle),
	}, nil
}

func storeKey(dataID, group string) string { return group + "\x00" + dataID }

// GetConfig 从 Nacos 配置中心拉取指定 dataID 的配置内容。
//
// 配置不存在时返回空字符串和 nil error。
//
// ctx 语义（#143，与 Registrar/Discoverer 的 #65 纪律对齐）：SDK API 不接收
// context，无法真正中断底层调用。这里以竞速方式兜底——ctx 已取消/超时时
// 快速失败返回 ctx.Err()（不发起或不再等待 SDK 调用）；SDK 调用若已在进行
// 会在后台自行完成（结果丢弃）。消费方（如 nns 逐服务串行拉配置）传入的
// 取消/超时预算对 config 路径生效。
func (s *ConfigStore) GetConfig(ctx context.Context, dataID, group string) (string, error) {
	content, err := invokeWithCtx(ctx, func() (string, error) {
		return s.client.GetConfig(vo.ConfigParam{
			DataId: dataID,
			Group:  group,
		})
	})
	if err != nil {
		return "", wrapErr("get config", dataID+"/"+group, err)
	}
	return content, nil
}

// ListenConfig 监听指定 dataID 的配置变更。
//
// 当配置内容变化时，回调 onChange 并传入最新内容字符串。
//
// 多播语义（registry#177）：同 dataID/group 的重复 Listen **不再静默失效**——
// 本封装与同 client 的其他封装经 [configbridge] 共享表，每个订阅者独立收到
// 推送；派发串行（#127）与 panic 隔离（#96/#125）由桥统一承担。
//
// ctx 语义（#143）：注册本身是本地登记，仅做进入前 ctx.Err() 预检
// （ctx 已取消/超时直接失败，不发起注册）；**onChange 不受 ctx 管理**——
// 监听挂上后派发链持续投递，ctx 取消不会停止监听，需要停监听走
// [ConfigStore.StopListenConfig]。
func (s *ConfigStore) ListenConfig(ctx context.Context, dataID, group string, onChange func(data string)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id, err := s.bridge.Subscribe(dataID, group, func(_, _, _, data string) {
		log.Info("config changed", "dataId", dataID, "group", group)
		onChange(data)
	})
	if err != nil {
		return wrapErr("listen config", dataID+"/"+group, err)
	}
	k := storeKey(dataID, group)
	s.hMu.Lock()
	s.handles[k] = append(s.handles[k], id)
	s.hMu.Unlock()
	return nil
}

// StopListenConfig 停止监听指定 dataID 的配置变更。
//
// 多播语义（registry#177）：按句柄栈 LIFO 弹出**本封装最近一次** Listen 的
// 订阅（接口无句柄的适配口径，与 etcdx 替换语义的「停当前生效者」同向）；
// 其他订阅者（同封装更早的 Listen、同 client 其他封装）**不受影响**——
// 此前 SDK 的 CancelListenConfig 按 key 整删，任一停止把同 key 所有订阅者
// 一并互杀。本封装已无订阅时幂等返回 nil（不触底层）。
//
// ctx 语义（#143）：与 [ConfigStore.GetConfig] 相同——竞速兜底，ctx 已取消/
// 超时快速失败，不再等待取消调用。
func (s *ConfigStore) StopListenConfig(ctx context.Context, dataID, group string) error {
	// ctx 预检（#143 口径）：即使本封装无订阅（no-op 路径），pre-canceled
	// 也保持快速失败——与 ListenConfig 的入口预检对称。
	if err := ctx.Err(); err != nil {
		return err
	}
	k := storeKey(dataID, group)
	s.hMu.Lock()
	stack := s.handles[k]
	if len(stack) == 0 {
		s.hMu.Unlock()
		return nil // 幂等：本封装无该 key 订阅（重复 Stop / 从未 Listen）
	}
	id := stack[len(stack)-1]
	if len(stack) == 1 {
		delete(s.handles, k)
	} else {
		s.handles[k] = stack[:len(stack)-1]
	}
	s.hMu.Unlock()

	if err := invokeWithCtxErr(ctx, func() error {
		return s.bridge.Unsubscribe(dataID, group, id)
	}); err != nil {
		return wrapErr("stop listen config", dataID+"/"+group, err)
	}
	return nil
}
