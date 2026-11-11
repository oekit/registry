package nacos

import (
	"context"
	"runtime/debug"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/registry"
)

// ConfigStore 实现 [registry.ConfigStore] 接口，基于 Nacos config client。
//
// 从 Nacos 配置中心拉取和监听服务运行时配置（如超时、重试、LB 策略）。
type ConfigStore struct {
	client config_client.IConfigClient
}

var _ registry.ConfigStore = (*ConfigStore)(nil)

// NewConfigStore 创建基于 Nacos 的配置存储。
//
// 必须传入非 nil 的 configClient，否则返回 ErrNilClient。
func NewConfigStore(client config_client.IConfigClient) (*ConfigStore, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	return &ConfigStore{client: client}, nil
}

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
// ctx 语义（#143）：注册本身是 SDK 侧的快速本地登记，仅做进入前 ctx.Err()
// 预检（ctx 已取消/超时直接失败，不发起注册）；**onChange 不受 ctx 管理**——
// 监听挂上后 SDK 派发链持续投递，ctx 取消不会停止监听，需要停监听走
// [ConfigStore.StopListenConfig]。
func (s *ConfigStore) ListenConfig(ctx context.Context, dataID, group string, onChange func(data string)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var deliverMu sync.Mutex
	err := s.client.ListenConfig(vo.ConfigParam{
		DataId: dataID,
		Group:  group,
		OnChange: func(_, _, dataID, data string) {
			// #127：SDK 对同一 dataID 每次推送各起一个新 goroutine 裸调
			// OnChange（config_client.go 裸 go listener(...)），两次连更会
			// 并发、乱序进入消费方 onChange——与 etcdx ConfigStore「回调串行
			// 执行、不会并发调用 onChange」的承诺分裂，快速连更可能旧配置
			// 最后生效。投递前加锁串行化，与 etcdx 口径对齐。
			deliverMu.Lock()
			defer deliverMu.Unlock()
			// #96/#125：SDK config 侧派发链无 recover（config_client.go 裸
			// go listener(...)），onChange panic 会沿该 goroutine 上抛打死
			// 进程。消费方回调质量不可控，就地隔离转为错误日志。
			defer func() {
				if r := recover(); r != nil {
					log.Error("config change callback panicked (isolated)",
						"dataId", dataID, "err", r, "stack", string(debug.Stack()))
				}
			}()
			log.Info("config changed", "dataId", dataID)
			onChange(data)
		},
	})
	if err != nil {
		return wrapErr("listen config", dataID+"/"+group, err)
	}
	return nil
}

// StopListenConfig 停止监听指定 dataID 的配置变更。
//
// ctx 语义（#143）：与 [ConfigStore.GetConfig] 相同——竞速兜底，ctx 已取消/
// 超时快速失败，不再等待 SDK 调用。
func (s *ConfigStore) StopListenConfig(ctx context.Context, dataID, group string) error {
	if err := invokeWithCtxErr(ctx, func() error {
		return s.client.CancelListenConfig(vo.ConfigParam{
			DataId: dataID,
			Group:  group,
		})
	}); err != nil {
		return wrapErr("stop listen config", dataID+"/"+group, err)
	}
	return nil
}
