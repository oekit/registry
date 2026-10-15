package nacos

import (
	"context"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/registry/registry"
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
func (s *ConfigStore) GetConfig(ctx context.Context, dataID, group string) (string, error) {
	content, err := s.client.GetConfig(vo.ConfigParam{
		DataId: dataID,
		Group:  group,
	})
	if err != nil {
		return "", wrapErr("get config", dataID+"/"+group, err)
	}
	return content, nil
}

// ListenConfig 监听指定 dataID 的配置变更。
//
// 当配置内容变化时，回调 onChange 并传入最新内容字符串。
func (s *ConfigStore) ListenConfig(ctx context.Context, dataID, group string, onChange func(data string)) error {
	err := s.client.ListenConfig(vo.ConfigParam{
		DataId: dataID,
		Group:  group,
		OnChange: func(_, _, dataID, data string) {
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
func (s *ConfigStore) StopListenConfig(ctx context.Context, dataID, group string) error {
	err := s.client.CancelListenConfig(vo.ConfigParam{
		DataId: dataID,
		Group:  group,
	})
	if err != nil {
		return wrapErr("stop listen config", dataID+"/"+group, err)
	}
	return nil
}
