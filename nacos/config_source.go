package nacos

import (
	"context"
	"fmt"

	"github.com/oekit/registry/config"

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
		Group:  s.cfg.Group,
	})
	if err != nil {
		return nil, fmt.Errorf("get nacos config [%s/%s]: %w", s.cfg.Group, s.cfg.DataId, err)
	}
	return []byte(content), nil
}

// Watch 监听配置变更，变更时调用 onChange（异步，立即返回）。
// Nacos SDK 无监听反注册 API，ctx 取消后 onChange 不再向调用方投递。
func (s *ConfigSource) Watch(ctx context.Context, onChange func()) {
	err := s.client.ListenConfig(vo.ConfigParam{
		DataId: s.cfg.DataId,
		Group:  s.cfg.Group,
		OnChange: func(namespace, group, dataId, data string) {
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
		log.Error("listen config failed", "dataId", s.cfg.DataId, "err", err)
	}
}
