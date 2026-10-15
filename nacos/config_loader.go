package nacos

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/oekit/core/config"
	"github.com/oekit/logx"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"gopkg.in/yaml.v3"
)

var log = logx.Delegating("nacos")

// LoadAndMerge 从 Nacos 拉取远程配置并与本地配置进行覆盖式合并。
func LoadAndMerge[T any](client config_client.IConfigClient, cfg Bootstrap, local *T) error {
	content, err := client.GetConfig(vo.ConfigParam{
		DataId: cfg.DataId,
		Group:  cfg.Group,
	})
	if err != nil {
		return fmt.Errorf("get nacos config [%s/%s]: %w", cfg.Group, cfg.DataId, err)
	}

	if content == "" {
		log.Warn("config content is empty, using local config", "dataId", cfg.DataId)
		return nil
	}

	var remote T
	if err := yaml.Unmarshal([]byte(content), &remote); err != nil {
		return fmt.Errorf("unmarshal nacos config: %w", err)
	}

	config.OverlayMerge(local, &remote)

	preserveNacosBootstrap(local, cfg)

	log.Info("config loaded and merged", "dataId", cfg.DataId)
	return nil
}

// Listen 监听 Nacos 配置变更，当配置发生变化时自动重新合并并回调 onChange。
//
// 契约：
//   - current 是注册时的基线快照，onChange 必须整体替换指针、不得原地修改
//     current（热更基于快照深拷贝，原地修改会与 SDK 回调 goroutine 竞争）；
//   - T 必须可 JSON 往返（深拷贝依赖），否则本次热更被跳过并记 Error，
//     绝不降级为原地合并调用方的活配置。
func Listen[T any](client config_client.IConfigClient, cfg Bootstrap, current *T, onChange func(*T)) {
	err := client.ListenConfig(vo.ConfigParam{
		DataId: cfg.DataId,
		Group:  cfg.Group,
		OnChange: func(namespace, group, dataId, data string) {
			log.Info("config changed", "namespace", namespace, "group", group, "dataId", dataId)

			base, ok := cloneValue(current)
			if !ok {
				// 快照克隆失败（T 含 chan/func 等不可 JSON 字段）时放弃本次
				// 热更：降级为原地合并会改写调用方正在被读取的活配置
				return
			}

			var remote T
			if err := yaml.Unmarshal([]byte(data), &remote); err != nil {
				log.Error("reload config unmarshal failed", "err", err)
				return
			}

			config.OverlayMerge(base, &remote)
			preserveNacosBootstrap(base, cfg)

			onChange(base)
			log.Info("config reloaded dynamically")
		},
	})
	if err != nil {
		log.Error("listen config failed", "err", err)
	}
}

func preserveNacosBootstrap[T any](cfg *T, bootstrap Bootstrap) {
	val := reflect.ValueOf(cfg).Elem()
	if val.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if field.Type() == reflect.TypeOf(Bootstrap{}) && field.CanSet() {
			field.Set(reflect.ValueOf(bootstrap))
			return
		}
	}
}

// cloneValue 深拷贝配置快照（JSON 往返）。
// 失败时返回 (nil, false) 而非原指针：返回原指针会让后续 OverlayMerge
// 原地改写调用方的活配置。
func cloneValue[T any](ptr *T) (*T, bool) {
	data, err := json.Marshal(ptr)
	if err != nil {
		log.Error("cloneValue marshal failed; hot update skipped", "err", err)
		return nil, false
	}
	clone := new(T)
	if err := json.Unmarshal(data, clone); err != nil {
		log.Error("cloneValue unmarshal failed; hot update skipped", "err", err)
		return nil, false
	}
	return clone, true
}
