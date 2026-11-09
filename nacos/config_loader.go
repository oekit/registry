package nacos

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync/atomic"

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
		Group:  cfg.GroupOrDefault(),
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
//   - current 是注册时的基线快照；库内以「最近一次交付给 onChange 的合并
//     结果」为下一次热更的合并基线（#105）——每次变更从上次生效值起算，
//     先前生效的远端覆盖不会被回滚成 bootstrap 值。调用方「整体替换自身
//     配置指针」与「原地修改自身配置」两种写法结果一致（闭包不再回读
//     入口 current）；作为交换，onChange 收到的 *T 归库所有——不得原地
//     改写该指针（SDK 派发是逐事件 go 裸调，原地改写会与下一次热更的
//     快照克隆竞争；要改请基于它拷贝出自己的配置再替换自身指针）；
//   - T 必须可 JSON 往返（深拷贝依赖），否则本次热更被跳过并记 Error，
//     绝不降级为原地合并调用方的活配置；
//   - onChange 的 panic 被就地隔离（recover 转错误日志，#96）：SDK config
//     侧在独立 goroutine 上裸调 OnChange（config_client.go 无 recover），
//     回调异常不得打死宿主进程。
//
// 返回注册错误（#53）：此前 ListenConfig 失败仅 log.Error——监听根本没挂上，
// 后续配置变更永不投递，调用方无从感知热更已失效（0.x 允许 breaking：调用方
// 需处理返回值，失败即 fail-fast 或自行告警）。
func Listen[T any](client config_client.IConfigClient, cfg Bootstrap, current *T, onChange func(*T)) error {
	// #105：闭包持有「最近一次生效的配置」，而不是入口参数 current——
	// 此前每次热更都从入口那一刻的 current 克隆基线，调用方按契约整体替换
	// 自身指针后闭包仍指向最初的 bootstrap 快照 ⇒ 第二次变更从 bootstrap
	// 起算，第一次的远端覆盖被静默回滚（改 A、改 B 依次进行 ⇒ 每次都回退
	// 前一次）。atomic.Pointer 承接上一次交付的结果；SDK 对同一 dataId 的
	// 变更逐事件派发，Store/Load 保证基线读写的原子可见性。
	latest := &atomic.Pointer[T]{}
	latest.Store(current)
	err := client.ListenConfig(vo.ConfigParam{
		DataId: cfg.DataId,
		Group:  cfg.GroupOrDefault(),
		OnChange: func(namespace, group, dataId, data string) {
			// #96：SDK config 侧派发链无 recover（config_client.go 裸
			// go listener(...)），onChange panic 会沿该 goroutine 上抛打死
			// 进程。消费方回调质量不可控，就地隔离转为错误日志。
			defer func() {
				if r := recover(); r != nil {
					log.Error("config change callback panicked (isolated)",
						"dataId", dataId, "err", r, "stack", string(debug.Stack()))
				}
			}()
			log.Info("config changed", "namespace", namespace, "group", group, "dataId", dataId)

			base, ok := cloneValue(latest.Load())
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

			// 先入库基线再回调：本次合并结果已是远端的最新生效值，回调
			// panic（被隔离）不应让它从基线里丢失；回调若整体替换自身
			// 指针也不影响库内基线（两者此后互不相干）。
			latest.Store(base)

			onChange(base)
			log.Info("config reloaded dynamically")
		},
	})
	if err != nil {
		log.Error("listen config failed", "err", err)
		return fmt.Errorf("nacos: listen config [%s/%s]: %w", cfg.GroupOrDefault(), cfg.DataId, err)
	}
	return nil
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
