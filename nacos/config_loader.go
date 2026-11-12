package nacos

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync/atomic"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/core/config"
	"github.com/oekit/logx"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"gopkg.in/yaml.v3"
)

var log = logx.Delegating("registry/nacos")

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

// configEvent 是 Listen 事件队列的载荷（SDK 推送四元组原样排队）。
type configEvent struct {
	namespace, group, dataId, data string
}

// listenEventQueueDepth 是单次 Listen 的事件队列深度：吸收瞬时连更抖动。
// 持续积压说明消费方 onChange 阻塞——入队采用阻塞发送把背压回传 SDK 派发
// goroutine，不丢事件（丢失配置变更会让旧配置无限期生效）。
const listenEventQueueDepth = 128

// Listen 监听 Nacos 配置变更，当配置发生变化时自动重新合并并回调 onChange。
//
// 契约：
//   - current 是注册时的基线快照；库内以「最近一次交付给 onChange 的合并
//     结果」为下一次热更的合并基线（#105）——每次变更从上次生效值起算，
//     先前生效的远端覆盖不会被回滚成 bootstrap 值。调用方「整体替换自身
//     配置指针」与「原地修改自身配置」两种写法结果一致（闭包不再回读
//     入口 current）；作为交换，onChange 收到的 *T 归库所有——不得原地
//     改写该指针（要改请基于它拷贝出自己的配置再替换自身指针）；
//   - 回调串行执行（#146）：SDK 对同一 dataID 的每次推送各起一个新
//     goroutine 裸调 OnChange（config_client.go 裸 go listener(...)，与
//     nacosimpl/config_store.go #127 的断言同源）——两次连更并发、乱序
//     进入回调。本方法用单 dispatcher goroutine + channel 排队把读-改-写
//     收敛到单一执行流：事件按到达序 FIFO 串行处理，回调不会并发调用；
//     残余边界：到达序 ≠ 推送序（SDK 每事件各起 goroutine，推送顺序本就
//     不可观测），本修复消灭的是基线丢失更新与回调并发；
//   - T 必须可 JSON 往返（深拷贝依赖），否则本次热更被跳过并记 Error，
//     绝不降级为原地合并调用方的活配置；
//   - onChange 的 panic 被就地隔离（recover 转错误日志，#96）：dispatcher
//     若被回调 panic 打死，后续事件全部滞留队列、SDK 派发 goroutine 逐个
//     阻塞在入队上——按事件隔离后热更循环继续。
//
// 返回注册错误（#53）：此前 ListenConfig 失败仅 log.Error——监听根本没挂上，
// 后续配置变更永不投递，调用方无从感知热更已失效（0.x 允许 breaking：调用方
// 需处理返回值，失败即 fail-fast 或自行告警）。
func Listen[T any](client config_client.IConfigClient, cfg Bootstrap, current *T, onChange func(*T)) error {
	latest := &atomic.Pointer[T]{}
	latest.Store(current)

	events := make(chan configEvent, listenEventQueueDepth)
	// 注册失败时收尾 dispatcher（否则空转驻留）；正常路径 stop 永不关闭，
	// dispatcher 与 SDK 监听同生命周期（SDK 无按事件反注册 API）。
	stop := make(chan struct{})
	async.GoSafe(func() {
		for {
			select {
			case ev := <-events:
				applyHotUpdate(latest, cfg, onChange, ev)
			case <-stop:
				return
			}
		}
	})
	err := client.ListenConfig(vo.ConfigParam{
		DataId: cfg.DataId,
		Group:  cfg.GroupOrDefault(),
		OnChange: func(namespace, group, dataId, data string) {
			// 只入队，读-改-写全部收敛到 dispatcher（#146）；队列满时阻塞
			// 背压（见 listenEventQueueDepth），不丢事件。
			events <- configEvent{namespace: namespace, group: group, dataId: dataId, data: data}
		},
	})
	if err != nil {
		close(stop) // 注册失败：dispatcher 在本轮处理后退出，不驻留泄漏
		log.Error("listen config failed", logx.ErrKey, err)
		return fmt.Errorf("nacos: listen config [%s/%s]: %w", cfg.GroupOrDefault(), cfg.DataId, err)
	}
	return nil
}

// applyHotUpdate 处理单个配置变更事件（仅在 Listen 的 dispatcher goroutine
// 内执行，读-改-写天然串行，#146）：克隆基线 → 合并远端 → 入库 → 回调。
func applyHotUpdate[T any](latest *atomic.Pointer[T], cfg Bootstrap, onChange func(*T), ev configEvent) {
	// #96：消费方回调质量不可控，panic 不得打死 dispatcher goroutine（后果：
	// 后续事件滞留队列、SDK 派发 goroutine 逐个阻塞在入队上），就地隔离转
	// 错误日志后继续处理下一事件。
	defer func() {
		if r := recover(); r != nil {
			log.Error("config change callback panicked (isolated)",
				"dataId", ev.dataId, logx.ErrKey, r, "stack", string(debug.Stack()))
		}
	}()
	log.Info("config changed", "namespace", ev.namespace, "group", ev.group, "dataId", ev.dataId)

	base, ok := cloneValue(latest.Load())
	if !ok {
		// 快照克隆失败（T 含 chan/func 等不可 JSON 字段）时放弃本次
		// 热更：降级为原地合并会改写调用方正在被读取的活配置
		return
	}

	var remote T
	if err := yaml.Unmarshal([]byte(ev.data), &remote); err != nil {
		log.Error("reload config unmarshal failed", logx.ErrKey, err)
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
}

func preserveNacosBootstrap[T any](cfg *T, bootstrap Bootstrap) {
	val := reflect.ValueOf(cfg).Elem()
	if val.Kind() != reflect.Struct {
		return
	}
	for i := range val.NumField() {
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
		log.Error("cloneValue marshal failed; hot update skipped", logx.ErrKey, err)
		return nil, false
	}
	clone := new(T)
	if err := json.Unmarshal(data, clone); err != nil {
		log.Error("cloneValue unmarshal failed; hot update skipped", logx.ErrKey, err)
		return nil, false
	}
	return clone, true
}
