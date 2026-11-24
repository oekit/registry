package nacos

import (
	"context"
	"reflect"
	"runtime/debug"
	"sync/atomic"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/core/config"
	"github.com/oekit/logx"
	"github.com/oekit/pkg/contextx"
	"github.com/oekit/pkg/errors"
	"github.com/oekit/pkg/reflectx"
	"github.com/oekit/registry/configbridge"

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
		// #196：错误面与请求面同源（GroupOrDefault）——此前用裸 cfg.Group，
		// 空分组时实际请求 DEFAULT_GROUP、消息却渲染成 "[/app.yaml]"，
		// 按消息里的分组去控制台查会查不到。
		return errors.Wrapf(err, "get nacos config [%s/%s]", cfg.GroupOrDefault(), cfg.DataId)
	}

	if content == "" {
		log.Warn("config content is empty, using local config", "dataId", cfg.DataId)
		return nil
	}

	var remote T
	if err := yaml.Unmarshal([]byte(content), &remote); err != nil {
		return errors.Wrapf(err, "unmarshal nacos config")
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
// Deprecated: 使用 [ListenCtx]。本入口以 context.Background 注册，监听与
// 进程同生命周期——没有停止出口：dispatcher goroutine、configbridge 订阅与
// SDK 监听都会驻留到进程结束（registry#251）。需要可停止的生命周期时改传
// ctx，以其取消作为收尾信号。
func Listen[T any](client config_client.IConfigClient, cfg Bootstrap, current *T, onChange func(*T)) error {
	return ListenCtx(context.Background(), client, cfg, current, onChange)
}

// ListenCtx 监听 Nacos 配置变更，当配置发生变化时自动重新合并并回调 onChange。
//
// 监听生命周期绑定 ctx（registry#251）：ctx 取消后先经 configbridge 摘除本
// 订阅（最后一个订阅者离开时触发 SDK CancelListenConfig），再关闭 stop 让
// dispatcher goroutine 退出；收尾 goroutine 中的 SDK 调用 panic 就地隔离
// （与 config_source.Watch 的 #144 同口径）。ctx 已取消/超时时入口快速失败，
// 不注册监听。
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
//   - 热更基线深拷贝走反射（pkg/reflectx 单源，#195）：`json:"-"` 与
//     未导出字段原样保留——此前 JSON 往返对这两类字段**静默丢值**（不报错、
//     不触发跳过），零值被 latest.Store 固化为后续热更的基线；func/chan
//     共享即拷贝，不再因此跳过热更。基线为 nil 时本次热更被跳过并记
//     Error，绝不降级为原地合并调用方的活配置；
//   - onChange 的 panic 被就地隔离（recover 转错误日志，#96）：dispatcher
//     若被回调 panic 打死，后续事件全部滞留队列、SDK 派发 goroutine 逐个
//     阻塞在入队上——按事件隔离后热更循环继续。
//
// 返回注册错误（#53）：此前 ListenConfig 失败仅 log.Error——监听根本没挂上，
// 后续配置变更永不投递，调用方无从感知热更已失效（0.x 允许 breaking：调用方
// 需处理返回值，失败即 fail-fast 或自行告警）。
func ListenCtx[T any](ctx context.Context, client config_client.IConfigClient, cfg Bootstrap, current *T, onChange func(*T)) error {
	// nil ctx 按 Background 处理（core#159 / registry#240 全仓口径）。
	ctx = contextx.OrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}

	latest := &atomic.Pointer[T]{}
	latest.Store(current)

	events := make(chan configEvent, listenEventQueueDepth)
	// stop 是 dispatcher 的退出信号：注册失败与 ctx 取消两条路径都会关闭它。
	stop := make(chan struct{})
	done := make(chan struct{})
	async.GoSafe(func() {
		defer close(done)
		for {
			select {
			case ev := <-events:
				applyHotUpdate(latest, cfg, onChange, ev)
			case <-stop:
				return
			}
		}
	})
	// registry#177：经 configbridge 多播桥注册——与同 client 的 config_source
	// 共享一张表（此前两处各调 SDK ListenConfig，同 key 二次注册新回调被
	// 静默丢弃、任一取消按 key 整删互杀）。
	//
	// #251：句柄必须保留——ctx 取消时按句柄精确摘除本订阅（最后一个订阅者
	// 离开才触发 SDK CancelListenConfig）。此前句柄丢弃且成功路径 stop 永不
	// 关闭：dispatcher goroutine、桥订阅与 SDK 监听全部进程级泄漏，动态
	// Listen 每次泄漏一套，旧 onChange 继续写回运行态。
	bridge := configbridge.For(client, bridgeHooks(client))
	id, err := bridge.Subscribe(
		cfg.DataId, cfg.GroupOrDefault(),
		func(namespace, group, dataId, data string) {
			// 只入队，读-改-写全部收敛到 dispatcher（#146）；队列满时阻塞
			// 背压（见 listenEventQueueDepth），不丢事件。入队在桥的 per-key
			// 派发锁内执行：背压只拖本 key 推送，不跨 key 耦合。
			//
			// #251：stop 已关闭（收尾窗口内 SDK 仍可能推送，或 Unsubscribe
			// 失败后订阅被桥回滚）时丢弃事件——否则送入无人消费的队列会
			// 阻塞桥的派发锁。
			select {
			case <-stop:
				return
			default:
			}
			select {
			case events <- configEvent{namespace: namespace, group: group, dataId: dataId, data: data}:
			case <-stop:
			}
		})
	if err != nil {
		close(stop) // 注册失败：dispatcher 退出，不驻留泄漏
		<-done
		log.Error("listen config failed", logx.ErrKey, err)
		return errors.Wrapf(err, "nacos: listen config [%s/%s]", cfg.GroupOrDefault(), cfg.DataId)
	}

	// ctx 不可取消（Background/TODO 的 Done() 为 nil）时不启动收尾 goroutine：
	// 它只会永久阻塞在 nil channel 上，给 Deprecated 的 [Listen] 兼容路径
	// 平白多泄漏一个 goroutine。可取消 ctx 才需要收尾（#251）。
	if ctxDone := ctx.Done(); ctxDone != nil {
		async.GoSafe(func() {
			// defer 顺序：close(stop) 最后执行——即使 Unsubscribe 或 SDK
			// Cancel panic，dispatcher、等待者与调用方都不会被卡住。
			defer close(stop)
			defer func() {
				if r := recover(); r != nil {
					log.Error("cancel listen config panicked (isolated)",
						"dataId", cfg.DataId, logx.ErrKey, r, "stack", string(debug.Stack()))
				}
			}()
			<-ctxDone
			if err := bridge.Unsubscribe(cfg.DataId, cfg.GroupOrDefault(), id); err != nil {
				log.Error("cancel listen config failed", "dataId", cfg.DataId, logx.ErrKey, err)
			}
		})
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
		// 快照克隆失败（nil 基线）时放弃本次热更：降级为原地合并会
		// 改写调用方正在被读取的活配置
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

// cloneValue 深拷贝配置快照（反射深拷贝，单源 pkg/reflectx，#195）。
//
// 此前走 JSON 往返：T 里 `json:"-"` 与未导出字段在 Marshal 时被丢弃、
// Unmarshal 后为零值——往返**不报错**，既不触发「跳过」也不记 Error，
// 零值 clone 随即被 latest.Store 固化为下一次热更的基线并经 onChange
// 交付给调用方（#195）。反射深拷贝整块值拷贝 struct（未导出字段原样
// 保留，core#84 同款）+ 可 Set 引用型字段逐一递归，`json:"-"` 与未导出
// 字段均不再丢值；func/chan 共享即拷贝（不再因此跳过热更）。
//
// 失败（nil 基线）时返回 (nil, false) 而非原指针：返回原指针会让后续
// OverlayMerge 原地改写调用方的活配置。
func cloneValue[T any](ptr *T) (*T, bool) {
	if ptr == nil {
		log.Error("cloneValue nil baseline; hot update skipped")
		return nil, false
	}
	return reflectx.DeepCopy(ptr), true
}
