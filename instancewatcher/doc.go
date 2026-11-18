// Package instancewatcher 提供框架无关的实例轮询 watcher。
//
// # 来源（#319）
//
// 整段自 hertz/nns/health.go 迁入（oekit/hertz#319），hertz/nns 保留
// re-export 薄壳；类型名（InstanceWatcher/InstanceEvent/EventType 及各
// Option）与行为不变。hertz 特有组件改注册名：healthLog
// （logx.Delegating("nns-hertz")）改为本包日志组件 log
// （logx.Delegating("registry/instancewatcher")）。imports 仅标准库 +
// concurrency/async + logx + registry/sdbridge，零 Hertz/Kitex 框架依赖，
// kitex 侧可同样消费（后续接入）。
//
// # 职责
//
// 基于 [registry.Discoverer] 定期轮询服务实例：排序 diff（顺序不敏感）出
// 增/删/改事件异步派发给订阅回调。三条防线随迁：
//
//   - 深拷贝纪律：入参服务列表、cache 写入、回调事件逐回调各自拷贝，
//     别名写穿不污染 diff 基线；
//   - 异步派发：回调经 async.GoSafe 独立 goroutine 执行（panic 收编），
//     单个慢/挂死回调不拖轮询主循环；
//   - inflight 计数：上一轮回调未排空时跳过本轮派发且 cache 不动
//     （cache 是 diff 基线，先消费再跳过会永久吞掉变更），排空后重 diff
//     补发。
//
// # 用法
//
//	w := instancewatcher.NewInstanceWatcher(disc,
//	    instancewatcher.WithWatchInterval(5*time.Second),
//	    instancewatcher.WithWatchServices("svc-a", "svc-b"),
//	    instancewatcher.WithWatchCallback(func(evt instancewatcher.InstanceEvent) {...}),
//	)
//	go w.Start(ctx)
//	defer w.Stop()
package instancewatcher
