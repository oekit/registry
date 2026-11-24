package nacos

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/concurrency/async"
)

// stubConfigClient 是 [config_client.IConfigClient] 的最小 mock：
// 内嵌接口，仅覆盖测试触达的方法，未覆盖方法不会被调用。
type stubConfigClient struct {
	config_client.IConfigClient

	getConfigFunc          func(vo.ConfigParam) (string, error)
	listenConfigFunc       func(vo.ConfigParam) error
	cancelListenConfigFunc func(vo.ConfigParam) error
}

func (s *stubConfigClient) GetConfig(p vo.ConfigParam) (string, error) {
	if s.getConfigFunc != nil {
		return s.getConfigFunc(p)
	}
	return "", nil
}

func (s *stubConfigClient) ListenConfig(p vo.ConfigParam) error {
	if s.listenConfigFunc != nil {
		return s.listenConfigFunc(p)
	}
	return nil
}

func (s *stubConfigClient) CancelListenConfig(p vo.ConfigParam) error {
	if s.cancelListenConfigFunc != nil {
		return s.cancelListenConfigFunc(p)
	}
	return nil
}

// hotCfg 是热更测试用的最小配置。
type hotCfg struct {
	N int
}

// TestListen_OnChangePanicIsolated 是 #96 的回归钉：onChange 的 panic 必须
// 被就地隔离——SDK config 侧在独立 goroutine 上裸调 OnChange
// （config_client.go 全程无 recover），回调 panic 会打死宿主进程。
// 若未隔离，本测试 goroutine 直接炸穿、测试失败。
func TestListen_OnChangePanicIsolated(t *testing.T) {
	var fired func(namespace, group, dataId, data string)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired = p.OnChange
			return nil
		},
	}
	err := Listen(stub, Bootstrap{DataId: "app.yaml", Group: "G"}, &hotCfg{N: 1},
		func(*hotCfg) { panic("consumer callback exploded") })
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if fired == nil {
		t.Fatal("ListenConfig was not registered")
	}
	// 模拟 SDK 派发 goroutine 直调 OnChange（带合法 yaml 使其走到 onChange）。
	fired("ns", "G", "app.yaml", "n: 2")
}

// TestLoadAndMerge_ErrorNamesEffectiveGroup 回归（#196）：错误文案必须与
// 实际请求的分组同源（GroupOrDefault）——此前请求面回填 DEFAULT_GROUP、
// 错误面用裸 cfg.Group，空分组时消息渲染成 "get nacos config [/app.yaml]"
// （分位为空、形似路径错误），按消息里的分组去 Nacos 控制台查会查不到。
func TestLoadAndMerge_ErrorNamesEffectiveGroup(t *testing.T) {
	stub := &stubConfigClient{
		getConfigFunc: func(p vo.ConfigParam) (string, error) {
			if p.Group != "DEFAULT_GROUP" {
				t.Errorf("请求面 group = %q, want DEFAULT_GROUP（回填口径）", p.Group)
			}
			return "", errors.New("nacos down")
		},
	}
	err := LoadAndMerge(stub, Bootstrap{DataId: "app.yaml"}, &hotCfg{})
	if err == nil {
		t.Fatal("LoadAndMerge 必须上抛 GetConfig 错误")
	}
	if !strings.Contains(err.Error(), "DEFAULT_GROUP/app.yaml") {
		t.Fatalf("错误文案必须点名生效分组/数据ID, got: %v", err)
	}
}

// TestConfigSource_Read_ErrorNamesEffectiveGroup 同型覆盖 ConfigSource.Read
// （config_source.go 同一文案族，#196）。
func TestConfigSource_Read_ErrorNamesEffectiveGroup(t *testing.T) {
	stub := &stubConfigClient{
		getConfigFunc: func(p vo.ConfigParam) (string, error) {
			if p.Group != "DEFAULT_GROUP" {
				t.Errorf("请求面 group = %q, want DEFAULT_GROUP（回填口径）", p.Group)
			}
			return "", errors.New("nacos down")
		},
	}
	s := NewConfigSource(stub, Bootstrap{DataId: "app.yaml"})
	_, err := s.Read()
	if err == nil {
		t.Fatal("Read 必须上抛 GetConfig 错误")
	}
	if !strings.Contains(err.Error(), "DEFAULT_GROUP/app.yaml") {
		t.Fatalf("错误文案必须点名生效分组/数据ID, got: %v", err)
	}
}

// TestConfigSource_Watch_OnChangePanicIsolated 同型覆盖 ConfigSource.Watch
// 的回调出口（#96）。
func TestConfigSource_Watch_OnChangePanicIsolated(t *testing.T) {
	var fired func(namespace, group, dataId, data string)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired = p.OnChange
			return nil
		},
	}
	s := NewConfigSource(stub, Bootstrap{DataId: "app.yaml", Group: "G"})
	s.Watch(context.Background(), func() { panic("consumer callback exploded") })
	if fired == nil {
		t.Fatal("ListenConfig was not registered")
	}
	// 模拟 SDK 派发 goroutine 直调 OnChange（ctx 未取消 → 走到 onChange）。
	fired("ns", "G", "app.yaml", "anything")
	if err := s.Err(); err != nil {
		t.Errorf("Err() = %v, want nil (listen registered fine)", err)
	}
}

// TestConfigSource_Watch_CancelPanicIsolated 是 #144 的回归钉：Watch 的 ctx
// 收尾 goroutine 会跑进 provider SDK 的 CancelListenConfig——SDK 在该调用里
// panic 时不得沿 goroutine 逃逸打死宿主进程（与同一方法内 onChange 的就地
// 隔离 #96 同口径，也同 nacosimpl invokeWithCtx 先例）。若隔离缺失，panic
// 逃逸出 goroutine，整个测试二进制当场崩溃（比断言失败更响）。
func TestConfigSource_Watch_CancelPanicIsolated(t *testing.T) {
	cancelEntered := make(chan struct{})
	stub := &stubConfigClient{
		cancelListenConfigFunc: func(_ vo.ConfigParam) error {
			close(cancelEntered) // 先留痕再 panic：证明收尾 goroutine 确实跑进了 SDK 调用
			panic("nacos sdk panicked in CancelListenConfig")
		},
	}
	s := NewConfigSource(stub, Bootstrap{DataId: "app.yaml", Group: "G"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Watch(ctx, func() {})

	cancel() // 触发收尾 goroutine：<-ctx.Done() → CancelListenConfig → panic
	select {
	case <-cancelEntered:
		// 收尾 goroutine 已进入 SDK 调用并 panic——进程仍存活到此处即证明
		// panic 被就地隔离（#144）。recover 分支会写一条 Error 日志（可观测），
		// 此处不再断言日志内容。
	case <-time.After(5 * time.Second):
		t.Fatal("ctx 取消后收尾 goroutine 未进入 CancelListenConfig")
	}
}

// TestConfigSource_Watch_CtxCancelUnregistersSDKListener 是 #126 的回归钉：
// ctx 取消后必须调用 SDK CancelListenConfig 反注册监听——此前 Watch 注册后
// 无任何停止出口（注释谎称「SDK 无监听反注册 API」，与同矩阵
// nacosimpl.StopListenConfig 直接矛盾），被取消的 watcher 留下永不反注册的
// SDK 监听（cacheMap 条目、服务端数据监听、推送 goroutine 只增不减）。
func TestConfigSource_Watch_CtxCancelUnregistersSDKListener(t *testing.T) {
	var fired func(namespace, group, dataId, data string)
	cancelled := make(chan vo.ConfigParam, 1)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired = p.OnChange
			return nil
		},
		cancelListenConfigFunc: func(p vo.ConfigParam) error {
			cancelled <- p
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewConfigSource(stub, Bootstrap{DataId: "app.yaml", Group: "G"})
	delivered := 0
	s.Watch(ctx, func() { delivered++ })
	if fired == nil {
		t.Fatal("ListenConfig was not registered")
	}

	cancel()
	select {
	case p := <-cancelled:
		if p.DataId != "app.yaml" || p.Group != "G" {
			t.Errorf("CancelListenConfig param = %s/%s, want app.yaml/G", p.DataId, p.Group)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ctx 取消后 SDK CancelListenConfig 未被调用——SDK 监听永不反注册（#126 泄漏）")
	}

	// ctx 取消后 onChange 不再向调用方投递（既有行为，回归钉）。
	fired("ns", "G", "app.yaml", "late")
	if delivered != 0 {
		t.Errorf("ctx 取消后仍向调用方投递 %d 次, want 0", delivered)
	}
}

// TestConfigSource_Watch_NilCtx 是 #240 的回归钉：Watch 实现 core
// config.ConfigSource（void 签名不可改），派发闭包与收尾 goroutine 都直接
// select/<-ctx.Done()——nil ctx 经入口 contextx.OrBackground 兜底为
// Background 后正常注册监听、正常投递（修复前 nil 进 select 直接 nil 解引用
// panic，对照 instancewatcher 的 TestInstanceWatcher_StartNilCtx 形态）。
func TestConfigSource_Watch_NilCtx(t *testing.T) {
	var fired func(namespace, group, dataId, data string)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired = p.OnChange
			return nil
		},
	}
	s := NewConfigSource(stub, Bootstrap{DataId: "app.yaml", Group: "G"})

	delivered := 0
	//nolint:staticcheck // SA1012 故意传 nil，验证 nil ctx 兜底不 panic（#240）
	s.Watch(nil, func() { delivered++ })
	if fired == nil {
		t.Fatal("ListenConfig was not registered")
	}

	// 模拟 SDK 派发 goroutine 直调 OnChange（nil ctx 按 Background → 走到 onChange）。
	fired("ns", "G", "app.yaml", "anything")
	if delivered != 1 {
		t.Fatalf("onChange delivered = %d, want 1（nil ctx 按 Background 正常投递）", delivered)
	}
	if err := s.Err(); err != nil {
		t.Errorf("Err() = %v, want nil (listen registered fine)", err)
	}
}

// TestConfigSource_Watch_ErrRecoversAfterSuccess 是 #251 的回归钉：Err() 的
// 文档承诺是 nil = 监听已挂上，但 listenErr 此前只在失败时 Store——一次
// 失败后即使重试成功，Err() 仍永久返回旧错误（状态面说谎，调用方按 fail-fast
// 逻辑会一直判死）。成功路径必须把失败状态清零。
func TestConfigSource_Watch_ErrRecoversAfterSuccess(t *testing.T) {
	fail := true
	stub := &stubConfigClient{
		listenConfigFunc: func(vo.ConfigParam) error {
			if fail {
				return errors.New("nacos temporarily unavailable")
			}
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewConfigSource(stub, Bootstrap{DataId: "app.yaml", Group: "G"})

	s.Watch(ctx, func() {})
	if err := s.Err(); err == nil {
		t.Fatal("Err() = nil, want 首次注册失败错误")
	} else if !strings.Contains(err.Error(), "temporarily unavailable") {
		t.Fatalf("Err() = %v, want 原始注册错误", err)
	}

	// 故障恢复后重试：桥在失败时已回滚本地表项，本次会重新调 SDK ListenConfig。
	fail = false
	s.Watch(ctx, func() {})
	if err := s.Err(); err != nil {
		t.Fatalf("#251: 注册成功后 Err() = %v, want nil（陈旧的失败状态未清理）", err)
	}
}

// twoFieldCfg 是 #105 回归用的双字段配置：热更回滚缺陷需要「两个不同配置项
// 分两次变更」才暴露——第一次改 A、第二次改 B，第二次之后 A 不得退回
// bootstrap 值。
type twoFieldCfg struct {
	A string
	B string
}

// TestListen_HotUpdateAccumulatesAcrossChanges 回归（#105）：热更合并基线必须
// 是「最近一次生效的配置」而非入口参数 current——此前闭包每次从入口那一刻的
// current 克隆基线，调用方按契约整体替换自身指针后，第二次变更从 bootstrap
// 快照起算，第一次的远端覆盖被静默回滚（改 A、改 B 依次进行 ⇒ A 被回退成
// a0，远端配置「看起来生效了」又「莫名失效了」）。
func TestListen_HotUpdateAccumulatesAcrossChanges(t *testing.T) {
	var fired func(namespace, group, dataId, data string)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired = p.OnChange
			return nil
		},
	}

	// 投递经 dispatcher goroutine 异步化（#146：OnChange 只入队，合并与
	// 回调在单执行流串行），用通道按次等待，语义与旧同步行为等价。
	delivered := make(chan *twoFieldCfg, 8)
	err := Listen(stub, Bootstrap{DataId: "app.yaml", Group: "G"},
		&twoFieldCfg{A: "a0", B: "b0"}, // 入口基线快照
		func(c *twoFieldCfg) {
			// 契约写法：整体替换调用方自身配置指针（cfg = newCfg 的等价形态）
			delivered <- &twoFieldCfg{A: c.A, B: c.B}
		})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if fired == nil {
		t.Fatal("ListenConfig was not registered")
	}

	// 第一次变更：远端只改 A ⇒ 投递 {A:a1, B:b0}
	fired("ns", "G", "app.yaml", "a: a1\n")
	d1, ok := waitDelivered(delivered)
	if !ok {
		t.Fatal("第一次变更未投递")
	}
	if d1.A != "a1" || d1.B != "b0" {
		t.Fatalf("第一次变更后 = %+v, want {A:a1 B:b0}", d1)
	}

	// 第二次变更：远端只改 B ⇒ A 必须保留 a1，不得回滚成 bootstrap 值 a0
	fired("ns", "G", "app.yaml", "b: b1\n")
	d2, ok := waitDelivered(delivered)
	if !ok {
		t.Fatal("第二次变更未投递")
	}
	if d2.A != "a1" || d2.B != "b1" {
		t.Fatalf("第二次变更后 = %+v, want {A:a1 B:b1}（A 回退成 a0 即 #105 复发）", d2)
	}
}

// waitDelivered 等待一次投递（#146 异步化后的确定性等待助手）。
func waitDelivered(ch <-chan *twoFieldCfg) (*twoFieldCfg, bool) {
	select {
	case d := <-ch:
		return d, true
	case <-time.After(5 * time.Second):
		return nil, false
	}
}

// TestListen_HotUpdate_InPlaceCallerStyleAgrees（#105 语义决定项）：契约放宽
// 后「原地修改自身配置」的写法（此前唯一不回滚、但违反旧契约的形态）与
// 「整体替换自身指针」结果一致——闭包以库内基线起算，不回读调用方结构。
func TestListen_HotUpdate_InPlaceCallerStyleAgrees(t *testing.T) {
	var fired func(namespace, group, dataId, data string)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired = p.OnChange
			return nil
		},
	}

	cfg := &twoFieldCfg{A: "a0", B: "b0"}
	delivered := make(chan *twoFieldCfg, 8)
	err := Listen(stub, Bootstrap{DataId: "app.yaml", Group: "G"}, cfg,
		func(c *twoFieldCfg) {
			// 原地修改写法：*cfg = *newCfg（改的是调用方自身结构，非交付指针）
			*cfg = *c
			delivered <- &twoFieldCfg{A: c.A, B: c.B}
		})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	fired("ns", "G", "app.yaml", "a: a1\n")
	fired("ns", "G", "app.yaml", "b: b1\n")

	d1, ok := waitDelivered(delivered)
	if !ok {
		t.Fatal("变更未全部投递（1/2）")
	}
	_ = d1
	d2, ok := waitDelivered(delivered)
	if !ok {
		t.Fatal("变更未全部投递（2/2）")
	}
	if d2.A != "a1" || d2.B != "b1" {
		t.Fatalf("原地写法第二次变更后 = %+v, want {A:a1 B:b1}", d2)
	}
	if cfg.A != "a1" || cfg.B != "b1" {
		t.Errorf("调用方自身配置 = %+v, want {A:a1 B:b1}", cfg)
	}
}

// runtimeFieldsCfg 是 #195 回归用配置：json:"-" 与未导出字段是 derived/
// runtime 字段的常见写法，热更基线深拷贝不得丢。
type runtimeFieldsCfg struct {
	N      int
	Hidden string `json:"-"` // JSON 往返会丢；反射深拷贝必须保
	cached string // 未导出：JSON 往返必丢（Marshal 看不见）
}

// TestListen_HotUpdatePreservesJSONDashAndUnexportedFields 回归（#195）：
// 热更基线此前走 JSON 往返深拷贝——json:"-" 与未导出字段被**静默**丢弃
// （往返不报错：既不触发「跳过」也不记 Error），零值 clone 被
// latest.Store 固化为后续热更的基线并经 onChange 交付，调用方按 godoc
// 典型用法「整体替换自身配置指针」后这些字段归零且永不恢复（OverlayMerge
// 只做非零覆盖，零值不会被远端补回）。
func TestListen_HotUpdatePreservesJSONDashAndUnexportedFields(t *testing.T) {
	var fired func(namespace, group, dataId, data string)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired = p.OnChange
			return nil
		},
	}

	delivered := make(chan *runtimeFieldsCfg, 8)
	err := Listen(stub, Bootstrap{DataId: "app.yaml", Group: "G"},
		&runtimeFieldsCfg{N: 1, Hidden: "keep-hidden", cached: "keep-cached"},
		func(c *runtimeFieldsCfg) { delivered <- c })
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	waitOne := func(step string) *runtimeFieldsCfg {
		t.Helper()
		select {
		case d := <-delivered:
			return d
		case <-time.After(5 * time.Second):
			t.Fatalf("%s：变更未投递", step)
			return nil
		}
	}

	// 第一次远端变更：只改 N
	fired("ns", "G", "app.yaml", "n: 2\n")
	d1 := waitOne("第一次变更")
	if d1.N != 2 {
		t.Fatalf("第一次变更后 N = %d, want 2", d1.N)
	}
	if d1.Hidden != "keep-hidden" {
		t.Fatalf("#195: json:\"-\" 字段热更后归零: %+v", d1)
	}
	if d1.cached != "keep-cached" {
		t.Fatalf("#195: 未导出字段热更后归零: %+v", d1)
	}

	// 第二次变更：零值一旦被固化为基线就永不恢复——字段必须继续在
	fired("ns", "G", "app.yaml", "n: 3\n")
	d2 := waitOne("第二次变更")
	if d2.N != 3 {
		t.Fatalf("第二次变更后 N = %d, want 3", d2.N)
	}
	if d2.Hidden != "keep-hidden" || d2.cached != "keep-cached" {
		t.Fatalf("#195: 字段在基线固化后丢失: %+v", d2)
	}
}

// wideCfg 是 #146 回归用的 16 字段配置：丢失更新缺陷需要「多个事件并发
// 推送、各自从同一基线克隆合并」才暴露——每个事件只写一个字段，串行应用
// 后 16 个字段全部生效；任一事件的合并结果被并发覆盖即有字段停留 0。
type wideCfg struct {
	F1, F2, F3, F4, F5, F6, F7, F8        int
	F9, F10, F11, F12, F13, F14, F15, F16 int
}

// seqCfg 是到达序 FIFO 契约测试用配置。
type seqCfg struct {
	N int
}

// TestListen_ConcurrentPushNoLostUpdate 回归（#146）：SDK 对同一 dataID 的
// 每次推送各起一个新 goroutine 裸调 OnChange（并发且乱序），此前 OnChange
// 闭包内 Load→clone→merge→Store 的读-改-写无同步——G1/G2 都从同一基线
// 克隆、各自合并后先后 Store，后 Store 者覆盖先 Store 者的合并结果（丢失
// 更新，违反 #105「每次变更从上次生效值起算」契约）。修复后事件在单
// dispatcher 内串行应用：16 个并发推送的字段全部生效，无一丢失。
func TestListen_ConcurrentPushNoLostUpdate(t *testing.T) {
	var fired func(namespace, group, dataId, data string)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired = p.OnChange
			return nil
		},
	}

	var mu sync.Mutex
	var last *wideCfg
	delivered := make(chan struct{}, 64)
	err := Listen(stub, Bootstrap{DataId: "app.yaml", Group: "G"}, &wideCfg{},
		func(c *wideCfg) {
			mu.Lock()
			last = c
			mu.Unlock()
			delivered <- struct{}{}
		})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if fired == nil {
		t.Fatal("ListenConfig was not registered")
	}

	const n = 16
	start := make(chan struct{})
	for i := 1; i <= n; i++ {
		data := fmt.Sprintf("f%d: %d", i, i)
		async.GoSafe(func() {
			<-start
			fired("ns", "G", "app.yaml", data) // 模拟 SDK 每事件各起 goroutine 并发派发
		})
	}
	close(start) // 同时开跑

	for i := 0; i < n; i++ {
		select {
		case <-delivered:
		case <-time.After(5 * time.Second):
			t.Fatalf("仅收到 %d/%d 次回调（事件滞留队列或丢失）", i, n)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	v := reflect.ValueOf(last).Elem()
	for i := 1; i <= n; i++ {
		f := v.FieldByName(fmt.Sprintf("F%d", i))
		if f.Int() != int64(i) {
			t.Errorf("F%d = %d; want %d（该事件的合并结果被并发覆盖，丢失更新 #146）", i, f.Int(), i)
		}
	}
}

// TestListen_EventsDeliveredInArrivalOrder 钉住 #146 修复的 FIFO 契约：
// 顺序推送的变更按到达序逐个合并回调（回调串行、不并发、不乱序），与
// etcdx ConfigStore「回调串行执行」的承诺对齐。
func TestListen_EventsDeliveredInArrivalOrder(t *testing.T) {
	var fired func(namespace, group, dataId, data string)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired = p.OnChange
			return nil
		},
	}

	delivered := make(chan int, 16)
	err := Listen(stub, Bootstrap{DataId: "app.yaml", Group: "G"}, &seqCfg{},
		func(c *seqCfg) { delivered <- c.N })
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	const n = 8
	for i := 1; i <= n; i++ {
		// 顺序入队：OnChange 同步返回即已入队（缓冲队列），到达序 = 调用序
		fired("ns", "G", "app.yaml", fmt.Sprintf("n: %d", i))
	}

	var got []int
	for i := 0; i < n; i++ {
		select {
		case v := <-delivered:
			got = append(got, v)
		case <-time.After(5 * time.Second):
			t.Fatalf("仅收到 %d/%d 次回调", i, n)
		}
	}
	for i, v := range got {
		if v != i+1 {
			t.Fatalf("投递序 = %v; want [1..%d]（到达序 FIFO，#146）", got, n)
		}
	}
}

// —— registry#177：config_loader 与 config_source 跨构造同 key 共存 ——

// TestLoaderAndSource_ShareBridgeSameKey 钉住 #177 证据 1 的 nacos 包场景：
// config_loader 与 config_source 各自直调 SDK ListenConfig 时，同 key 第二次
// 注册被 SDK 静默丢弃（只有先注册者收到）；任一取消（source 的 ctx 收尾走
// CancelListenConfig）按 key 整删，幸存者静默失联。
// 经 configbridge 桥后：底层只注册一次、两构造的回调全部收到、source
// ctx 取消只摘自己，loader 继续热更。
func TestLoaderAndSource_ShareBridgeSameKey(t *testing.T) {
	var sdkOnChanges []func(namespace, group, dataId, data string)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			sdkOnChanges = append(sdkOnChanges, p.OnChange)
			return nil
		},
		cancelListenConfigFunc: func(_ vo.ConfigParam) error { return nil },
	}
	cfg := Bootstrap{DataId: "app.yaml", Group: "G"}

	// loader 先注册（热更路径）。断言回调**次数**而非合并值——
	// OverlayMerge 的覆盖语义不属本测试面（#177 钉的是收没收到）。
	var loaderGot atomic.Int32
	if err := Listen(stub, cfg, &hotCfg{N: 1}, func(*hotCfg) {
		loaderGot.Add(1)
	}); err != nil {
		t.Fatalf("loader Listen: %v", err)
	}

	// source 同 key 注册（此前被 SDK 静默丢弃）。
	src := NewConfigSource(stub, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var srcGot atomic.Int32
	src.Watch(ctx, func() { srcGot.Add(1) })
	if err := src.Err(); err != nil {
		t.Fatalf("source Watch: %v", err)
	}
	if len(sdkOnChanges) != 1 {
		t.Fatalf("★ 跨构造同 key 应共享底层注册（SDK 单槽只注册一次）, got %d", len(sdkOnChanges))
	}

	// 一次推送：两构造都收到（此前只有 loader 收到）。
	sdkOnChanges[0]("ns", "G", "app.yaml", `{"N":2}`)
	if srcGot.Load() != 1 {
		t.Fatalf("★ source 应收到推送（此前被静默丢弃）, got %d", srcGot.Load())
	}
	deadline := time.Now().Add(2 * time.Second)
	for loaderGot.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("★ loader 应收到推送（此前的静默丢回调回归）")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// source ctx 取消：只摘 source，loader 继续热更（互杀回归）。
	cancel()
	time.Sleep(100 * time.Millisecond) // 收尾 goroutine 异步摘除
	sdkOnChanges[0]("ns", "G", "app.yaml", `{"N":3}`)
	if got := srcGot.Load(); got != 1 {
		t.Fatalf("★ source 取消后不应再收到, got %d", got)
	}
	deadline = time.Now().Add(2 * time.Second)
	for loaderGot.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("★ source 取消后 loader 仍应热更（按 key 互杀回归）")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitGoroutinesBack 等待 goroutine 数回落到基线（#251 泄漏测试助手）。
// 超时说明收尾路径没有把本方法启动的 goroutine 全部回收。
func waitGoroutinesBack(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= baseline {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("#251: goroutine 未随收尾回收: now=%d baseline=%d", n, baseline)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestListenCtx_CtxCancelStopsDispatcherAndUnsubscribes 是 #251 的回归钉：
// 此前 Listen 成功路径的 stop 永不关闭、桥订阅句柄丢弃——被取消/替换的
// 监听留下 dispatcher goroutine、configbridge 表项与 SDK 监听，旧 onChange
// 继续写回运行态。ListenCtx 必须在 ctx 取消后 Unsubscribe（最后一个订阅者
// 离开触发 SDK CancelListenConfig）、停止 dispatcher，且迟到推送不再投递。
func TestListenCtx_CtxCancelStopsDispatcherAndUnsubscribes(t *testing.T) {
	fired := make(chan func(namespace, group, dataId, data string), 1)
	cancelled := make(chan vo.ConfigParam, 1)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired <- p.OnChange
			return nil
		},
		cancelListenConfigFunc: func(p vo.ConfigParam) error {
			cancelled <- p
			return nil
		},
	}
	baseline := runtime.NumGoroutine()

	var delivered atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	if err := ListenCtx(ctx, stub, Bootstrap{DataId: "app.yaml", Group: "G"}, &hotCfg{N: 1},
		func(*hotCfg) { delivered.Add(1) }); err != nil {
		cancel()
		t.Fatalf("ListenCtx: %v", err)
	}
	var sdkOnChange func(namespace, group, dataId, data string)
	select {
	case sdkOnChange = <-fired:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("ListenConfig was not registered")
	}

	cancel()
	select {
	case p := <-cancelled:
		if p.DataId != "app.yaml" || p.Group != "G" {
			t.Errorf("CancelListenConfig param = %s/%s, want app.yaml/G", p.DataId, p.Group)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("#251: ctx 取消后未摘除桥订阅（SDK CancelListenConfig 未调用）")
	}
	// 收尾完成后本方法启动的 dispatcher/收尾 goroutine 必须全部退出。
	waitGoroutinesBack(t, baseline)

	// 收尾后 SDK 仍可能迟到派发（或 Unsubscribe 失败后订阅被桥回滚）：
	// 旧回调不得再入队、不得再写回运行态（阻塞派发锁/泄漏点）。
	sdkOnChange("ns", "G", "app.yaml", "n: 2")
	time.Sleep(50 * time.Millisecond)
	if got := delivered.Load(); got != 0 {
		t.Fatalf("#251: ctx 取消后旧 onChange 仍投递 %d 次", got)
	}
}

// TestListenCtx_SubscribeFailureNoDispatcherLeak 是 #251 的另一半：注册失败
// 路径必须同步收尾 dispatcher（此前 close(stop) 后不等待，goroutine 退出
// 时机不可判定）。ListenCtx 返回错误时 goroutine 数已回落基线。
func TestListenCtx_SubscribeFailureNoDispatcherLeak(t *testing.T) {
	stub := &stubConfigClient{
		listenConfigFunc: func(vo.ConfigParam) error { return errors.New("nacos unavailable") },
	}
	baseline := runtime.NumGoroutine()
	err := ListenCtx(context.Background(), stub, Bootstrap{DataId: "app.yaml", Group: "G"},
		&hotCfg{}, func(*hotCfg) {})
	if err == nil {
		t.Fatal("ListenCtx 必须上抛注册错误")
	}
	waitGoroutinesBack(t, baseline)
}

// TestListenCtx_RestartAfterCancel 钉住收尾后的可重启性：ctx 取消完成
// Unsubscribe 后，同一 client 的同 key 再注册必须重新走 SDK ListenConfig
// （桥的 bridged 状态已随最后一个订阅者离开复位），且再次取消仍能收尾。
func TestListenCtx_RestartAfterCancel(t *testing.T) {
	var listenN, cancelN atomic.Int32
	stub := &stubConfigClient{
		listenConfigFunc: func(vo.ConfigParam) error {
			listenN.Add(1)
			return nil
		},
		cancelListenConfigFunc: func(vo.ConfigParam) error {
			cancelN.Add(1)
			return nil
		},
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	if err := ListenCtx(ctx1, stub, Bootstrap{DataId: "app.yaml", Group: "G"}, &hotCfg{}, func(*hotCfg) {}); err != nil {
		cancel1()
		t.Fatalf("第一次 ListenCtx: %v", err)
	}
	cancel1()
	deadline := time.Now().Add(2 * time.Second)
	for cancelN.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("第一次取消未触发 SDK CancelListenConfig")
		}
		time.Sleep(10 * time.Millisecond)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := ListenCtx(ctx2, stub, Bootstrap{DataId: "app.yaml", Group: "G"}, &hotCfg{}, func(*hotCfg) {}); err != nil {
		t.Fatalf("取消后重新 ListenCtx: %v", err)
	}
	if got := listenN.Load(); got != 2 {
		t.Fatalf("取消后重启应重新注册 SDK 监听（listen 总 2 次）, got %d", got)
	}
	cancel2()
	deadline = time.Now().Add(2 * time.Second)
	for cancelN.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("第二次取消未触发 SDK CancelListenConfig")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
