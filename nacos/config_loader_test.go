package nacos

import (
	"context"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// stubConfigClient 是 [config_client.IConfigClient] 的最小 mock：
// 内嵌接口，仅覆盖测试触达的方法，未覆盖方法不会被调用。
type stubConfigClient struct {
	config_client.IConfigClient

	listenConfigFunc       func(vo.ConfigParam) error
	cancelListenConfigFunc func(vo.ConfigParam) error
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

// hotCfg 是热更测试用的最小可 JSON 往返配置。
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

	var delivered []*twoFieldCfg
	err := Listen(stub, Bootstrap{DataId: "app.yaml", Group: "G"},
		&twoFieldCfg{A: "a0", B: "b0"}, // 入口基线快照
		func(c *twoFieldCfg) {
			// 契约写法：整体替换调用方自身配置指针（cfg = newCfg 的等价形态）
			delivered = append(delivered, &twoFieldCfg{A: c.A, B: c.B})
		})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	if fired == nil {
		t.Fatal("ListenConfig was not registered")
	}

	// 第一次变更：远端只改 A ⇒ 投递 {A:a1, B:b0}
	fired("ns", "G", "app.yaml", "a: a1\n")
	if len(delivered) != 1 {
		t.Fatalf("第一次变更未投递, delivered=%d", len(delivered))
	}
	if delivered[0].A != "a1" || delivered[0].B != "b0" {
		t.Fatalf("第一次变更后 = %+v, want {A:a1 B:b0}", delivered[0])
	}

	// 第二次变更：远端只改 B ⇒ A 必须保留 a1，不得回滚成 bootstrap 值 a0
	fired("ns", "G", "app.yaml", "b: b1\n")
	if len(delivered) != 2 {
		t.Fatalf("第二次变更未投递, delivered=%d", len(delivered))
	}
	if delivered[1].A != "a1" || delivered[1].B != "b1" {
		t.Fatalf("第二次变更后 = %+v, want {A:a1 B:b1}（A 回退成 a0 即 #105 复发）", delivered[1])
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
	var delivered []*twoFieldCfg
	err := Listen(stub, Bootstrap{DataId: "app.yaml", Group: "G"}, cfg,
		func(c *twoFieldCfg) {
			// 原地修改写法：*cfg = *newCfg（改的是调用方自身结构，非交付指针）
			*cfg = *c
			delivered = append(delivered, &twoFieldCfg{A: c.A, B: c.B})
		})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	fired("ns", "G", "app.yaml", "a: a1\n")
	fired("ns", "G", "app.yaml", "b: b1\n")

	if len(delivered) != 2 {
		t.Fatalf("变更未全部投递, delivered=%d", len(delivered))
	}
	if delivered[1].A != "a1" || delivered[1].B != "b1" {
		t.Fatalf("原地写法第二次变更后 = %+v, want {A:a1 B:b1}", delivered[1])
	}
	if cfg.A != "a1" || cfg.B != "b1" {
		t.Errorf("调用方自身配置 = %+v, want {A:a1 B:b1}", cfg)
	}
}
