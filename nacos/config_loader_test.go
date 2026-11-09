package nacos

import (
	"context"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// stubConfigClient 是 [config_client.IConfigClient] 的最小 mock：
// 内嵌接口，仅覆盖测试触达的方法，未覆盖方法不会被调用。
type stubConfigClient struct {
	config_client.IConfigClient

	listenConfigFunc func(vo.ConfigParam) error
}

func (s *stubConfigClient) ListenConfig(p vo.ConfigParam) error {
	if s.listenConfigFunc != nil {
		return s.listenConfigFunc(p)
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
