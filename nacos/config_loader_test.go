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
