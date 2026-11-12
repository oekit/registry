package nacos

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/registry"
)

// stubNamingClient is a minimal mock of [naming_client.INamingClient].
// We embed the interface so we only have to override the methods we
// exercise in tests; the embedded field stays nil and we never call
// the un-overridden methods.
type stubNamingClient struct {
	naming_client.INamingClient

	regInstanceFunc        func(vo.RegisterInstanceParam) (bool, error)
	deregInstanceFunc      func(vo.DeregisterInstanceParam) (bool, error)
	getServiceFunc         func(vo.GetServiceParam) (model.Service, error)
	selectInstancesFunc    func(vo.SelectInstancesParam) ([]model.Instance, error)
	selectAllInstancesFunc func(vo.SelectAllInstancesParam) ([]model.Instance, error)
	subscribeFunc          func(*vo.SubscribeParam) error
	unsubscribeFunc        func(*vo.SubscribeParam) error
}

func (s *stubNamingClient) RegisterInstance(p vo.RegisterInstanceParam) (bool, error) {
	if s.regInstanceFunc != nil {
		return s.regInstanceFunc(p)
	}
	return true, nil
}

func (s *stubNamingClient) DeregisterInstance(p vo.DeregisterInstanceParam) (bool, error) {
	if s.deregInstanceFunc != nil {
		return s.deregInstanceFunc(p)
	}
	return true, nil
}

func (s *stubNamingClient) GetService(p vo.GetServiceParam) (model.Service, error) {
	if s.getServiceFunc != nil {
		return s.getServiceFunc(p)
	}
	return model.Service{Name: p.ServiceName, GroupName: p.GroupName}, nil
}

func (s *stubNamingClient) SelectInstances(p vo.SelectInstancesParam) ([]model.Instance, error) {
	if s.selectInstancesFunc != nil {
		return s.selectInstancesFunc(p)
	}
	return nil, nil
}

func (s *stubNamingClient) SelectAllInstances(p vo.SelectAllInstancesParam) ([]model.Instance, error) {
	if s.selectAllInstancesFunc != nil {
		return s.selectAllInstancesFunc(p)
	}
	return nil, nil
}

func (s *stubNamingClient) Subscribe(p *vo.SubscribeParam) error {
	if s.subscribeFunc != nil {
		return s.subscribeFunc(p)
	}
	return nil
}

func (s *stubNamingClient) Unsubscribe(p *vo.SubscribeParam) error {
	if s.unsubscribeFunc != nil {
		return s.unsubscribeFunc(p)
	}
	return nil
}

// stubConfigClient is a minimal mock of [config_client.IConfigClient].
type stubConfigClient struct {
	config_client.IConfigClient

	getConfigFunc          func(vo.ConfigParam) (string, error)
	publishConfigFunc      func(vo.ConfigParam) (bool, error)
	listenConfigFunc       func(vo.ConfigParam) error
	cancelListenConfigFunc func(vo.ConfigParam) error
}

func (s *stubConfigClient) GetConfig(p vo.ConfigParam) (string, error) {
	if s.getConfigFunc != nil {
		return s.getConfigFunc(p)
	}
	return "", nil
}

func (s *stubConfigClient) PublishConfig(p vo.ConfigParam) (bool, error) {
	if s.publishConfigFunc != nil {
		return s.publishConfigFunc(p)
	}
	return true, nil
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

// --- Registrar tests ---

// TestRegistrar_NewWithOptions verifies WithRegistrarGroup and
// WithRegistrarCluster options are applied.
func TestRegistrar_NewWithOptions(t *testing.T) {
	stub := &stubNamingClient{}
	r, err := NewRegistrar(stub,
		WithRegistrarGroup("CUSTOM_GROUP"),
		WithRegistrarCluster("CUSTOM_CLUSTER"),
	)
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	if r.group != "CUSTOM_GROUP" {
		t.Errorf("expected group=CUSTOM_GROUP, got %q", r.group)
	}
	if r.cluster != "CUSTOM_CLUSTER" {
		t.Errorf("expected cluster=CUSTOM_CLUSTER, got %q", r.cluster)
	}
}

// TestRegistrar_NewDefaultGroup covers the default "DEFAULT_GROUP"
// when WithRegistrarGroup is not used.
func TestRegistrar_NewDefaultGroup(t *testing.T) {
	r, err := NewRegistrar(&stubNamingClient{})
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	if r.group != "DEFAULT_GROUP" {
		t.Errorf("expected default group=DEFAULT_GROUP, got %q", r.group)
	}
}

// TestRegistrar_Register_HappyPath verifies the Register call goes
// through to the Nacos client and returns nil on success.
func TestRegistrar_Register_HappyPath(t *testing.T) {
	stub := &stubNamingClient{
		regInstanceFunc: func(p vo.RegisterInstanceParam) (bool, error) {
			if p.ServiceName != "svc-x" {
				t.Errorf("expected ServiceName=svc-x, got %q", p.ServiceName)
			}
			return true, nil
		},
	}
	r, _ := NewRegistrar(stub, WithRegistrarGroup("G"), WithRegistrarCluster("C"))
	err := r.Register(context.Background(), registry.Instance{
		Service: "svc-x", IP: "10.0.0.1", Port: 8080,
	})
	if err != nil {
		t.Errorf("Register: %v", err)
	}
}

// TestRegistrar_Register_DefaultWeight covers the inst.Weight<=0 → 10
// fallback.
func TestRegistrar_Register_DefaultWeight(t *testing.T) {
	stub := &stubNamingClient{
		regInstanceFunc: func(p vo.RegisterInstanceParam) (bool, error) {
			if p.Weight != 10 {
				t.Errorf("expected default Weight=10, got %v", p.Weight)
			}
			return true, nil
		},
	}
	r, _ := NewRegistrar(stub)
	err := r.Register(context.Background(), registry.Instance{Service: "s", IP: "1.1.1.1", Port: 1})
	if err != nil {
		t.Errorf("Register: %v", err)
	}
}

// TestRegistrar_Register_CustomWeight covers the non-default Weight
// passthrough.
func TestRegistrar_Register_CustomWeight(t *testing.T) {
	stub := &stubNamingClient{
		regInstanceFunc: func(p vo.RegisterInstanceParam) (bool, error) {
			if p.Weight != 50 {
				t.Errorf("expected Weight=50, got %v", p.Weight)
			}
			return true, nil
		},
	}
	r, _ := NewRegistrar(stub)
	err := r.Register(context.Background(), registry.Instance{Service: "s", IP: "1.1.1.1", Port: 1, Weight: 50})
	if err != nil {
		t.Errorf("Register: %v", err)
	}
}

// TestRegistrar_Register_SDKError covers the error path.
func TestRegistrar_Register_SDKError(t *testing.T) {
	stub := &stubNamingClient{
		regInstanceFunc: func(_ vo.RegisterInstanceParam) (bool, error) {
			return false, errors.New("nacos connection refused")
		},
	}
	r, _ := NewRegistrar(stub)
	err := r.Register(context.Background(), registry.Instance{Service: "s", IP: "1.1.1.1", Port: 1})
	if err == nil {
		t.Fatal("expected error from SDK, got nil")
	}
	if !contains(err.Error(), "register") {
		t.Errorf("expected error to mention 'register', got: %v", err)
	}
}

// TestRegistrar_Register_ReturnedFalse covers the (false, nil) case.
func TestRegistrar_Register_ReturnedFalse(t *testing.T) {
	stub := &stubNamingClient{
		regInstanceFunc: func(_ vo.RegisterInstanceParam) (bool, error) {
			return false, nil
		},
	}
	r, _ := NewRegistrar(stub)
	err := r.Register(context.Background(), registry.Instance{Service: "s", IP: "1.1.1.1", Port: 1})
	if err == nil {
		t.Fatal("expected error when SDK returns false, got nil")
	}
}

// TestRegistrar_Deregister_HappyPath covers the Deregister happy path.
func TestRegistrar_Deregister_HappyPath(t *testing.T) {
	called := false
	stub := &stubNamingClient{
		deregInstanceFunc: func(p vo.DeregisterInstanceParam) (bool, error) {
			called = true
			if p.ServiceName != "svc-y" {
				t.Errorf("expected ServiceName=svc-y, got %q", p.ServiceName)
			}
			return true, nil
		},
	}
	r, _ := NewRegistrar(stub, WithRegistrarGroup("G"), WithRegistrarCluster("C"))
	if err := r.Deregister(context.Background(), registry.Instance{Service: "svc-y", IP: "1.1.1.1", Port: 1}); err != nil {
		t.Errorf("Deregister: %v", err)
	}
	if !called {
		t.Error("expected SDK DeregisterInstance to be called")
	}
}

// TestRegistrar_Deregister_SDKError covers the error path.
func TestRegistrar_Deregister_SDKError(t *testing.T) {
	stub := &stubNamingClient{
		deregInstanceFunc: func(_ vo.DeregisterInstanceParam) (bool, error) {
			return false, errors.New("nacos unavailable")
		},
	}
	r, _ := NewRegistrar(stub)
	err := r.Deregister(context.Background(), registry.Instance{Service: "s", IP: "1.1.1.1", Port: 1})
	if err == nil {
		t.Fatal("expected error from SDK, got nil")
	}
	if !contains(err.Error(), "deregister") {
		t.Errorf("expected error to mention 'deregister', got: %v", err)
	}
}

// --- Discoverer tests ---

// TestDiscoverer_NewWithOptions verifies WithDiscovererGroup/Cluster.
func TestDiscoverer_NewWithOptions(t *testing.T) {
	d, err := NewDiscoverer(&stubNamingClient{},
		WithDiscovererGroup("DG"),
		WithDiscovererClusters("DC1", "DC2"),
	)
	if err != nil {
		t.Fatalf("NewDiscoverer: %v", err)
	}
	if d.group != "DG" {
		t.Errorf("expected group=DG, got %q", d.group)
	}
	if len(d.clusters) != 2 || d.clusters[0] != "DC1" || d.clusters[1] != "DC2" {
		t.Errorf("expected clusters=[DC1, DC2], got %v", d.clusters)
	}
}

// TestDiscoverer_Discover_HappyPath verifies Discover reads from
// the SDK and converts to ekit instances. (Uses SelectInstances, not
// GetService — see discoverer.go.)
func TestDiscoverer_Discover_HappyPath(t *testing.T) {
	stub := &stubNamingClient{
		selectInstancesFunc: func(p vo.SelectInstancesParam) ([]model.Instance, error) {
			if p.ServiceName != "svc-z" {
				t.Errorf("expected ServiceName=svc-z, got %q", p.ServiceName)
			}
			return []model.Instance{
				{Ip: "10.0.0.1", Port: 8080, Weight: 10},
				{Ip: "10.0.0.2", Port: 8081, Weight: 20},
			}, nil
		},
	}
	d, _ := NewDiscoverer(stub)
	got, err := d.Discover(context.Background(), "svc-z")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 instances, got %d", len(got))
	}
	if got[0].IP != "10.0.0.1" || got[1].IP != "10.0.0.2" {
		t.Errorf("unexpected IPs: %v", got)
	}
}

// TestClusterRoundTrip 是 #94 的 round-trip 钉：Register 写入的集群名，
// Discover/DiscoverAll 读回时必须原样带回（stub 充当 Nacos 服务端——
// RegisterInstance 捕获 ClusterName，SelectInstances/SelectAllInstances
// 按捕获值返回 model.Instance，模拟服务端持久化并回读）。
// 此前 toInstance 丢掉 ClusterName，写读两侧不对称，按集群发现
// （WithDiscovererClusters）的结果无法归属集群。
func TestClusterRoundTrip(t *testing.T) {
	// 服务端视角：Register 写入的 ClusterName 被持久化，实例查询按其返回。
	var persistedClusterName string
	stub := &stubNamingClient{
		regInstanceFunc: func(p vo.RegisterInstanceParam) (bool, error) {
			persistedClusterName = p.ClusterName
			return true, nil
		},
		selectInstancesFunc: func(_ vo.SelectInstancesParam) ([]model.Instance, error) {
			return []model.Instance{
				{Ip: "10.0.0.1", Port: 8080, Weight: 10, ClusterName: persistedClusterName},
			}, nil
		},
		selectAllInstancesFunc: func(_ vo.SelectAllInstancesParam) ([]model.Instance, error) {
			return []model.Instance{
				{Ip: "10.0.0.1", Port: 8080, Weight: 10, ClusterName: persistedClusterName},
			}, nil
		},
	}

	r, err := NewRegistrar(stub, WithRegistrarCluster("gray"))
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	inst := registry.Instance{Service: "svc-rt", IP: "10.0.0.1", Port: 8080}
	if regErr := r.Register(context.Background(), inst); regErr != nil {
		t.Fatalf("Register: %v", regErr)
	}
	if persistedClusterName != "gray" {
		t.Fatalf("Register did not persist ClusterName: got %q, want gray", persistedClusterName)
	}

	d, err := NewDiscoverer(stub)
	if err != nil {
		t.Fatalf("NewDiscoverer: %v", err)
	}
	got, err := d.Discover(context.Background(), "svc-rt")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(got) != 1 || got[0].Cluster != "gray" {
		t.Fatalf("Discover round-trip lost Cluster: got %+v, want Cluster=gray", got)
	}

	all, err := d.DiscoverAll(context.Background(), "svc-rt")
	if err != nil {
		t.Fatalf("DiscoverAll: %v", err)
	}
	if len(all) != 1 || all[0].Cluster != "gray" {
		t.Fatalf("DiscoverAll round-trip lost Cluster: got %+v, want Cluster=gray", all)
	}

	// inst.Cluster 非空覆盖 Registrar 默认值的写侧行为也一并钉住。
	persistedClusterName = ""
	if regErr := r.Register(context.Background(), registry.Instance{Service: "svc-rt", IP: "10.0.0.2", Port: 8081, Cluster: "unit-b"}); regErr != nil {
		t.Fatalf("Register with inst.Cluster: %v", regErr)
	}
	if persistedClusterName != "unit-b" {
		t.Errorf("inst.Cluster override not written: got %q, want unit-b", persistedClusterName)
	}
}

// TestDiscoverer_Discover_Error covers the error path.
func TestDiscoverer_Discover_Error(t *testing.T) {
	stub := &stubNamingClient{
		selectInstancesFunc: func(_ vo.SelectInstancesParam) ([]model.Instance, error) {
			return nil, errors.New("network error")
		},
	}
	d, _ := NewDiscoverer(stub)
	_, err := d.Discover(context.Background(), "svc-z")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestDiscoverer_DiscoverAll covers the DiscoverAll path.
func TestDiscoverer_DiscoverAll(t *testing.T) {
	stub := &stubNamingClient{
		selectAllInstancesFunc: func(_ vo.SelectAllInstancesParam) ([]model.Instance, error) {
			return []model.Instance{
				{Ip: "10.0.0.5", Port: 9000, Weight: 10},
			}, nil
		},
	}
	d, _ := NewDiscoverer(stub)
	got, err := d.DiscoverAll(context.Background(), "svc-all")
	if err != nil {
		t.Fatalf("DiscoverAll: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("expected 1 instance, got %d", len(got))
	}
}

// TestDiscoverer_DiscoverAll_Error covers the DiscoverAll error path.
func TestDiscoverer_DiscoverAll_Error(t *testing.T) {
	stub := &stubNamingClient{
		selectAllInstancesFunc: func(_ vo.SelectAllInstancesParam) ([]model.Instance, error) {
			return nil, errors.New("select failed")
		},
	}
	d, _ := NewDiscoverer(stub)
	_, err := d.DiscoverAll(context.Background(), "svc-all")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- Subscriber tests ---

// TestSubscriber_NewWithOptions verifies WithSubscriberGroup.
func TestSubscriber_NewWithOptions(t *testing.T) {
	s, err := NewSubscriber(&stubNamingClient{}, WithSubscriberGroup("SG"))
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if s.group != "SG" {
		t.Errorf("expected group=SG, got %q", s.group)
	}
}

// TestSubscriber_Subscribe verifies Subscribe calls SDK.Subscribe.
func TestSubscriber_Subscribe(t *testing.T) {
	subscribeCalled := false
	stub := &stubNamingClient{
		subscribeFunc: func(p *vo.SubscribeParam) error {
			subscribeCalled = true
			if p.ServiceName != "svc-sub" {
				t.Errorf("expected ServiceName=svc-sub, got %q", p.ServiceName)
			}
			return nil
		},
	}
	s, _ := NewSubscriber(stub)
	err := s.Subscribe(context.Background(), "svc-sub", func(_ []registry.Instance) {})
	if err != nil {
		t.Errorf("Subscribe: %v", err)
	}
	if !subscribeCalled {
		t.Error("expected SDK Subscribe to be called")
	}
}

// TestSubscriber_Subscribe_Error covers the error path.
func TestSubscriber_Subscribe_Error(t *testing.T) {
	stub := &stubNamingClient{
		subscribeFunc: func(_ *vo.SubscribeParam) error {
			return errors.New("subscribe failed")
		},
	}
	s, _ := NewSubscriber(stub)
	err := s.Subscribe(context.Background(), "svc-sub", func(_ []registry.Instance) {})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestSubscriber_Unsubscribe verifies Unsubscribe calls SDK.Unsubscribe.
func TestSubscriber_Unsubscribe(t *testing.T) {
	unsubscribeCalled := false
	stub := &stubNamingClient{
		unsubscribeFunc: func(_ *vo.SubscribeParam) error {
			unsubscribeCalled = true
			return nil
		},
	}
	s, _ := NewSubscriber(stub)
	if err := s.Unsubscribe(context.Background(), "svc-sub"); err != nil {
		t.Errorf("Unsubscribe: %v", err)
	}
	if !unsubscribeCalled {
		t.Error("expected SDK Unsubscribe to be called")
	}
}

// TestSubscriber_Unsubscribe_Error covers the error path.
func TestSubscriber_Unsubscribe_Error(t *testing.T) {
	stub := &stubNamingClient{
		unsubscribeFunc: func(_ *vo.SubscribeParam) error {
			return errors.New("unsubscribe failed")
		},
	}
	s, _ := NewSubscriber(stub)
	if err := s.Unsubscribe(context.Background(), "svc-sub"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- ConfigStore tests ---

// TestConfigStore_GetConfig_HappyPath covers the GetConfig happy path.
func TestConfigStore_GetConfig_HappyPath(t *testing.T) {
	stub := &stubConfigClient{
		getConfigFunc: func(p vo.ConfigParam) (string, error) {
			if p.DataId != "key" {
				t.Errorf("expected DataId=key, got %q", p.DataId)
			}
			return "value", nil
		},
	}
	cs, _ := NewConfigStore(stub)
	got, err := cs.GetConfig(context.Background(), "key", "G")
	if err != nil {
		t.Errorf("GetConfig: %v", err)
	}
	if got != "value" {
		t.Errorf("expected 'value', got %q", got)
	}
}

// TestConfigStore_GetConfig_Error covers the error path.
func TestConfigStore_GetConfig_Error(t *testing.T) {
	stub := &stubConfigClient{
		getConfigFunc: func(_ vo.ConfigParam) (string, error) {
			return "", errors.New("get config failed")
		},
	}
	cs, _ := NewConfigStore(stub)
	if _, err := cs.GetConfig(context.Background(), "key", "G"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestConfigStore_ListenConfig covers the ListenConfig path.
func TestConfigStore_ListenConfig(t *testing.T) {
	called := false
	stub := &stubConfigClient{
		listenConfigFunc: func(_ vo.ConfigParam) error {
			called = true
			return nil
		},
	}
	cs, _ := NewConfigStore(stub)
	if err := cs.ListenConfig(context.Background(), "k", "G", func(_ string) {}); err != nil {
		t.Errorf("ListenConfig: %v", err)
	}
	if !called {
		t.Error("expected SDK ListenConfig to be called")
	}
}

// TestConfigStore_StopListenConfig covers the StopListenConfig path
// (delegates to SDK CancelListenConfig).
func TestConfigStore_StopListenConfig(t *testing.T) {
	called := false
	stub := &stubConfigClient{
		cancelListenConfigFunc: func(_ vo.ConfigParam) error {
			called = true
			return nil
		},
	}
	cs, _ := NewConfigStore(stub)
	if err := cs.StopListenConfig(context.Background(), "k", "G"); err != nil {
		t.Errorf("StopListenConfig: %v", err)
	}
	if !called {
		t.Error("expected SDK CancelListenConfig to be called")
	}
}

// contains is a small strings.Contains shim.
func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || (len(s) > 0 && len(sub) > 0 && indexOfSub(s, sub) >= 0))
}

// --- ctx 感知测试（#65）---
//
// SDK API 不接收 context，invokeWithCtx 以 goroutine+select 竞速兜底：
// ctx 已取消 → 快速失败且不触碰 SDK；SDK 阻塞中 ctx 取消 → 立即返回
// context.Canceled（关停期反注册的超时预算生效）。同步一律用 channel，
// 不 sleep。

// TestRegistrar_Register_PreCanceledCtx 预取消的 ctx 必须快速失败，
// 且不发起 SDK 调用（此前 ctx 被完全忽略，注册照常进行）。
func TestRegistrar_Register_PreCanceledCtx(t *testing.T) {
	called := make(chan vo.RegisterInstanceParam, 1)
	stub := &stubNamingClient{
		regInstanceFunc: func(p vo.RegisterInstanceParam) (bool, error) {
			called <- p
			return true, nil
		},
	}
	r, _ := NewRegistrar(stub)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := r.Register(ctx, registry.Instance{Service: "s", IP: "1.1.1.1", Port: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Register with pre-canceled ctx: err = %v, want context.Canceled", err)
	}
	select {
	case p := <-called:
		t.Fatalf("SDK RegisterInstance must not be called, got call for %q", p.ServiceName)
	default:
	}
}

// TestRegistrar_Deregister_PreCanceledCtx 同上，覆盖 Deregister——
// 关停期 ctx 可能已先一步取消，此时必须立即返回而不是再撞一次 SDK。
func TestRegistrar_Deregister_PreCanceledCtx(t *testing.T) {
	called := make(chan vo.DeregisterInstanceParam, 1)
	stub := &stubNamingClient{
		deregInstanceFunc: func(p vo.DeregisterInstanceParam) (bool, error) {
			called <- p
			return true, nil
		},
	}
	r, _ := NewRegistrar(stub)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := r.Deregister(ctx, registry.Instance{Service: "s", IP: "1.1.1.1", Port: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Deregister with pre-canceled ctx: err = %v, want context.Canceled", err)
	}
	select {
	case p := <-called:
		t.Fatalf("SDK DeregisterInstance must not be called, got call for %q", p.ServiceName)
	default:
	}
}

// TestRegistrar_Deregister_CancelDuringSDKCall 是 #65 的核心场景：
// 关停期 Nacos 不可达、SDK 内部阻塞重试时，closer 的超时预算必须生效——
// ctx 取消后 Deregister 立即返回 context.Canceled，而不是等 SDK 跑完。
func TestRegistrar_Deregister_CancelDuringSDKCall(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	stub := &stubNamingClient{
		deregInstanceFunc: func(_ vo.DeregisterInstanceParam) (bool, error) {
			close(entered)
			<-release // 模拟 SDK 内部阻塞（不可达重试）
			return true, nil
		},
	}
	r, _ := NewRegistrar(stub)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Deregister(ctx, registry.Instance{Service: "s", IP: "1.1.1.1", Port: 1})
	}()
	<-entered // SDK 调用已开始（channel 同步）
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Deregister after cancel: err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Deregister blocked on SDK call after ctx cancel — shutdown budget not enforced")
	}

	// 放行后台 SDK 调用（无法中断，只会在后台完成），goroutine 不泄漏。
	close(release)
}

// TestRegistrar_Register_CancelDuringSDKCall 同型覆盖 Register。
func TestRegistrar_Register_CancelDuringSDKCall(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	stub := &stubNamingClient{
		regInstanceFunc: func(_ vo.RegisterInstanceParam) (bool, error) {
			close(entered)
			<-release
			return true, nil
		},
	}
	r, _ := NewRegistrar(stub)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Register(ctx, registry.Instance{Service: "s", IP: "1.1.1.1", Port: 1})
	}()
	<-entered
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Register after cancel: err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Register blocked on SDK call after ctx cancel")
	}
	close(release)
}

// TestRegistrar_TimeoutCtxStillSucceeds 带超时预算但正常完成的调用不受影响
// （预算充足时返回 SDK 的真实结果）。
func TestRegistrar_TimeoutCtxStillSucceeds(t *testing.T) {
	stub := &stubNamingClient{
		regInstanceFunc: func(_ vo.RegisterInstanceParam) (bool, error) {
			return true, nil
		},
	}
	r, _ := NewRegistrar(stub)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Register(ctx, registry.Instance{Service: "s", IP: "1.1.1.1", Port: 1}); err != nil {
		t.Errorf("Register with ample timeout: %v", err)
	}
}

// TestDiscoverer_Discover_PreCanceledCtx 发现路径的预取消快速失败。
func TestDiscoverer_Discover_PreCanceledCtx(t *testing.T) {
	called := make(chan vo.SelectInstancesParam, 1)
	stub := &stubNamingClient{
		selectInstancesFunc: func(p vo.SelectInstancesParam) ([]model.Instance, error) {
			called <- p
			return nil, nil
		},
	}
	d, _ := NewDiscoverer(stub)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := d.Discover(ctx, "svc-z")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Discover with pre-canceled ctx: err = %v, want context.Canceled", err)
	}
	select {
	case p := <-called:
		t.Fatalf("SDK SelectInstances must not be called, got call for %q", p.ServiceName)
	default:
	}

	// DiscoverAll 同型
	calledAll := make(chan vo.SelectAllInstancesParam, 1)
	stubAll := &stubNamingClient{
		selectAllInstancesFunc: func(p vo.SelectAllInstancesParam) ([]model.Instance, error) {
			calledAll <- p
			return nil, nil
		},
	}
	dAll, _ := NewDiscoverer(stubAll)
	if _, err := dAll.DiscoverAll(ctx, "svc-z"); !errors.Is(err, context.Canceled) {
		t.Fatalf("DiscoverAll with pre-canceled ctx: err = %v, want context.Canceled", err)
	}
	select {
	case p := <-calledAll:
		t.Fatalf("SDK SelectAllInstances must not be called, got call for %q", p.ServiceName)
	default:
	}
}

// TestDiscoverer_Discover_CancelDuringSDKCall 发现路径：SDK 阻塞中取消
// 必须立即返回。
func TestDiscoverer_Discover_CancelDuringSDKCall(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	stub := &stubNamingClient{
		selectInstancesFunc: func(_ vo.SelectInstancesParam) ([]model.Instance, error) {
			close(entered)
			<-release
			return nil, nil
		},
	}
	d, _ := NewDiscoverer(stub)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := d.Discover(ctx, "svc-z")
		errCh <- err
	}()
	<-entered
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Discover after cancel: err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Discover blocked on SDK call after ctx cancel")
	}
	close(release)
}

// TestInvokeWithCtx_PanicBecomesError SDK panic 必须转为 error 返回
// （不打死进程、不让 select 悬挂）。
func TestInvokeWithCtx_PanicBecomesError(t *testing.T) {
	_, err := invokeWithCtx(context.Background(), func() (bool, error) {
		panic("sdk exploded")
	})
	if err == nil {
		t.Fatal("expected error from panicked SDK call, got nil")
	}
	if !contains(err.Error(), "panicked") {
		t.Errorf("err = %v, want it to mention panic", err)
	}
}

func indexOfSub(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// captureSubscribeCallbacks 从 stub 的 subscribeFunc 里按服务名收集注册的
// 推送回调，测试用 (*cb)(nil, err) 模拟 SDK 推送。
func captureSubscribeCallbacks(stub *stubNamingClient) func(service string) *func([]model.Instance, error) {
	var mu sync.Mutex
	cbs := make(map[string][]*func([]model.Instance, error))
	stub.subscribeFunc = func(p *vo.SubscribeParam) error {
		mu.Lock()
		defer mu.Unlock()
		cbs[p.ServiceName] = append(cbs[p.ServiceName], &p.SubscribeCallback)
		return nil
	}
	return func(service string) *func([]model.Instance, error) {
		mu.Lock()
		defer mu.Unlock()
		if got := cbs[service]; len(got) > 0 {
			return got[0]
		}
		return nil
	}
}

// --- #142 回归钉（LastErr 按服务分账）---

// TestSubscriber_LastErr_PerServiceIsolation 是 #142 的核心回归钉：服务 A
// 推送失败置位后，服务 B 的成功推送**不得**清掉 A 的账——此前 lastErr 是
// Subscriber 全局单值，A 的陈旧视图被 B 的一次成功推送伪装成「健康」。
//
// ⚠️ 覆盖面说明（#161）：本钉（与下一条聚合钉）用测试桩手动造非 nil err，
// 只覆盖**账本逻辑**（分账/聚合/清账），不覆盖**送达面**——钉死的
// nacos-sdk-go v2.3.6 生产路径对回调恒传 err=nil（唯一调用点 subscribe_
// callback_wrapper.go:59），账本在真实 SDK 上不会被置位；「视图过期」的
// 真实可达信号源见 TestSubscriber_LastPushFor_RealSignalSource。
func TestSubscriber_LastErr_PerServiceIsolation(t *testing.T) {
	stub := &stubNamingClient{}
	cbOf := captureSubscribeCallbacks(stub)
	s, err := NewSubscriber(stub)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	for _, svc := range []string{"svc-a", "svc-b"} {
		if err := s.Subscribe(context.Background(), svc, func([]registry.Instance) {}); err != nil {
			t.Fatalf("Subscribe %s: %v", svc, err)
		}
	}

	// A 推送失败、B 推送成功：A 必须仍报「视图可能过期」，B 必须新鲜。
	if cb := cbOf("svc-a"); cb == nil {
		t.Fatal("svc-a callback not captured")
	} else {
		(*cb)(nil, errors.New("push A failed"))
	}
	if cb := cbOf("svc-b"); cb == nil {
		t.Fatal("svc-b callback not captured")
	} else {
		(*cb)(nil, nil)
	}

	errA := s.LastErrFor("svc-a")
	if errA == nil || !strings.Contains(errA.Error(), "svc-a") || !strings.Contains(errA.Error(), "push A failed") {
		t.Fatalf("LastErrFor(svc-a) = %v, want error mentioning svc-a/push A failed", errA)
	}
	if errB := s.LastErrFor("svc-b"); errB != nil {
		t.Fatalf("LastErrFor(svc-b) = %v, want nil（B 的成功不得受 A 的失败影响）", errB)
	}
	agg := s.LastErr()
	if agg == nil || !strings.Contains(agg.Error(), "svc-a") {
		t.Fatalf("LastErr() = %v, want aggregate mentioning svc-a", agg)
	}

	// A 自己的下一次成功推送清 A 自己的账。
	if cb := cbOf("svc-a"); cb != nil {
		(*cb)(nil, nil)
	}
	if errA := s.LastErrFor("svc-a"); errA != nil {
		t.Fatalf("LastErrFor(svc-a) after own success = %v, want nil", errA)
	}
	if agg := s.LastErr(); agg != nil {
		t.Fatalf("LastErr() after all services fresh = %v, want nil", agg)
	}
}

// TestSubscriber_LastErr_AggregateJoinsServices 钉住聚合面：多个服务同时
// 失败时 LastErr() 聚合全部（errors.Join），且按服务名字典序确定输出。
func TestSubscriber_LastErr_AggregateJoinsServices(t *testing.T) {
	stub := &stubNamingClient{}
	cbOf := captureSubscribeCallbacks(stub)
	s, _ := NewSubscriber(stub)
	for _, svc := range []string{"svc-a", "svc-b"} {
		if err := s.Subscribe(context.Background(), svc, func([]registry.Instance) {}); err != nil {
			t.Fatalf("Subscribe %s: %v", svc, err)
		}
	}
	if cb := cbOf("svc-a"); cb != nil {
		(*cb)(nil, errors.New("boom-a"))
	}
	if cb := cbOf("svc-b"); cb != nil {
		(*cb)(nil, errors.New("boom-b"))
	}
	agg := s.LastErr()
	if agg == nil {
		t.Fatal("LastErr() = nil, want aggregate of both failures")
	}
	ia, ib := strings.Index(agg.Error(), "svc-a"), strings.Index(agg.Error(), "svc-b")
	if ia < 0 || ib < 0 {
		t.Fatalf("LastErr() = %v, want both svc-a and svc-b mentioned", agg)
	}
	if ia > ib {
		t.Errorf("LastErr() 输出顺序不稳定：want svc-a before svc-b（字典序）, got %v", agg)
	}
}

// TestSubscriber_LastPushFor_RealSignalSource 回归钉（#161）：LastPushFor 是
// 「视图过期」在钉死 SDK（v2.3.6）上唯一可达的真实信号源——成功回调记时刻、
// 错误回调不记（推送没成功，视图仍是旧的）、退订清账、从未推送报 false。
func TestSubscriber_LastPushFor_RealSignalSource(t *testing.T) {
	stub := &stubNamingClient{}
	cbOf := captureSubscribeCallbacks(stub)
	s, _ := NewSubscriber(stub)
	if err := s.Subscribe(context.Background(), "svc-a", func([]registry.Instance) {}); err != nil {
		t.Fatalf("Subscribe svc-a: %v", err)
	}

	// 从未成功推送：false。
	if _, ok := s.LastPushFor("svc-a"); ok {
		t.Fatal("★ 从未推送时 LastPushFor 必须报 false")
	}

	// 错误回调（测试桩造数）：err 账目置位，但推送时刻**不得**记录——
	// 推送没成功，视图仍是旧的（#161 语义：只记成功回调时刻）。
	if cb := cbOf("svc-a"); cb == nil {
		t.Fatal("svc-a callback not captured")
	} else {
		(*cb)(nil, errors.New("push failed"))
	}
	if errA := s.LastErrFor("svc-a"); errA == nil {
		t.Fatal("错误回调必须置 err 账目（账本逻辑面）")
	}
	if _, ok := s.LastPushFor("svc-a"); ok {
		t.Fatal("★ 错误回调不得记录推送时刻（视图仍是旧的，#161）")
	}

	// 成功回调：时刻记录（真实可达信号）。
	before := time.Now()
	if cb := cbOf("svc-a"); cb != nil {
		(*cb)(nil, nil)
	}
	ts, ok := s.LastPushFor("svc-a")
	if !ok {
		t.Fatal("★ 成功回调后 LastPushFor 必须报 true")
	}
	if ts.Before(before) || ts.After(time.Now()) {
		t.Fatalf("★ 记录的时刻必须落在本次回调附近: %v", ts)
	}
	// 成功推送同时清 err 账目（#142 原语义不回退）。
	if errA := s.LastErrFor("svc-a"); errA != nil {
		t.Fatalf("成功推送必须清 err 账目: %v", errA)
	}

	// 退订清账（与 lastErrs 同口径）：LastPushFor 不再报「曾经推送过」。
	if err := s.Unsubscribe(context.Background(), "svc-a"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if _, ok := s.LastPushFor("svc-a"); ok {
		t.Fatal("★ 退订后 LastPushFor 必须报 false（#161 时刻账目同步清）")
	}
}

// TestSubscriber_LastErr_ClearedOnUnsubscribe 钉住退订清账：全部退订成功后
// 该服务的错误账目必须移除（否则 unclean 推送留下的错误在聚合面永久滞留，
// 健康检查对已退订服务误报「视图过期」，#142）；退订失败（订阅仍在）则保留。
func TestSubscriber_LastErr_ClearedOnUnsubscribe(t *testing.T) {
	stub := &stubNamingClient{}
	cbOf := captureSubscribeCallbacks(stub)
	s, _ := NewSubscriber(stub)
	if err := s.Subscribe(context.Background(), "svc-a", func([]registry.Instance) {}); err != nil {
		t.Fatalf("Subscribe svc-a: %v", err)
	}
	if err := s.Subscribe(context.Background(), "svc-b", func([]registry.Instance) {}); err != nil {
		t.Fatalf("Subscribe svc-b: %v", err)
	}
	if cb := cbOf("svc-a"); cb != nil {
		(*cb)(nil, errors.New("boom-a"))
	}
	if cb := cbOf("svc-b"); cb != nil {
		(*cb)(nil, errors.New("boom-b"))
	}

	// svc-a 退订成功 ⇒ 清账。
	if err := s.Unsubscribe(context.Background(), "svc-a"); err != nil {
		t.Fatalf("Unsubscribe svc-a: %v", err)
	}
	if err := s.LastErrFor("svc-a"); err != nil {
		t.Fatalf("LastErrFor(svc-a) after unsubscribe = %v, want nil", err)
	}
	// svc-b 退订失败（订阅仍在）⇒ 保留账目。
	stub.unsubscribeFunc = func(*vo.SubscribeParam) error { return errors.New("nacos unavailable") }
	if err := s.Unsubscribe(context.Background(), "svc-b"); err == nil {
		t.Fatal("expected Unsubscribe svc-b error, got nil")
	}
	if err := s.LastErrFor("svc-b"); err == nil {
		t.Fatal("LastErrFor(svc-b) after failed unsubscribe = nil, want retained error")
	}
}

// --- #143 回归钉（ConfigStore 的 ctx 纪律）---

// TestConfigStore_GetConfig_PreCanceledCtx：预取消的 ctx 必须快速失败且不
// 发起 SDK 调用——此前 ctx 被完全忽略，消费方（nns 逐服务串行拉配置）的
// 取消/超时预算对 config 路径无效（#143）。
func TestConfigStore_GetConfig_PreCanceledCtx(t *testing.T) {
	called := make(chan vo.ConfigParam, 1)
	stub := &stubConfigClient{
		getConfigFunc: func(p vo.ConfigParam) (string, error) {
			called <- p
			return "v", nil
		},
	}
	cs, _ := NewConfigStore(stub)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := cs.GetConfig(ctx, "k", "G"); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetConfig with pre-canceled ctx: err = %v, want context.Canceled", err)
	}
	select {
	case p := <-called:
		t.Fatalf("SDK GetConfig must not be called, got call for %q", p.DataId)
	default:
	}
}

// TestConfigStore_GetConfig_CancelDuringSDKCall：SDK 调用进行中 ctx 取消必须
// 立即返回（与 Registrar/Discoverer 的 #65 竞速语义同款，#143）。
func TestConfigStore_GetConfig_CancelDuringSDKCall(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	stub := &stubConfigClient{
		getConfigFunc: func(_ vo.ConfigParam) (string, error) {
			close(entered)
			<-release // 模拟 SDK 内部阻塞
			return "v", nil
		},
	}
	cs, _ := NewConfigStore(stub)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := cs.GetConfig(ctx, "k", "G")
		errCh <- err
	}()
	<-entered
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetConfig after cancel: err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GetConfig blocked on SDK call after ctx cancel")
	}
	close(release) // 放行后台 SDK 调用，goroutine 不泄漏
}

// TestConfigStore_StopListenConfig_PreCanceledCtx：同款覆盖 StopListenConfig。
func TestConfigStore_StopListenConfig_PreCanceledCtx(t *testing.T) {
	called := make(chan vo.ConfigParam, 1)
	stub := &stubConfigClient{
		cancelListenConfigFunc: func(p vo.ConfigParam) error {
			called <- p
			return nil
		},
	}
	cs, _ := NewConfigStore(stub)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := cs.StopListenConfig(ctx, "k", "G"); !errors.Is(err, context.Canceled) {
		t.Fatalf("StopListenConfig with pre-canceled ctx: err = %v, want context.Canceled", err)
	}
	select {
	case p := <-called:
		t.Fatalf("SDK CancelListenConfig must not be called, got call for %q", p.DataId)
	default:
	}
}

// TestConfigStore_ListenConfig_PreCanceledCtx：ListenConfig 注册本身做进入前
// 预检（#143）——ctx 已取消时不发起 SDK 注册，onChange 不受 ctx 管理（取消
// 靠 StopListenConfig，godoc 钉明）。
func TestConfigStore_ListenConfig_PreCanceledCtx(t *testing.T) {
	called := make(chan vo.ConfigParam, 1)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			called <- p
			return nil
		},
	}
	cs, _ := NewConfigStore(stub)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := cs.ListenConfig(ctx, "k", "G", func(string) {}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListenConfig with pre-canceled ctx: err = %v, want context.Canceled", err)
	}
	select {
	case p := <-called:
		t.Fatalf("SDK ListenConfig must not be called, got call for %q", p.DataId)
	default:
	}
}

// --- #95/#96 回归钉（订阅指针身份 + 回调 panic 隔离）---
//
// sdkLikeNamingClient 单元级复刻 nacos-sdk-go v2 的回调注册/移除机制
// （naming_cache/subscribe_callback.go + naming_client.go）：
//   - Subscribe 登记 &param.SubscribeCallback 指针（NewSubscribeCallback-
//     FuncWrapper 的 CallbackFunc 字段）；
//   - Unsubscribe 仅移除 **指针完全相同** 的 wrapper（RemoveCallbackFunc
//     按 CallbackFunc 指针身份比较）——传新构造的 param 永远匹配不上；
//   - 剩余 wrapper 为空（IsSubscribed=false）才触发 gRPC 层退订
//     （serviceProxy.Unsubscribe）。
//
// 此前 stubNamingClient 只记录「Unsubscribe 被以什么参数调用」，
// 完全不走 SDK 的注册/移除机制，Unsubscribe 静默 no-op 的缺陷测不出来。
type sdkLikeNamingClient struct {
	stubNamingClient

	mu         sync.Mutex
	registered []*func([]model.Instance, error) // 已登记的回调指针（注册序）
	grpcUnsub  int                              // gRPC 层退订触发次数
}

func (c *sdkLikeNamingClient) Subscribe(p *vo.SubscribeParam) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registered = append(c.registered, &p.SubscribeCallback)
	return nil
}

func (c *sdkLikeNamingClient) Unsubscribe(p *vo.SubscribeParam) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cb := &p.SubscribeCallback
	var remain []*func([]model.Instance, error)
	for _, item := range c.registered {
		if item != cb {
			remain = append(remain, item)
		}
	}
	c.registered = remain
	if len(remain) == 0 {
		c.grpcUnsub++
	}
	return nil
}

// dispatch 模拟 SDK 推送：向全部未移除的回调派发一次实例变更。
func (c *sdkLikeNamingClient) dispatch() {
	c.mu.Lock()
	cbs := make([]*func([]model.Instance, error), len(c.registered))
	copy(cbs, c.registered)
	c.mu.Unlock()
	for _, cb := range cbs {
		(*cb)(nil, nil)
	}
}

// TestSubscriber_Unsubscribe_RemovesRegisteredCallback 是 #95 的回归钉：
// Unsubscribe 必须复用注册时的同一 param 指针——否则 SDK 按指针身份
// 移除永远匹配不上，回调 ghost 不死（每次推送仍被调用），且 IsSubscribed
// 恒 true 连 gRPC 层退订也被跳过。钉住三个断言：①退订后推送不再触达
// 回调；②SDK 侧 wrapper 清空；③gRPC 层退订被触发。
func TestSubscriber_Unsubscribe_RemovesRegisteredCallback(t *testing.T) {
	sdk := &sdkLikeNamingClient{}
	s, err := NewSubscriber(sdk)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	calls := 0
	if err := s.Subscribe(context.Background(), "order", func([]registry.Instance) {
		calls++
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	sdk.dispatch()
	if calls != 1 {
		t.Fatalf("push before unsubscribe: calls = %d, want 1", calls)
	}

	if err := s.Unsubscribe(context.Background(), "order"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	sdk.dispatch() // ghost push：退订后的推送不得再触达回调
	if calls != 1 {
		t.Fatalf("push after unsubscribe: calls = %d, want 1 (callback deregistration is a silent no-op, #95)", calls)
	}
	sdk.mu.Lock()
	remaining, grpcUnsub := len(sdk.registered), sdk.grpcUnsub
	sdk.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("SDK wrapper remaining after unsubscribe = %d, want 0 (param pointer identity broken, #95)", remaining)
	}
	if grpcUnsub != 1 {
		t.Fatalf("gRPC-level unsubscribe fired %d times, want 1 (IsSubscribed stuck true skips it, #95)", grpcUnsub)
	}
}

// TestSubscriber_Unsubscribe_DoubleSubscribeRemovesAll 钉住重复订阅场景：
// 同一服务 Subscribe 两次后 Unsubscribe 一次，两个 wrapper 都必须被移除。
func TestSubscriber_Unsubscribe_DoubleSubscribeRemovesAll(t *testing.T) {
	sdk := &sdkLikeNamingClient{}
	s, _ := NewSubscriber(sdk)
	cb := func([]registry.Instance) {}
	if err := s.Subscribe(context.Background(), "svc", cb); err != nil {
		t.Fatalf("Subscribe #1: %v", err)
	}
	if err := s.Subscribe(context.Background(), "svc", cb); err != nil {
		t.Fatalf("Subscribe #2: %v", err)
	}
	sdk.mu.Lock()
	registered := len(sdk.registered)
	sdk.mu.Unlock()
	if registered != 2 {
		t.Fatalf("registered wrappers after double Subscribe = %d, want 2", registered)
	}
	if err := s.Unsubscribe(context.Background(), "svc"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	sdk.mu.Lock()
	remaining, grpcUnsub := len(sdk.registered), sdk.grpcUnsub
	sdk.mu.Unlock()
	if remaining != 0 || grpcUnsub != 1 {
		t.Fatalf("after unsubscribe: remaining=%d grpcUnsub=%d, want 0/1", remaining, grpcUnsub)
	}
}

// TestSubscriber_Unsubscribe_FailureReusesParamPointer 钉住退订失败重试：
// 失败的 param 必须重新入账，重试时仍复用同一指针——换新指针将再次
// 变成静默 no-op（#95 的原始缺陷形态）。
func TestSubscriber_Unsubscribe_FailureReusesParamPointer(t *testing.T) {
	var seen []*vo.SubscribeParam
	fail := true
	stub := &stubNamingClient{
		unsubscribeFunc: func(p *vo.SubscribeParam) error {
			seen = append(seen, p)
			if fail {
				return errors.New("nacos unavailable")
			}
			return nil
		},
	}
	s, _ := NewSubscriber(stub)
	if err := s.Subscribe(context.Background(), "svc", func([]registry.Instance) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := s.Unsubscribe(context.Background(), "svc"); err == nil {
		t.Fatal("expected Unsubscribe error on first attempt, got nil")
	}
	fail = false
	if err := s.Unsubscribe(context.Background(), "svc"); err != nil {
		t.Fatalf("Unsubscribe retry: %v", err)
	}
	if len(seen) != 2 || seen[0] != seen[1] {
		t.Fatalf("retry must reuse the same param pointer (SDK removes by pointer identity, #95): got %d calls, same=%v",
			len(seen), len(seen) == 2 && seen[0] == seen[1])
	}
}

// TestSubscriber_Subscribe_CallbackPanicIsolated 是 #96 的回归钉：
// 消费方回调 panic 必须被就地隔离（SDK naming 推送沿 gRPC 收包
// goroutine 直通派发，全程无 recover，裸回调会打死整个进程）。
// 若未隔离，本测试 goroutine 直接炸穿、测试失败。
func TestSubscriber_Subscribe_CallbackPanicIsolated(t *testing.T) {
	var sdkCb *func([]model.Instance, error)
	stub := &stubNamingClient{
		subscribeFunc: func(p *vo.SubscribeParam) error {
			sdkCb = &p.SubscribeCallback
			return nil
		},
	}
	s, _ := NewSubscriber(stub)
	if err := s.Subscribe(context.Background(), "svc", func([]registry.Instance) {
		panic("consumer callback exploded")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if sdkCb == nil {
		t.Fatal("SDK Subscribe was not called")
	}
	// 模拟 SDK 派发链直调回调（naming_cache notifyListener → cb）。
	(*sdkCb)(nil, nil)
}

// TestConfigStore_ListenConfig_CallbackPanicIsolated 是 #125 的回归钉：
// 消费方 onChange 的 panic 必须被就地隔离——SDK config 侧在独立 goroutine
// 上裸调 OnChange（config_client.go:103 go listener(...) 全程无 recover），
// 回调 panic 会沿该 goroutine 上抛打死宿主进程。
// 断言：回调 panic 后宿主（测试进程）仍存活，且后续回调仍继续投递。
func TestConfigStore_ListenConfig_CallbackPanicIsolated(t *testing.T) {
	var fired func(namespace, group, dataId, data string)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired = p.OnChange
			return nil
		},
	}
	cs, _ := NewConfigStore(stub)
	var got []string
	err := cs.ListenConfig(context.Background(), "k", "G", func(data string) {
		if data == "boom" {
			panic("consumer callback exploded")
		}
		got = append(got, data)
	})
	if err != nil {
		t.Fatalf("ListenConfig: %v", err)
	}
	if fired == nil {
		t.Fatal("SDK ListenConfig was not called")
	}

	// 模拟 SDK 派发 goroutine 直调 OnChange：panic 不得逃逸出闭包。
	var escaped any
	func() {
		defer func() { escaped = recover() }()
		fired("ns", "G", "k", "boom")
	}()
	if escaped != nil {
		t.Errorf("onChange 的 panic 逃逸出 SDK 派发闭包（未隔离，会沿派发 goroutine 打死宿主进程）: %v", escaped)
	}

	// 回调 panic 后宿主仍存活且后续回调仍继续投递。
	fired("ns", "G", "k", "after-panic")
	if len(got) != 1 || got[0] != "after-panic" {
		t.Errorf("panic 后续回调未继续投递: got=%v, want=[after-panic]", got)
	}
}

// TestConfigStore_ListenConfig_SerialDelivery 是 #127 的回归钉：SDK 对同一
// dataID 每次推送各起一个新 goroutine 裸调 OnChange（config_client.go:103
// `go listener(...)`），两次连更会并发、乱序进入消费方 onChange——与 etcdx
// ConfigStore「回调在监听专属 goroutine 中串行执行（不会并发调用 onChange）」
// 的承诺分裂，快速连更可能旧配置最后生效。
// 用可控同步原语造确定性复现：第一次投递（旧配置 v2）卡在闸门上时发起第二次
// 推送（新配置 v3）——串行实现下 v3 必须排队到 v2 完成之后，最终生效值为 v3。
func TestConfigStore_ListenConfig_SerialDelivery(t *testing.T) {
	var fired func(namespace, group, dataId, data string)
	stub := &stubConfigClient{
		listenConfigFunc: func(p vo.ConfigParam) error {
			fired = p.OnChange
			return nil
		},
	}
	cs, _ := NewConfigStore(stub)

	var seq, inFlight, overlap int32
	var applied string
	firstInside := make(chan struct{})
	releaseFirst := make(chan struct{})
	err := cs.ListenConfig(context.Background(), "k", "G", func(data string) {
		if atomic.AddInt32(&inFlight, 1) > 1 {
			atomic.StoreInt32(&overlap, 1)
		}
		if atomic.AddInt32(&seq, 1) == 1 {
			close(firstInside)
			<-releaseFirst
		}
		applied = data
		atomic.AddInt32(&inFlight, -1)
	})
	if err != nil {
		t.Fatalf("ListenConfig: %v", err)
	}
	if fired == nil {
		t.Fatal("SDK ListenConfig was not called")
	}

	// 第一次推送（旧配置 v2）：模拟 SDK 起 goroutine 派发，先进入回调并卡住。
	go fired("ns", "G", "k", "v2")
	<-firstInside

	// 第二次推送（新配置 v3）：SDK 语义 = 再起一个 goroutine 裸调 OnChange。
	p2Done := make(chan struct{})
	go func() {
		fired("ns", "G", "k", "v3")
		close(p2Done)
	}()
	select {
	case <-p2Done:
		t.Error("onChange 被并发/乱序投递：第一次回调未完成时第二次已投递（与 etcdx「串行投递」承诺分裂，#127）")
	case <-time.After(500 * time.Millisecond):
		// v3 排队到 v2 之后（串行）——符合预期
	}
	close(releaseFirst)
	<-p2Done

	if atomic.LoadInt32(&overlap) != 0 {
		t.Error("onChange 被并发调用（inFlight>1），必须串行投递（#127）")
	}
	if applied != "v3" {
		t.Errorf("最终生效配置 = %q, want %q（旧配置最后生效 = #127 复发）", applied, "v3")
	}
}
