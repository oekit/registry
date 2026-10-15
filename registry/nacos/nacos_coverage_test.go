package nacos

import (
	"context"
	"errors"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/oekit/registry/registry"
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

func indexOfSub(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
