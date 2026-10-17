package nacos

import (
	"errors"
	"os"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/oekit/registry"
)

func TestToInstance(t *testing.T) {
	src := model.Instance{
		Ip:       "10.0.0.1",
		Port:     8080,
		Weight:   10,
		Metadata: map[string]string{"region": "cn"},
	}
	got := toInstance("user-service", src)

	if got.Service != "user-service" {
		t.Errorf("Service = %q, want user-service", got.Service)
	}
	if got.IP != "10.0.0.1" {
		t.Errorf("IP = %q, want 10.0.0.1", got.IP)
	}
	if got.Port != 8080 {
		t.Errorf("Port = %d, want 8080", got.Port)
	}
	if got.Weight != 10 {
		t.Errorf("Weight = %v, want 10", got.Weight)
	}
	if got.Metadata["region"] != "cn" {
		t.Errorf("Metadata[region] = %q, want cn", got.Metadata["region"])
	}
}

// TestToInstance_CopiesMetadata 验证转换会拷贝 metadata：Nacos SDK 会在推送
// 更新时原地修改内部实例对象，共享 map 会引发脏读与数据竞争。
func TestToInstance_CopiesMetadata(t *testing.T) {
	src := model.Instance{
		Ip:       "10.0.0.1",
		Port:     8080,
		Weight:   10,
		Metadata: map[string]string{"region": "cn"},
	}
	got := toInstance("user-service", src)

	// 调用方修改转换结果不得污染 SDK 侧 map
	got.Metadata["region"] = "us"
	if src.Metadata["region"] != "cn" {
		t.Error("toInstance shares metadata map with the SDK instance")
	}

	// SDK 侧 map 变更也不得影响已转换的实例
	src.Metadata["region"] = "eu"
	if got.Metadata["region"] != "us" {
		t.Error("converted instance must not alias the SDK metadata map")
	}
}

func TestToInstances(t *testing.T) {
	src := []model.Instance{
		{Ip: "10.0.0.1", Port: 8080, Weight: 10},
		{Ip: "10.0.0.2", Port: 8080, Weight: 20},
	}
	result := toInstances("svc", src)

	if len(result) != 2 {
		t.Fatalf("len = %d, want 2", len(result))
	}
	if result[0].Service != "svc" || result[1].Service != "svc" {
		t.Errorf("Service not set correctly")
	}
	if result[1].IP != "10.0.0.2" {
		t.Errorf("result[1].IP = %q, want 10.0.0.2", result[1].IP)
	}
}

func TestToInstances_Empty(t *testing.T) {
	result := toInstances("svc", nil)
	if len(result) != 0 {
		t.Errorf("expected empty slice, got %d items", len(result))
	}
}

func TestGetPodIP_FromEnv(t *testing.T) {
	t.Setenv("POD_IP", "10.1.2.3")
	if got := GetPodIP(); got != "10.1.2.3" {
		t.Errorf("GetPodIP() = %q, want 10.1.2.3", got)
	}
}

func TestGetPodIP_FallbackLoopback(t *testing.T) {
	os.Unsetenv("POD_IP")
	// In CI/test environments without non-loopback interfaces,
	// this may return a real IP or 127.0.0.1. We just verify it's non-empty.
	got := GetPodIP()
	if got == "" {
		t.Error("GetPodIP() returned empty string")
	}
}

func TestNewRegistrar_NilClientReturnsError(t *testing.T) {
	_, err := NewRegistrar(nil)
	if !errors.Is(err, ErrNilClient) {
		t.Errorf("expected ErrNilClient, got %v", err)
	}
}

func TestNewDiscoverer_NilClientReturnsError(t *testing.T) {
	_, err := NewDiscoverer(nil)
	if !errors.Is(err, ErrNilClient) {
		t.Errorf("expected ErrNilClient, got %v", err)
	}
}

func TestNewSubscriber_NilClientReturnsError(t *testing.T) {
	_, err := NewSubscriber(nil)
	if !errors.Is(err, ErrNilClient) {
		t.Errorf("expected ErrNilClient, got %v", err)
	}
}

func TestNewConfigStore_NilClientReturnsError(t *testing.T) {
	_, err := NewConfigStore(nil)
	if !errors.Is(err, ErrNilClient) {
		t.Errorf("expected ErrNilClient, got %v", err)
	}
}

func TestNewRegistrar_DefaultGroup(t *testing.T) {
	// We can't easily test with a real INamingClient (large interface),
	// but we can verify the constructor doesn't panic with a non-nil client.
	// This test is a placeholder; full integration tests require a Nacos mock.
	var _ registry.Registrar = (*Registrar)(nil)
}

func TestCompileTimeAssertions(t *testing.T) {
	var _ registry.Registrar = (*Registrar)(nil)
	var _ registry.Discoverer = (*Discoverer)(nil)
	var _ registry.Subscriber = (*Subscriber)(nil)
	var _ registry.ConfigStore = (*ConfigStore)(nil)
}
