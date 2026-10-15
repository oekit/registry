package registrymocks

import (
	"context"
	"errors"
	"testing"

	"github.com/oekit/registry/registry"
)

func TestRegistrar_BasicFlow(t *testing.T) {
	r := &Registrar{}
	ctx := context.Background()
	inst := registry.Instance{Service: "svc", IP: "1.2.3.4", Port: 8080}
	if err := r.Register(ctx, inst); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := r.Deregister(ctx, inst); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if len(r.Registered) != 1 || len(r.Deregistered) != 1 {
		t.Errorf("expected 1 registered + 1 deregistered, got %d/%d", len(r.Registered), len(r.Deregistered))
	}
}

func TestRegistrar_ErrorInjection(t *testing.T) {
	r := &Registrar{RegErr: errors.New("boom")}
	if err := r.Register(context.Background(), registry.Instance{}); err == nil {
		t.Error("expected Register error")
	}
	if len(r.Registered) != 0 {
		t.Error("expected nothing recorded when RegErr set")
	}
}

func TestDiscoverer_PerService(t *testing.T) {
	d := &Discoverer{
		InstancesMap: map[string][]registry.Instance{
			"svc-a": {{IP: "1.1.1.1"}},
			"svc-b": {{IP: "2.2.2.2"}},
		},
	}
	a, _ := d.Discover(context.Background(), "svc-a")
	if len(a) != 1 || a[0].IP != "1.1.1.1" {
		t.Errorf("svc-a = %+v", a)
	}
	b, _ := d.Discover(context.Background(), "svc-b")
	if len(b) != 1 || b[0].IP != "2.2.2.2" {
		t.Errorf("svc-b = %+v", b)
	}
}

func TestSubscriber_PushCallback(t *testing.T) {
	sub := NewSubscriber()
	called := 0
	err := sub.Subscribe(context.Background(), "svc", func([]registry.Instance) {
		called++
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sub.Push("svc", []registry.Instance{{IP: "1.2.3.4"}})
	sub.Push("svc", []registry.Instance{{IP: "2.3.4.5"}})
	if called != 2 {
		t.Errorf("expected 2 callback invocations, got %d", called)
	}
}

func TestConfigStore_FireChange(t *testing.T) {
	cs := NewConfigStore()
	received := ""
	err := cs.ListenConfig(context.Background(), "dataID", "GROUP", func(s string) {
		received = s
	})
	if err != nil {
		t.Fatalf("ListenConfig: %v", err)
	}
	cs.FireChange("dataID", "GROUP", "new-value")
	if received != "new-value" {
		t.Errorf("received = %q, want %q", received, "new-value")
	}
}
