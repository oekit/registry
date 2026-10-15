package sdbridge

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/oekit/registry/registry"
)

func TestServiceNameFromDesc(t *testing.T) {
	tests := []struct {
		name string
		desc string
		want string
	}{
		{name: "plain", desc: "echo-service", want: "echo-service"},
		{name: "with tags", desc: "echo-service?env=prod&v=1", want: "echo-service"},
		{name: "empty tag value", desc: "echo-service?", want: "echo-service"},
		{name: "empty", desc: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ServiceNameFromDesc(tt.desc); got != tt.want {
				t.Errorf("ServiceNameFromDesc(%q) = %q, want %q", tt.desc, got, tt.want)
			}
		})
	}
}

func TestValidate_OK(t *testing.T) {
	insts := []registry.Instance{{Service: "echo", IP: "10.0.0.1", Port: 8080}}
	got, err := Validate(insts, "echo")
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if len(got) != 1 || got[0].IP != "10.0.0.1" {
		t.Errorf("Validate() = %v, want original instances", got)
	}
}

func TestValidate_Empty(t *testing.T) {
	for _, in := range [][]registry.Instance{nil, {}} {
		_, err := Validate(in, "echo")
		if err == nil {
			t.Fatal("Validate(empty) error = nil, want error")
		}
		if !errors.Is(err, registry.ErrNoInstances) {
			t.Errorf("Validate(empty) error %q does not wrap registry.ErrNoInstances", err)
		}
		// 消息不含前缀（前缀由 ResolverCore.Fetch 添加）
		if want := `no instances for "echo"`; err.Error() != want {
			t.Errorf("Validate(empty) error = %q, want %q", err.Error(), want)
		}
	}
}

func TestResolverCore_Fetch(t *testing.T) {
	boom := errors.New("nacos down")
	newCore := func(fetch FetchFunc, opts ...ResolverCoreOption) *ResolverCore {
		return NewResolverCore(fetch, "ekit", "kitex resolver", opts...)
	}

	t.Run("success", func(t *testing.T) {
		insts := []registry.Instance{{Service: "echo", IP: "10.0.0.1", Port: 8080, Weight: 10}}
		core := newCore(func(_ context.Context, _ string) ([]registry.Instance, error) {
			return insts, nil
		})
		got, err := core.Fetch(context.Background(), "echo")
		if err != nil {
			t.Fatalf("Fetch() error = %v", err)
		}
		if len(got) != 1 {
			t.Errorf("len(Fetch()) = %d, want 1", len(got))
		}
	})

	t.Run("fetch error with DiscoverFetchErr", func(t *testing.T) {
		core := newCore(func(_ context.Context, _ string) ([]registry.Instance, error) {
			return nil, boom
		}, WithFetchErr(DiscoverFetchErr))
		_, err := core.Fetch(context.Background(), "echo")
		if !errors.Is(err, boom) {
			t.Fatalf("Fetch() error = %v, want wrap %v", err, boom)
		}
		// 与 registry/kitex、registry/hertz 历史文案逐字一致
		if want := `kitex resolver: discover "echo": nacos down`; err.Error() != want {
			t.Errorf("Fetch() error = %q, want %q", err.Error(), want)
		}
	})

	t.Run("fetch error default plain format", func(t *testing.T) {
		core := newCore(func(_ context.Context, _ string) ([]registry.Instance, error) {
			return nil, boom
		})
		_, err := core.Fetch(context.Background(), "echo")
		if !errors.Is(err, boom) {
			t.Fatalf("Fetch() error = %v, want wrap %v", err, boom)
		}
		// 与 nns/kitex、nns/hertz 历史文案逐字一致
		if want := `kitex resolver: nacos down`; err.Error() != want {
			t.Errorf("Fetch() error = %q, want %q", err.Error(), want)
		}
	})

	t.Run("empty instances", func(t *testing.T) {
		core := newCore(func(_ context.Context, _ string) ([]registry.Instance, error) {
			return nil, nil
		})
		_, err := core.Fetch(context.Background(), "echo")
		if err == nil {
			t.Fatal("Fetch() error = nil, want error")
		}
		if !errors.Is(err, registry.ErrNoInstances) {
			t.Errorf("Fetch() error %q does not wrap registry.ErrNoInstances", err)
		}
		// 与 4 个适配器历史文案逐字一致（前缀 + no instances for %q）
		if want := `kitex resolver: no instances for "echo"`; err.Error() != want {
			t.Errorf("Fetch() error = %q, want %q", err.Error(), want)
		}
	})
}

func TestResolverCore_NameAndCacheable(t *testing.T) {
	core := NewResolverCore(func(_ context.Context, _ string) ([]registry.Instance, error) {
		return nil, nil
	}, "nns-kitex", "nns-kitex resolver")
	if core.Name() != "nns-kitex" {
		t.Errorf("Name() = %q, want %q", core.Name(), "nns-kitex")
	}
	if !core.Cacheable() {
		t.Error("Cacheable() = false, want true")
	}
}

func TestToInstance(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 8080}

	t.Run("explicit weight", func(t *testing.T) {
		inst, err := ToInstance("svc", addr, 20, 10, map[string]string{"env": "prod"})
		if err != nil {
			t.Fatalf("ToInstance() error = %v", err)
		}
		if inst.Weight != 20 || inst.Metadata["env"] != "prod" {
			t.Errorf("ToInstance() = %+v, want weight 20 + tags", inst)
		}
	})

	t.Run("zero weight uses default", func(t *testing.T) {
		inst, err := ToInstance("svc", addr, 0, 10, nil)
		if err != nil {
			t.Fatalf("ToInstance() error = %v", err)
		}
		if inst.Weight != 10 {
			t.Errorf("Weight = %v, want 10", inst.Weight)
		}
	})

	t.Run("empty name error", func(t *testing.T) {
		if _, err := ToInstance("", addr, 10, 10, nil); err == nil {
			t.Error("ToInstance(empty name) error = nil, want error")
		}
	})
}
