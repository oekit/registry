package sdbridge

import (
	"testing"

	"github.com/oekit/registry"
)

func TestInstanceEndpoint(t *testing.T) {
	tests := []struct {
		name    string
		inst    registry.Instance
		wantNet string
		want    string
	}{
		{
			name:    "ipv4",
			inst:    registry.Instance{Service: "echo", IP: "10.0.0.1", Port: 8080},
			wantNet: "tcp",
			want:    "10.0.0.1:8080",
		},
		{
			name:    "ipv6 bracketed",
			inst:    registry.Instance{Service: "echo", IP: "fd00::1", Port: 9090},
			wantNet: "tcp",
			want:    "[fd00::1]:9090",
		},
		{
			name:    "zero instance",
			inst:    registry.Instance{},
			wantNet: "tcp",
			want:    ":0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNet, got := InstanceEndpoint(tt.inst)
			if gotNet != tt.wantNet {
				t.Errorf("network = %q, want %q", gotNet, tt.wantNet)
			}
			if got != tt.want {
				t.Errorf("addr = %q, want %q", got, tt.want)
			}
			// 与根包 Instance.Addr() 同源（JoinHostPort 单一实现）
			if addr := tt.inst.Addr(); got != addr {
				t.Errorf("InstanceEndpoint() = %q, Instance.Addr() = %q, must agree", got, addr)
			}
		})
	}
}

func TestWeightOrDefault(t *testing.T) {
	tests := []struct {
		name          string
		weight        float64
		defaultWeight float64
		want          float64
	}{
		{name: "positive passthrough", weight: 10, defaultWeight: 5, want: 10},
		{name: "fractional passthrough", weight: 0.5, defaultWeight: 5, want: 0.5},
		{name: "zero uses default", weight: 0, defaultWeight: 5, want: 5},
		{name: "negative uses default", weight: -1, defaultWeight: 5, want: 5},
		{name: "both zero falls to DefaultInstanceWeight", weight: 0, defaultWeight: 0, want: registry.DefaultInstanceWeight},
		{name: "negative default falls to DefaultInstanceWeight", weight: -2, defaultWeight: -3, want: registry.DefaultInstanceWeight},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WeightOrDefault(tt.weight, tt.defaultWeight); got != tt.want {
				t.Errorf("WeightOrDefault(%v, %v) = %v, want %v", tt.weight, tt.defaultWeight, got, tt.want)
			}
		})
	}
	if registry.DefaultInstanceWeight != 10 {
		t.Fatalf("registry.DefaultInstanceWeight = %v, want 10 (#214 口径)", registry.DefaultInstanceWeight)
	}
}

// TestEnsureNotNil_NoPanicOnValue 非 nil 依赖不触发 panic。
func TestEnsureNotNil_NoPanicOnValue(t *testing.T) {
	reg := &recorderRegistrar{}
	EnsureNotNil(reg, "hertz: NewRegistry", "registrar") // must not panic
	EnsureNotNil(struct{}{}, "x: New", "thing")
}

// TestEnsureNotNil_PanicMessage nil 依赖构造期 panic，消息与 hertz#219
// 文案口径逐字一致（"<caller> requires non-nil <what>"）。
func TestEnsureNotNil_PanicMessage(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil dep")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value = %v, want string", r)
		}
		if want := "hertz: NewRegistry requires non-nil registrar"; msg != want {
			t.Errorf("panic message = %q, want %q", msg, want)
		}
	}()
	EnsureNotNil(nil, "hertz: NewRegistry", "registrar")
}
