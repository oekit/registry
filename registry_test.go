package registry

import (
	"net"
	"testing"

	"github.com/oekit/core/network"
)

func TestInstance_Addr(t *testing.T) {
	tests := []struct {
		name string
		inst Instance
		want string
	}{
		{"standard", Instance{IP: "10.0.0.1", Port: 8080}, "10.0.0.1:8080"},
		{"port80", Instance{IP: "127.0.0.1", Port: 80}, "127.0.0.1:80"},
		{"maxPort", Instance{IP: "192.168.1.1", Port: 65535}, "192.168.1.1:65535"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.inst.Addr(); got != tt.want {
				t.Errorf("Addr() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestApply_Defaults(t *testing.T) {
	opt := Apply()
	if opt.Weight != 10 {
		t.Errorf("default Weight = %v, want 10", opt.Weight)
	}
	if opt.Metadata != nil {
		t.Errorf("default Metadata should be nil, got %v", opt.Metadata)
	}
	if opt.Group != "" {
		t.Errorf("default Group should be empty, got %q", opt.Group)
	}
	if opt.Cluster != "" {
		t.Errorf("default Cluster should be empty, got %q", opt.Cluster)
	}
}

func TestApply_WithOptions(t *testing.T) {
	opt := Apply(
		WithWeight(50),
		WithMetadata(map[string]string{"version": "v1"}),
		WithCluster("cluster-a"),
		WithGroup("group-a"),
	)
	if opt.Weight != 50 {
		t.Errorf("Weight = %v, want 50", opt.Weight)
	}
	if opt.Cluster != "cluster-a" {
		t.Errorf("Cluster = %q, want cluster-a", opt.Cluster)
	}
	if opt.Group != "group-a" {
		t.Errorf("Group = %q, want group-a", opt.Group)
	}
	if opt.Metadata["version"] != "v1" {
		t.Errorf("Metadata[version] = %q, want v1", opt.Metadata["version"])
	}
}

func TestApply_WithMetadata_Merges(t *testing.T) {
	opt := Apply(
		WithMetadata(map[string]string{"a": "1"}),
		WithMetadata(map[string]string{"b": "2"}),
	)
	if opt.Metadata["a"] != "1" {
		t.Errorf("Metadata[a] = %q, want 1", opt.Metadata["a"])
	}
	if opt.Metadata["b"] != "2" {
		t.Errorf("Metadata[b] = %q, want 2", opt.Metadata["b"])
	}
}

func TestApply_NilOptionIsSafe(t *testing.T) {
	opt := Apply(nil, WithWeight(20), nil)
	if opt.Weight != 20 {
		t.Errorf("Weight = %v, want 20", opt.Weight)
	}
}

func TestParseAddr_UnspecifiedHostResolvesLocalIP(t *testing.T) {
	localIP := network.GetPodIP()

	tests := []struct {
		name    string
		addr    net.Addr
		wantIP  string
		wantErr bool
	}{
		// kitex/hertz 对 ":9090" 监听时 listener.Addr() 的两种真实形态
		{"ipv4Wildcard", &net.TCPAddr{IP: net.IPv4zero, Port: 9090}, localIP, false},
		{"ipv6Wildcard", &net.TCPAddr{IP: net.IPv6unspecified, Port: 9090}, localIP, false},
		{"nilIPEmptyHost", &net.TCPAddr{Port: 9090}, localIP, false},
		{"explicitIPKept", &net.TCPAddr{IP: net.IPv4(192, 168, 1, 5), Port: 8080}, "192.168.1.5", false},
		{"nilAddr", nil, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip, port, err := ParseAddr(tt.addr)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseAddr: %v", err)
			}
			if ip != tt.wantIP {
				t.Errorf("ip = %q, want %q", ip, tt.wantIP)
			}
			if port != 9090 && tt.name != "explicitIPKept" {
				t.Errorf("port = %d, want 9090", port)
			}
			if tt.name == "explicitIPKept" && port != 8080 {
				t.Errorf("port = %d, want 8080", port)
			}
		})
	}
}
