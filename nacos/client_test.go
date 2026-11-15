package nacos

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/oekit/registry"
)

func TestParseAddr(t *testing.T) {
	tests := []struct {
		name     string
		addr     string
		wantHost string
		wantPort uint64
		wantErr  bool
	}{
		{"host:port", "nacos.local:8848", "nacos.local", 8848, false},
		{"host:port/path", "nacos.local:8848/nacos", "nacos.local", 8848, false},
		{"ipv6", "[::1]:8848", "::1", 8848, false},
		{"ipv6 with path", "[::1]:8848/nacos", "::1", 8848, false},
		{"missing port", "nacos.local", "", 0, true},
		{"empty", "", "", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, err := parseAddr(tt.addr)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseAddr(%q) err = %v, wantErr %v", tt.addr, err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if host != tt.wantHost {
				t.Errorf("host = %q, want %q", host, tt.wantHost)
			}
			if port != tt.wantPort {
				t.Errorf("port = %d, want %d", port, tt.wantPort)
			}
		})
	}
}

// TestParseAddr_PortRangeFailClosed 回归（#197）：端口范围 fail-closed，
// 与 registry.ParseAddr 的 #104 同口径复用 registry.ErrInvalidPort——
// 此前只做语法校验（上界 2^64-1 且接受 "0"），0/70000 直通 ServerConfig，
// 配错端口推迟到 SDK 连接期才暴露且报错不点名端口越界。
func TestParseAddr_PortRangeFailClosed(t *testing.T) {
	for _, addr := range []string{"nacos.local:0", "nacos.local:65536", "nacos.local:70000"} {
		if _, _, err := parseAddr(addr); !errors.Is(err, registry.ErrInvalidPort) {
			t.Errorf("parseAddr(%q) err = %v, want wrapped registry.ErrInvalidPort", addr, err)
		}
	}
	// 边界内放行
	if _, port, err := parseAddr("nacos.local:65535"); err != nil || port != 65535 {
		t.Errorf("parseAddr 65535 = (%d, %v), want (65535, nil)", port, err)
	}
}

// TestBuildNacosConfig_GrpcPortRangeFailClosed 同门覆盖 GrpcPort（#197：
// types.go 同样零校验）——非 0 越界 fail-closed，0 表示未设置（SDK 默认
// serverPort+1000）放行。
func TestBuildNacosConfig_GrpcPortRangeFailClosed(t *testing.T) {
	if _, _, err := buildNacosConfig(Bootstrap{Addr: "nacos.local:8848", GrpcPort: 99999}); !errors.Is(err, registry.ErrInvalidPort) {
		t.Errorf("grpc_port 99999 err = %v, want wrapped registry.ErrInvalidPort", err)
	}
	if _, _, err := buildNacosConfig(Bootstrap{Addr: "nacos.local:8848"}); err != nil {
		t.Errorf("unset grpc_port (0) must be allowed, got %v", err)
	}
}

func TestDefaultDirs_NotHardcodedTmp(t *testing.T) {
	// 确保基于 os.TempDir()（跨平台），而非硬编码 "/tmp"
	// （Linux 下 os.TempDir() 恰为 "/tmp"，不能用 != "/tmp..." 判定）
	if want := filepath.Join(os.TempDir(), "nacos", "log"); defaultNacosLogDir != want {
		t.Errorf("LogDir = %q, want %q（应基于 os.TempDir()）", defaultNacosLogDir, want)
	}
	if want := filepath.Join(os.TempDir(), "nacos", "cache"); defaultNacosCacheDir != want {
		t.Errorf("CacheDir = %q, want %q（应基于 os.TempDir()）", defaultNacosCacheDir, want)
	}
}
