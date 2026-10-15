package nacos

import (
	"os"
	"path/filepath"
	"testing"
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
