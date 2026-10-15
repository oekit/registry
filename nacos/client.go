package nacos

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// defaultNacosLogDir / defaultNacosCacheDir 是 Nacos SDK 日志/缓存默认目录。
// 使用 os.TempDir() 而非硬编码 /tmp，保证跨平台（macOS/Linux/Windows）可用。
var (
	defaultNacosLogDir   = filepath.Join(os.TempDir(), "nacos", "log")
	defaultNacosCacheDir = filepath.Join(os.TempDir(), "nacos", "cache")
)

// buildNacosConfig 从 Bootstrap 配置构建 Nacos server 和 client 配置。
func buildNacosConfig(cfg Bootstrap) ([]constant.ServerConfig, *constant.ClientConfig, error) {
	host, port, err := parseAddr(cfg.Addr)
	if err != nil {
		return nil, nil, err
	}

	contextPath := cfg.ContextPath
	if contextPath == "" {
		contextPath = parseContextPath(cfg.Addr)
	}

	serverConfig := []constant.ServerConfig{
		{
			IpAddr:      host,
			Port:        port,
			Scheme:      "http",
			ContextPath: contextPath,
			GrpcPort:    cfg.GrpcPort,
		},
	}

	clientConfig := &constant.ClientConfig{
		NamespaceId:         cfg.NamespaceId,
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		LogDir:              defaultNacosLogDir,
		CacheDir:            defaultNacosCacheDir,
		LogLevel:            "warn",
		Username:            cfg.Username,
		Password:            cfg.Password,
	}

	return serverConfig, clientConfig, nil
}

// NewConfigClient creates a Nacos config client from Bootstrap configuration.
func NewConfigClient(cfg Bootstrap) (config_client.IConfigClient, error) {
	serverConfigs, clientConfig, err := buildNacosConfig(cfg)
	if err != nil {
		return nil, err
	}

	client, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  clientConfig,
		ServerConfigs: serverConfigs,
	})
	if err != nil {
		return nil, fmt.Errorf("create nacos config client: %w", err)
	}
	return client, nil
}

// NewNamingClient creates a Nacos naming client for service registration and discovery.
func NewNamingClient(cfg Bootstrap) (naming_client.INamingClient, error) {
	serverConfigs, clientConfig, err := buildNacosConfig(cfg)
	if err != nil {
		return nil, err
	}

	client, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig:  clientConfig,
		ServerConfigs: serverConfigs,
	})
	if err != nil {
		return nil, fmt.Errorf("create nacos naming client: %w", err)
	}
	return client, nil
}

// parseAddr splits "host:port[/path]" and returns host string and port uint64.
// 支持格式: "host:port", "host:port/path", "host:port/nacos", "[::1]:8848"（IPv6）。
// 使用 net.SplitHostPort 正确处理 IPv6 地址（如 [::1]:8848）。
func parseAddr(addr string) (string, uint64, error) {
	// 分离 path（如果有）
	hostPort := addr
	if idx := strings.Index(addr, "/"); idx > 0 {
		hostPort = addr[:idx]
	}

	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", 0, fmt.Errorf("invalid nacos addr %q: %w", addr, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid nacos port in %q: %w", addr, err)
	}
	return host, port, nil
}

// parseContextPath extracts the path portion from "host:port/path" addr.
// Returns empty string if no path is present.
func parseContextPath(addr string) string {
	if idx := strings.Index(addr, "/"); idx > 0 {
		return addr[idx:]
	}
	return ""
}
