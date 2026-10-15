package registry

import (
	"fmt"
	"net"
	"strconv"

	"github.com/oekit/core/network"
)

// ParseAddr 从 net.Addr 解析出 IP 和端口号。
// 当 host 为未指定地址（空、"::" 或 "0.0.0.0"——kitex/hertz 对 ":9090" 这类
// 通配监听时，listener.Addr() 与 WithServiceAddr 会产生这些形式）时，
// 自动使用 network.GetPodIP() 获取本机 IP（支持 K8s POD_IP），保证注册出去
// 的实例地址对其他节点可拨号。
// 这是 ekit 中解析注册地址的唯一实现，registry/kitex、registry/hertz、nns/kitex
// 应委托至此，避免逻辑重复。
func ParseAddr(addr net.Addr) (string, int, error) {
	if addr == nil {
		return "", 0, fmt.Errorf("addr is nil")
	}
	host, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return "", 0, fmt.Errorf("split host port %q: %w", addr.String(), err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("parse port %q: %w", portStr, err)
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = network.GetPodIP()
	}
	return host, port, nil
}
