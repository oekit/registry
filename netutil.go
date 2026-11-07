package registry

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/oekit/core/network"
)

// ErrLoopbackAddr 表示解析出的注册地址是回环地址（127.0.0.0/8、::1）。
// 回环地址对其他节点不可拨号——以它注册出去的实例永远无法被其他节点访问，
// 故 ParseAddr 对回环结果 fail-closed 拒绝（#64）。
// 可通过 errors.Is(err, ErrLoopbackAddr) 判定。
var ErrLoopbackAddr = errors.New("registry: loopback address is not dialable by other nodes")

// ParseAddr 从 net.Addr 解析出 IP 和端口号。
// 当 host 为未指定地址（空、"::" 或 "0.0.0.0"——kitex/hertz 对 ":9090" 这类
// 通配监听时，listener.Addr() 与 WithServiceAddr 会产生这些形式）时，
// 自动使用 network.GetPodIP() 获取本机 IP（支持 K8s POD_IP），保证注册出去
// 的实例地址对其他节点可拨号。
//
// 结果为回环地址时返回包装 [ErrLoopbackAddr] 的 error，拒绝注册（fail-closed，
// #64）：此前 GetPodIP 失败会静默回退 127.0.0.1——IPv6-only 环境、多网卡取错
// 首卡、POD_IP 未注入时，实例以 127.0.0.1 注册到注册中心，其他节点永远拨不通，
// 全程无 error、无告警。显式监听回环地址（如 127.0.0.1:9090）同样被拒绝：
// 该地址对其他节点同样不可拨号，大概率是本机调试误配。
//
// 这是 ekit 中解析注册地址的唯一实现，kitex/registry、hertz/registry、kitex/nns
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
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return "", 0, fmt.Errorf("refusing loopback host %q: %w", host, ErrLoopbackAddr)
	}
	return host, port, nil
}
