package registry

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"

	"github.com/oekit/core/network"
)

// ErrLoopbackAddr 表示解析出的注册地址是回环地址（127.0.0.0/8、::1）。
// 回环地址对其他节点不可拨号——以它注册出去的实例永远无法被其他节点访问，
// 故 ParseAddr 对回环结果 fail-closed 拒绝（#64）。
// 可通过 errors.Is(err, ErrLoopbackAddr) 判定。
var ErrLoopbackAddr = errors.New("registry: loopback address is not dialable by other nodes")

// ErrInvalidHostAddr 表示解析出的注册主机**根本不是 IP 字面量**（#77）。
// ParseAddr 的文档承诺是「保证注册出去的实例地址对其他节点可拨号」——
// 「不是 IP」的主机名/垃圾串（域名、注错的 POD_IP、多余空格、带了端口）
// 此前与回环共用「解析成功且是回环 ⇒ 拒」的判据，解析失败分支未被覆盖，
// 垃圾值原样放行且 err==nil ⇒ 实例以不可拨通的地址注册、全程无告警。
// 与 [ErrLoopbackAddr] 同族（#64 同一条「可拨号」契约的另一条失效路径），
// 可通过 errors.Is(err, ErrInvalidHostAddr) 判定。
var ErrInvalidHostAddr = errors.New("registry: host is not a dialable IP literal")

// ErrInvalidPort 表示解析出的注册端口不在可拨号范围（1..65535）（#104）。
// ParseAddr 的文档承诺是「保证注册出去的实例地址对其他节点可拨号」——
// 端口维度此前只做语法校验不做范围校验，-1/0/70000 一律放行且 err==nil：
//   - 端口 0 在 TCP 语义里是「让内核挑临时端口」，对**拨入方**而言 host:0
//     永远连不上——注册出去的实例地址不可拨通；
//   - 负端口被下游（nacosimpl）的 uint64 转换变成 18446744073709551615
//     原样注册进 Nacos，消费方拨号必然失败且值已失去可读性。
//
// 与 host 维度的 [ErrLoopbackAddr] / [ErrInvalidHostAddr] 同口径 fail-closed
// （#64/#77 同一条「可拨号」契约的端口维度失效路径），新增独立 sentinel
// 便于调用方区分「地址格式错」与「端口错」，可通过 errors.Is 判定。
var ErrInvalidPort = errors.New("registry: port is outside the dialable range 1..65535")

// ParseAddr 从 net.Addr 解析出 IP 和端口号。
// 当 host 为未指定地址（空、"::" 或 "0.0.0.0"——kitex/hertz 对 ":9090" 这类
// 通配监听时，listener.Addr() 与 WithServiceAddr 会产生这些形式）时，
// 自动使用 network.GetPodIP() 获取本机 IP（支持 K8s POD_IP），保证注册出去
// 的实例地址对其他节点可拨号。
//
// 拒绝面（fail-closed）：
//
//   - 回环地址（127.0.0.0/8、::1）⇒ 包装 [ErrLoopbackAddr]（#64）：此前
//     GetPodIP 失败会静默回退 127.0.0.1——IPv6-only 环境、多网卡取错首卡、
//     POD_IP 未注入时，实例以 127.0.0.1 注册到注册中心，其他节点永远拨不通，
//     全程无 error、无告警。显式监听回环地址（如 127.0.0.1:9090）同样被拒绝：
//     该地址对其他节点同样不可拨号，大概率是本机调试误配。
//   - 不是 IP 字面量的主机（域名/垃圾串）⇒ 包装 [ErrInvalidHostAddr]（#77，
//     net.ParseIP 判据只覆盖「解析成功且是回环」，解析失败分支此前原样放行；
//     core#87 已在 GetLocalIP 上游把非法 POD_IP 拦下，本判据是注册面的纵深，
//     覆盖自定义 net.Addr / 上游兜底之外的一切透传路径）。
//   - 带 zone 的 IPv6（如 fe80::1%eth0）⇒ 同样包装 [ErrInvalidHostAddr]：
//     地址本身合法，但 zone 是**本机**接口名——注册中心消费方拿到
//     `fe80::1%eth0` 永远拨不通（zone 语义只在本机有效），违背「对其他节点
//     可拨号」承诺，显式拒绝并写进文档（#77 的遗留语义决定项）。
//   - 端口不在 1..65535（-1 / 0 / 70000 等）⇒ 包装 [ErrInvalidPort]（#104）：
//     端口维度此前只做语法校验不做范围校验，不可拨通的端口原样放行且
//     err==nil——端口 0 对拨入方永远连不上，负端口经下游 uint64 转换后
//     变成 18446744073709551615 注册进注册中心；与 host 维度同口径拒绝。
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
	// #104：端口维度与 host 维度同口径 fail-closed——语法合法但不可拨通的
	// 端口（-1 / 0 / 70000）此前原样放行且 err==nil，违背「对其他节点可拨号」
	// 的文档承诺。在 host 兜底解析前先拦，让配错端口 fail-fast、不带噪声。
	if port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("refusing port %d (must be within 1..65535 to be dialable by other nodes): %w", port, ErrInvalidPort)
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		// #77 纵深：network.GetPodIP 的口径（core#87）是「非法/非 IPv4 ⇒ 忽略并
		// 回退网卡探测」——POD_IP=::1 这类回环值会被它静默换成本机网卡 IP，
		// 注册面的回环拒绝（ErrLoopbackAddr）就永远看不到了。这里在回退前
		// 直读一次 POD_IP：能解析且是回环 ⇒ 显式拒绝（配错要看得见，不能被
		// 「善意的兜底」掩盖成看似可拨通的地址）；其余形态仍交给 GetPodIP。
		if raw := strings.TrimSpace(os.Getenv("POD_IP")); raw != "" {
			if cfgIP, perr := netip.ParseAddr(raw); perr == nil && cfgIP.IsLoopback() {
				return "", 0, fmt.Errorf("refusing loopback host %q (from POD_IP): %w", raw, ErrLoopbackAddr)
			}
		}
		host = network.GetPodIP()
	}
	// #77：判据从「解析成功且是回环 ⇒ 拒」收紧为「不是合法 IP 字面量 ⇒ 拒」。
	// netip.ParseAddr 而非 net.ParseIP：后者不认 zone 后缀，zoned IPv6 会被
	// 误判成垃圾而失去「给出专门文案」的机会。
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return "", 0, fmt.Errorf("refusing non-IP host %q (must be dialable by other nodes): %w", host, ErrInvalidHostAddr)
	}
	if ip.IsLoopback() {
		return "", 0, fmt.Errorf("refusing loopback host %q: %w", host, ErrLoopbackAddr)
	}
	if ip.Zone() != "" {
		return "", 0, fmt.Errorf("refusing zoned IPv6 host %q (zone is node-local, not dialable by other nodes): %w", host, ErrInvalidHostAddr)
	}
	return host, port, nil
}
