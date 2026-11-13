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

// ErrNonRoutableAddr 表示解析出的注册主机是合法 IP 字面量但**不可跨节点
// 路由**（#157）：链路本地单播（169.254.0.0/16——含云元数据端点
// 169.254.169.254、fe80::/10）、组播（224.0.0.0/4、ff00::/8）、受限广播
// （255.255.255.255）、this-network（0.0.0.0/8 非未指定拼写段）。
//
// ParseAddr 的文档承诺是「保证注册出去的实例地址对其他节点可拨号」——
// host 维度此前只判回环与 zone，同一族「不可拨号」地址里回环拒、非 IP
// 字面量拒、端口越界拒，唯独可路由性维度没人管：169.254.169.254、
// 224.0.0.1、255.255.255.255、0.1.2.3 全部放行且 err==nil。且实现内部
// 自相矛盾——`fe80::1%eth0` 被拒（「zone is node-local, not dialable」），
// 同一地址去掉 zone 却放行。与 [ErrLoopbackAddr] 同族同口径 fail-closed，
// 新增独立 sentinel 便于调用方区分「非 IP 字面量」与「不可路由的 IP」，
// 可通过 errors.Is 判定。私网（RFC1918/ULA）不适用本判据——注册面必须
// 放行私网地址（VPC 内可拨）。
var ErrNonRoutableAddr = errors.New("registry: host is not routable from other nodes")

// ParseAddr 从 net.Addr 解析出 IP 和端口号。
// 当 host 为未指定地址（空、"::"、"0.0.0.0" 及其任意拼写——kitex/hertz 对
// ":9090" 这类通配监听时，listener.Addr() 与 WithServiceAddr 会产生这些
// 形式；#157 起按 netip IsUnspecified 语义判定，不再只比字面量）时，
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
//   - 不可跨节点路由的 IP 字面量（链路本地单播 169.254/16——含云元数据
//     端点 169.254.169.254——与 fe80::/10、组播 224/4 与 ff00::/8、受限广播
//     255.255.255.255、this-network 0.0.0.0/8 段）⇒ 包装 [ErrNonRoutableAddr]
//     （#157）：同一「可拨号」契约的可路由性维度——回环拒、非 IP 字面量拒，
//     唯独这一段此前放行且 err==nil，实例以拨不通的地址注册、全程无告警；
//     与 zoned IPv6 的拒绝口径（「zone is node-local, not dialable」）收敛为
//     同一结论，不再因拼写差异分裂。私网（RFC1918/ULA）不在拒绝面——
//     注册面必须放行私网地址（VPC 内可拨）。
//
// 未指定地址（空、"::"、"0.0.0.0" 及其任意拼写——::0、0:0:0:0:0:0:0:0、
// ::ffff:0.0.0.0，#157 起按 netip IsUnspecified 语义判定而非字面量比对）
// 自动使用 network.GetPodIP() 获取本机 IP（支持 K8s POD_IP），保证注册出去
// 的实例地址对其他节点可拨号。
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
	// #157：未指定地址兜底的触发条件从字面量比对改为 netip 语义判定——
	// ::0 / 0:0:0:0:0:0:0:0 / ::ffff:0.0.0.0 与 "::"/"0.0.0.0" 同语义
	// （IsUnspecified），此前以「拨不通的通配地址」原样注册，与同一语义的
	// 字面量拼写（已被兜底成 Pod IP）行为分裂。
	if host == "" || hostIsUnspecified(host) {
		// #77 纵深：network.GetPodIP 的口径（core#87）是「非法/非 IPv4 ⇒ 忽略并
		// 回退网卡探测」——POD_IP=::1 这类回环值会被它静默换成本机网卡 IP，
		// 注册面的回环拒绝（ErrLoopbackAddr）就永远看不到了。这里在回退前
		// 直读一次 POD_IP：能解析且是回环/不可路由段 ⇒ 显式拒绝（配错要看得见，
		// 不能被「善意的兜底」掩盖成看似可拨通的地址）；其余形态仍交给 GetPodIP。
		if raw := strings.TrimSpace(os.Getenv("POD_IP")); raw != "" {
			if cfgIP, perr := netip.ParseAddr(raw); perr == nil {
				if cfgIP.IsLoopback() {
					return "", 0, fmt.Errorf("refusing loopback host %q (from POD_IP): %w", raw, ErrLoopbackAddr)
				}
				if nonRoutableHost(cfgIP) {
					return "", 0, fmt.Errorf("refusing non-routable host %q (from POD_IP): %w", raw, ErrNonRoutableAddr)
				}
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
	// #157：可路由性判据——链路本地单播（169.254/16 含云元数据端点 +
	// fe80::/10）、组播（224/4 + ff00::/8）、受限广播（255.255.255.255）、
	// this-network（0.0.0.0/8 段）均不可跨节点拨号，与回环同口径 fail-closed；
	// 与 zoned 链路本地的既有拒绝收敛为同一结论（不可拨号），判据不再因
	// 拼写差异分裂。
	if nonRoutableHost(ip) {
		return "", 0, fmt.Errorf("refusing non-routable host %q (link-local/multicast/broadcast/this-network are not dialable by other nodes): %w", host, ErrNonRoutableAddr)
	}
	if ip.Unmap().IsUnspecified() {
		// 防御位：未指定地址理应已走兜底（GetPodIP 产出不会是未指定拼写）；
		// 若兜底仍产出未指定地址，按不可路由拒绝而非静默注册通配地址。
		return "", 0, fmt.Errorf("refusing unspecified host %q (wildcard address is not dialable by other nodes): %w", host, ErrNonRoutableAddr)
	}
	return host, port, nil
}

// hostIsUnspecified 判定 host 是否为未指定地址的任意拼写（#157）：netip 的
// IsUnspecified 语义判据覆盖 ::0、0:0:0:0:0:0:0:0、::ffff:0.0.0.0（Is4In6
// 形态 IsUnspecified 恒 false，必须 Unmap 后判定）——此前只比 "::"/"0.0.0.0"
// 两个字面量，非规范拼写绕过 GetPodIP 兜底。非 IP 字面量返回 false（交给
// 下游的 ErrInvalidHostAddr 分支）。
func hostIsUnspecified(host string) bool {
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return ip.Unmap().IsUnspecified()
}

// nonRoutableHost 判定 IP 是否不可跨节点路由（#157）：链路本地单播
// （169.254/16 含云元数据端点 169.254.169.254、fe80::/10）、组播
// （224/4、ff00::/8）、受限广播（255.255.255.255）、this-network
// （0.0.0.0/8 段）。判据与同生态 core/network IsPublicIP、pkg/urlguard
// 出站阻断清单同源（netip 已有现成方法，广播/this-network 两段 netip 无
// 现成判据、按 urlguard 段清单判）；私网（RFC1918/ULA）不在此列——注册
// 面必须放行私网地址（VPC 内可拨）。
func nonRoutableHost(ip netip.Addr) bool {
	if ip.IsLinkLocalUnicast() || ip.IsMulticast() {
		return true
	}
	// Is4 段判据（广播/this-network）对 Is4In6 形态（::ffff:x.y.z.w）恒 false，
	// 统一 Unmap 到 v4 语义判定——::ffff:255.255.255.255 与 255.255.255.255
	// 同结论。
	if u := ip.Unmap(); u.Is4() {
		b := u.As4()
		if b == [4]byte{255, 255, 255, 255} || b[0] == 0 {
			return true
		}
	}
	return false
}
