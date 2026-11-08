package registry

import (
	"errors"
	"fmt"
	"net"
	"strings"
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

func TestEncodeTargetTags(t *testing.T) {
	tests := []struct {
		name string
		host string
		tags map[string]string
		want string
	}{
		{"empty tags returns host", "h", nil, "h"},
		{"empty map returns host", "h", map[string]string{}, "h"},
		{"single tag", "h", map[string]string{"a": "1"}, "h?a=1"},
		{"keys sorted", "h", map[string]string{"b": "2", "a": "1"}, "h?a=1&b=2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EncodeTargetTags(tt.host, tt.tags); got != tt.want {
				t.Errorf("EncodeTargetTags(%q, %v) = %q, want %q", tt.host, tt.tags, got, tt.want)
			}
		})
	}
}

func TestEncodeTargetTags_Deterministic(t *testing.T) {
	tags := map[string]string{"a": "1", "b": "2", "c": "3"}
	first := EncodeTargetTags("h", tags)
	for i := 0; i < 10; i++ {
		if got := EncodeTargetTags("h", tags); got != first {
			t.Fatalf("EncodeTargetTags not deterministic: %q vs %q", got, first)
		}
	}
}

// TestEncodeTargetTags_EscapesSpecialChars 值里的分隔符必须被转义（#63）：
// 编码后的 key 中「?」「&」「=」只允许由编码器自身产生。
func TestEncodeTargetTags_EscapesSpecialChars(t *testing.T) {
	got := EncodeTargetTags("h", map[string]string{"a": "b?c=d"})
	if want := "h?a=b%3Fc%3Dd"; got != want {
		t.Errorf("EncodeTargetTags = %q, want %q", got, want)
	}
	// key 含分隔符同样转义
	got = EncodeTargetTags("h", map[string]string{"k&=?": "v"})
	if want := "h?k%26%3D%3F=v"; got != want {
		t.Errorf("EncodeTargetTags = %q, want %q", got, want)
	}
	// 转义后 tag 段不再出现裸分隔符：首个 ? 即服务名分隔符
	// （sdbridge.ServiceNameFromDesc 按首个 ? 截取服务名）
	got = EncodeTargetTags("svc", map[string]string{"a": "x?y&z=w"})
	if n := strings.Count(got, "?"); n != 1 {
		t.Errorf("encoded key %q contains %d '?', want 1", got, n)
	}
}

// TestEncodeTargetTags_NoDelimiterCollision 是 #63 的原始碰撞回归：
// 修复前 ("h", {"a": "b?c=d"}) 与 ("h?a=b", {"c": "d"}) 都编码成 "h?a=b?c=d"，
// 两个不同目标共享缓存条目。
func TestEncodeTargetTags_NoDelimiterCollision(t *testing.T) {
	a := EncodeTargetTags("h", map[string]string{"a": "b?c=d"})
	b := EncodeTargetTags("h?a=b", map[string]string{"c": "d"})
	if a == b {
		t.Fatalf("cache key collision: EncodeTargetTags(h,{a:b?c=d}) == EncodeTargetTags(h?a=b,{c:d}) == %q", a)
	}
}

// TestEncodeTargetTags_Injective 对一组含分隔符的刁钻 (host, tags) 组合
// 做两两互异检查：编码必须对输入单射，否则不同目标共享缓存条目。
func TestEncodeTargetTags_Injective(t *testing.T) {
	cases := []struct {
		host string
		tags map[string]string
	}{
		{"h", nil},
		{"h", map[string]string{"a": "b"}},
		{"h", map[string]string{"a": "b?c=d"}},
		{"h", map[string]string{"a": "b?c", "d": ""}},
		{"h", map[string]string{"a=b": "c"}},
		{"h", map[string]string{"a": "b&c=d"}},
		{"h", map[string]string{"a&b": "c"}},
		{"h", map[string]string{"a": "b c"}}, // 空格 → QueryEscape 转成 +
		{"h", map[string]string{"a": "b+c"}}, // 加号与空格不得互相混淆
		{"h", map[string]string{"a": "%3F"}}, // 已转义形态的明文值
		// 注：host 含 ? 的组合（如 ("h?a=b", {c:d}) vs ("h?a=b?c=d", nil)）
		// 仍会同 key——那是 host 侧的既有二义（服务名不应含 ?，见 godoc），
		// 不在本用例的单射断言范围内；tag 侧碰撞已被转义消除。
	}
	seen := make(map[string]string, len(cases))
	for _, c := range cases {
		key := EncodeTargetTags(c.host, c.tags)
		if prev, dup := seen[key]; dup {
			t.Errorf("collision: (%q, %v) and %s encode to same key %q", c.host, c.tags, prev, key)
			continue
		}
		seen[key] = fmt.Sprintf("(%q, %v)", c.host, c.tags)
	}
}

func TestParseAddr_UnspecifiedHostResolvesLocalIP(t *testing.T) {
	localIP := network.GetPodIP()
	// #64 后：GetPodIP 只剩回环地址可用时（无 POD_IP 且无非回环网卡），
	// 通配地址解析必须报错而非以回环地址注册——期望值跟随本机环境动态判定。
	localIsLoopback := net.ParseIP(localIP) != nil && net.ParseIP(localIP).IsLoopback()

	tests := []struct {
		name    string
		addr    net.Addr
		wantIP  string
		wantErr bool
	}{
		// kitex/hertz 对 ":9090" 监听时 listener.Addr() 的两种真实形态
		{"ipv4Wildcard", &net.TCPAddr{IP: net.IPv4zero, Port: 9090}, localIP, localIsLoopback},
		{"ipv6Wildcard", &net.TCPAddr{IP: net.IPv6unspecified, Port: 9090}, localIP, localIsLoopback},
		{"nilIPEmptyHost", &net.TCPAddr{Port: 9090}, localIP, localIsLoopback},
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

// TestParseAddr_RejectsLoopbackFallback 是 #64 的核心回归：
// 通配监听 + GetPodIP 回退回环（这里用 POD_IP=127.0.0.1 精确控制 GetPodIP
// 的返回值）时，必须返回 [ErrLoopbackAddr] 拒绝注册，而不是静默以
// 127.0.0.1 注册——那会让其他节点永远拨不通该实例。
func TestParseAddr_RejectsLoopbackFallback(t *testing.T) {
	tests := []struct {
		name   string
		podIP  string
		addr   net.Addr
		errMsg string
	}{
		{"ipv4LoopbackViaPODIP", "127.0.0.1", &net.TCPAddr{Port: 9090}, "127.0.0.1"},
		{"ipv6LoopbackViaPODIP", "::1", &net.TCPAddr{Port: 9090}, "::1"},
		{"ipv4LoopbackAny127", "127.0.0.5", &net.TCPAddr{Port: 9090}, "127.0.0.5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("POD_IP", tt.podIP)
			ip, _, err := ParseAddr(tt.addr)
			if err == nil {
				t.Fatalf("ParseAddr with POD_IP=%s: got (%q, nil), want ErrLoopbackAddr", tt.podIP, ip)
			}
			if !errors.Is(err, ErrLoopbackAddr) {
				t.Errorf("err = %v, want ErrLoopbackAddr", err)
			}
			if !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("err = %v, want it to mention %q", err, tt.errMsg)
			}
		})
	}
}

// TestParseAddr_AcceptsNonLoopbackPODIP 对照组：POD_IP 是正常可拨号地址时
// 通配解析照常通过。
func TestParseAddr_AcceptsNonLoopbackPODIP(t *testing.T) {
	t.Setenv("POD_IP", "10.1.2.3")
	ip, port, err := ParseAddr(&net.TCPAddr{Port: 9090})
	if err != nil {
		t.Fatalf("ParseAddr: %v", err)
	}
	if ip != "10.1.2.3" || port != 9090 {
		t.Errorf("ParseAddr = (%q, %d), want (10.1.2.3, 9090)", ip, port)
	}
}

// TestParseAddr_RejectsExplicitLoopback 显式监听回环地址同样拒绝：
// 该地址对其他节点同样不可拨号，大概率是本机调试误配。
func TestParseAddr_RejectsExplicitLoopback(t *testing.T) {
	tests := []struct {
		name string
		addr net.Addr
	}{
		{"ipv4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9090}},
		{"ipv6", &net.TCPAddr{IP: net.ParseIP("::1"), Port: 9090}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := ParseAddr(tt.addr)
			if !errors.Is(err, ErrLoopbackAddr) {
				t.Errorf("ParseAddr(%v) err = %v, want ErrLoopbackAddr", tt.addr, err)
			}
		})
	}
}

// TestInfoFrom_PropagatesLoopbackRejection 注册入口（InfoFrom）必须把
// 回环拒绝透传给调用方——否则 fail-closed 防线在适配层被吞。
func TestInfoFrom_PropagatesLoopbackRejection(t *testing.T) {
	t.Setenv("POD_IP", "127.0.0.1")
	_, err := InfoFrom("svc", &net.TCPAddr{Port: 9090}, 10, 10, nil)
	if !errors.Is(err, ErrLoopbackAddr) {
		t.Errorf("InfoFrom err = %v, want ErrLoopbackAddr", err)
	}
}

// hostPortAddr 是可注入任意 host 形态的 net.Addr 测试桩：net.TCPAddr 只能
// 装合法 IP（或 nil），「域名/垃圾串」形态的 host 必须走自定义 Addr 注入。
type hostPortAddr struct{ s string }

func (a hostPortAddr) Network() string { return "tcp" }
func (a hostPortAddr) String() string  { return a.s }

// TestParseAddr_RejectsNonIPHost #77 回归：判据从「解析成功且是回环 ⇒ 拒」
// 收紧为「不是合法 IP 字面量 ⇒ 拒」——修复前「不是 IP」的主机原样放行且
// err==nil，实例以不可拨通的地址注册进注册中心、全程无告警（违背 ParseAddr
// 「保证对其他节点可拨号」的文档承诺；#64 只堵了回环这一条失效路径）。
// issue 探针的 POD_IP=not-an-ip 形态在 core#87 修复后已被上游拦下，本用例
// 直接把垃圾 host 注入 net.Addr（等价于旧版透传面），断言 fail-closed。
func TestParseAddr_RejectsNonIPHost(t *testing.T) {
	tests := []struct {
		name string
		addr string // host:port 形态的 addr 字符串；host 段就是要注入的垃圾值
		want string // err 文案必须提及的 host 段
	}{
		{"garbage", "not-an-ip:9090", "not-an-ip"},
		{"hostname", "localhost:9090", "localhost"},
		{"domain", "example.internal:9090", "example.internal"},
		{"withPort", "[1.2.3.4:8080]:9090", "1.2.3.4:8080"}, // 括号形态：host 解出 "1.2.3.4:8080"（POD_IP 带端口的注入形态）
		{"leadingSpace", " 10.1.2.3:9090", " 10.1.2.3"},
		{"cidrNotation", "10.1.2.3/24:9090", "10.1.2.3/24"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip, port, err := ParseAddr(hostPortAddr{tt.addr})
			if err == nil {
				t.Fatalf("ParseAddr addr=%q: got (%q, %d, nil), want ErrInvalidHostAddr", tt.addr, ip, port)
			}
			if !errors.Is(err, ErrInvalidHostAddr) {
				t.Errorf("addr=%q err = %v, want ErrInvalidHostAddr", tt.addr, err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want it to mention the offending host %q", err, tt.want)
			}
		})
	}
}

// TestParseAddr_RejectsZonedIPv6 #77 的语义决定项（写进文档）：带 zone 的
// IPv6（fe80::1%eth0）地址本身合法（net.ParseIP 不认、netip.ParseAddr 认），
// 但 zone 是**本机**接口名——注册中心消费方拿到后永远拨不通，同样按
// 「不可拨号」拒绝（ErrInvalidHostAddr）。
func TestParseAddr_RejectsZonedIPv6(t *testing.T) {
	// net.SplitHostPort 的括号形态：host 解出 fe80::1%eth0
	ip, port, err := ParseAddr(hostPortAddr{"[fe80::1%eth0]:9090"})
	if err == nil {
		t.Fatalf("ParseAddr zoned IPv6: got (%q, %d, nil), want ErrInvalidHostAddr", ip, port)
	}
	if !errors.Is(err, ErrInvalidHostAddr) {
		t.Errorf("err = %v, want ErrInvalidHostAddr", err)
	}
	if !strings.Contains(err.Error(), "zone") {
		t.Errorf("err = %v, want it to mention the zone semantics", err)
	}
}

// TestParseAddr_AcceptsPlainIPs 对照组：白名单内的正常形态不被误伤——
// 裸 IPv4/IPv6 照常通过（#64 的回环拒绝语义不变）。
func TestParseAddr_AcceptsPlainIPs(t *testing.T) {
	tests := []struct {
		name   string
		addr   string
		wantIP string
	}{
		{"ipv4", "10.20.30.40:9090", "10.20.30.40"},
		{"ipv6", "[fd00::1]:9090", "fd00::1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip, port, err := ParseAddr(hostPortAddr{tt.addr})
			if err != nil {
				t.Fatalf("ParseAddr(%s): %v", tt.addr, err)
			}
			if ip != tt.wantIP || port != 9090 {
				t.Errorf("ParseAddr = (%q, %d), want (%q, 9090)", ip, port, tt.wantIP)
			}
		})
	}
}
