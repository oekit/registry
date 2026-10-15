package nacos

import (
	"fmt"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/oekit/core/network"
	"github.com/oekit/logx"
	"github.com/oekit/registry/registry"
)

var log = logx.Delegating("registry/nacos")

// toInstance 将 Nacos SDK 的 model.Instance 转换为 provider 无关的 registry.Instance。
// metadata 做深拷贝：SDK 会在推送更新时原地修改内部实例对象，共享 map
// 会引发脏读与数据竞争。
func toInstance(serviceName string, m model.Instance) registry.Instance {
	return registry.Instance{
		Service:  serviceName,
		IP:       m.Ip,
		Port:     int(m.Port),
		Weight:   m.Weight,
		Metadata: copyMetadata(m.Metadata),
	}
}

// copyMetadata 返回 metadata 的浅层拷贝（值为 string，一层即深拷贝）；nil 透传 nil。
func copyMetadata(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// toInstances 批量转换。
func toInstances(serviceName string, src []model.Instance) []registry.Instance {
	result := make([]registry.Instance, 0, len(src))
	for _, m := range src {
		result = append(result, toInstance(serviceName, m))
	}
	return result
}

// GetPodIP 获取本机 IP 地址。
//
// 优先读取 POD_IP 环境变量（Kubernetes 中通过 downward API 注入），
// 其次遍历网卡获取第一个非回环 IPv4 地址，最后回退到 127.0.0.1。
//
// 用于服务注册时自动探测本机 IP：
//
//	ip := nacosimpl.GetPodIP()
//	reg.Register(ctx, registry.Instance{Service: "my-svc", IP: ip, Port: 8080})
func GetPodIP() string {
	return network.GetPodIP()
}

// wrapErr 包装错误并附带 Nacos 上下文信息。
func wrapErr(op, service string, err error) error {
	return fmt.Errorf("registry/nacos: %s [%s]: %w", op, service, err)
}
