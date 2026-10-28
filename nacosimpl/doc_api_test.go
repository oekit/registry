package nacos

import (
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
)

// TestDocumentedAPI_SignaturePins 钉住**包文档里出现的那些符号与签名**。
//
// 本包的包文档是使用方的主要入口，而此前它写的示例**编译不过**：
//
//   - `nacosimpl.WithGroup(cfg.Group)` —— 该符号**不存在**（真实的是
//     WithRegistrarGroup / WithDiscovererGroup / WithSubscriberGroup 三个
//     各自独立的选项）；
//   - `store := nacosimpl.NewConfigStore(configClient)` —— NewConfigStore
//     返回 (*ConfigStore, error)，单值接收编译不过；
//   - `reg := nacosimpl.NewRegistrar(...)` —— 同样漏了 error。
//
// 本用例把文档里承诺的调用形态逐一编译一遍 ⇒ 文档再漂移时**当场红**。
func TestDocumentedAPI_SignaturePins(t *testing.T) {
	// 包文档示例里的四个构造器：都返回 (T, error)。
	var (
		_ func(naming_client.INamingClient, ...RegistrarOption) (*Registrar, error)   = NewRegistrar
		_ func(naming_client.INamingClient, ...DiscovererOption) (*Discoverer, error) = NewDiscoverer
		_ func(naming_client.INamingClient, ...SubscriberOption) (*Subscriber, error) = NewSubscriber
		_ func(config_client.IConfigClient) (*ConfigStore, error)                     = NewConfigStore
	)

	// 三个组选项是**各自独立**的类型（不是同一个 WithGroup）。
	var (
		_ RegistrarOption  = WithRegistrarGroup("g")
		_ DiscovererOption = WithDiscovererGroup("g")
		_ SubscriberOption = WithSubscriberGroup("g")
	)
}
