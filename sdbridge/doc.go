// Package sdbridge 提供服务发现适配器的框架无关共享核心。
//
// registry/{kitex,hertz} 与 nns/{kitex,hertz} 共 4 个 Resolver 适配器的
// "拉取 + 校验 + 错误包装" 骨架、Hertz 服务描述中的 "?tags" 后缀解析、
// 实例列表校验与注册信息转换，原先各自拷贝一份；本包将其下沉为唯一实现，
// 各框架适配层只保留实例类型转换（kitex 仓 kitex/registry 与 hertz 仓 hertz/registry 的 convert.go）
// 与框架 Result 组装的薄壳。
//
// 本包只允许 import 标准库与 ekit/registry（oekit 平台契约 github.com/oekit/oekit.github.io/blob/main/docs/PLATFORM_CONTRACT.md），
// 禁止依赖 Hertz/Kitex 等具体框架。
//
// # 用法（框架适配层侧）
//
//	core := sdbridge.NewResolverCore(fetch, "ekit", "kitex resolver",
//	    sdbridge.WithFetchErr(sdbridge.DiscoverFetchErr))
//	insts, err := core.Fetch(ctx, service) // 拉取 + 校验 + 统一错误前缀
package sdbridge
