// host_ctx.go 把本次调用的 abi.Host 沿 context 传到最底层的 HTTP 出站函数。
//
// 为什么用 context 而不是给每个函数加参数：executor 的出站调用要穿过
// upstreamAttempt → runFailover → pumpWithFailover → pumpAttemptWithTimeout
// → openPumpStream 等一长串函数，逐层加参只是为了把一个不可变的句柄带到底，
// 而这些函数里没有一个需要读它。context 本来就是承载这类请求作用域值的
// 通道，且 http.Request 自带 context，出站函数可以直接从 req.Context() 取。
package main

import (
	"context"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
)

type hostContextKey struct{}

// withHost 把本次调用的宿主句柄放进 ctx。nil 宿主原样返回 ctx，让测试
// 路径（没有 cgo 宿主）自然退化为直连。
func withHost(ctx context.Context, h *abi.Host) context.Context {
	if h == nil {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, hostContextKey{}, h)
}

// hostFromContext 取回 withHost 放进去的句柄；没有则返回 nil。
func hostFromContext(ctx context.Context) *abi.Host {
	if ctx == nil {
		return nil
	}
	h, _ := ctx.Value(hostContextKey{}).(*abi.Host)
	return h
}
