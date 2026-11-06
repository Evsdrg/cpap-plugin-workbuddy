// host_callback_id_test.go 钉住「出站请求必须带回宿主签发的 callback id」。
//
// 这是用量面板「上游接口」列能显示真实出站路径的前提：宿主在
// host.http.do / do_stream 的回调里按 host_callback_id 找到发起它的请求，
// 再把出站路径记到那条用量上。插件若丢掉这个 id，宿主只能拿到一个
// 无归属的调用，用量记录的上游接口就退化成入站协议。
package main

import (
	"context"
	"net/http"
	"testing"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
)

// TestWithHostCarriesCallbackID 验证 withHost/hostFromContext 这一对不丢句柄。
func TestWithHostCarriesCallbackID(t *testing.T) {
	host := &abi.Host{CallbackID: "cb-123"}
	ctx := withHost(context.Background(), host)
	got := hostFromContext(ctx)
	if got == nil {
		t.Fatal("hostFromContext() = nil, want the host installed by withHost")
	}
	if got.CallbackID != "cb-123" {
		t.Fatalf("CallbackID = %q, want cb-123", got.CallbackID)
	}
}

// TestHostFromContextWithoutHost 验证没有宿主时返回 nil，让出站函数退化为
// 测试直连通道而不是崩溃。
func TestHostFromContextWithoutHost(t *testing.T) {
	if got := hostFromContext(context.Background()); got != nil {
		t.Fatalf("hostFromContext() = %+v, want nil", got)
	}
	if got := hostFromContext(nil); got != nil {
		t.Fatalf("hostFromContext(nil) = %+v, want nil", got)
	}
}

// TestWithHostIgnoresNilHost 验证 nil 宿主不会把 ctx 弄坏。
func TestWithHostIgnoresNilHost(t *testing.T) {
	ctx := withHost(context.Background(), nil)
	if got := hostFromContext(ctx); got != nil {
		t.Fatalf("hostFromContext() = %+v, want nil", got)
	}
}

// TestExecutorRequestContextCarriesHost 验证 executor 请求路径确实把宿主
// 放进了 http.Request 的 context —— 出站函数就是从这里取的。
func TestExecutorRequestContextCarriesHost(t *testing.T) {
	host := &abi.Host{CallbackID: "cb-exec"}
	ctx := withHost(context.Background(), host)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.invalid/v2/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	got := hostFromContext(req.Context())
	if got == nil || got.CallbackID != "cb-exec" {
		t.Fatalf("request context host = %+v, want callback cb-exec", got)
	}
}
