// host_wire_test.go 验证出站 HTTP 调用真的把 host_callback_id 送到了宿主。
//
// 前面的 host_callback_id_test.go 只验证了 context 传递这一层；这一层即使
// 全对，若 hostHTTPDo 忘了把 h.CallbackID 放进 wire，宿主依然拿不到归因。
// 这里把 SDK 的 host 传输替换成一个记录器，直接观察生产代码构造的 wire。
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
)

// captureHostCalls 把 SDK 的 host 传输换成记录器，返回 (方法名, 请求体) 的
// 快照函数。测试结束后自动恢复。
func captureHostCalls(t *testing.T, reply map[string]any) func() []struct {
	Method string
	Body   map[string]any
} {
	t.Helper()
	var calls []struct {
		Method string
		Body   map[string]any
	}
	abi.SetHostCaller(func(method string, request []byte) ([]byte, error) {
		var body map[string]any
		_ = json.Unmarshal(request, &body)
		calls = append(calls, struct {
			Method string
			Body   map[string]any
		}{Method: method, Body: body})
		result, _ := json.Marshal(reply)
		envelope, _ := json.Marshal(map[string]any{"ok": true, "result": json.RawMessage(result)})
		return envelope, nil
	})
	t.Cleanup(func() { abi.ClearHostCaller() })
	return func() []struct {
		Method string
		Body   map[string]any
	} {
		return calls
	}
}

// TestHostHTTPDoSendsCallbackID 验证非流式出站把 callback id 放进 wire。
func TestHostHTTPDoSendsCallbackID(t *testing.T) {
	// hostBridgeAvailable() 要求 hostAPI 非空；测试里用 SDK 的传输替换，
	// 所以把 hostAPI 指向一个非空哨兵即可放行。
	restoreHostAPI := installFakeHostAPI(t)
	defer restoreHostAPI()

	calls := captureHostCalls(t, map[string]any{
		"status_code": 200,
		"headers":     map[string][]string{},
		"body":        []byte(`{}`),
	})

	host := &abi.Host{CallbackID: "cb-do-1"}
	ctx := withHost(context.Background(), host)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.invalid/v2/chat/completions", strings.NewReader(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hostHTTPDo(req); err != nil {
		t.Fatalf("hostHTTPDo() error = %v", err)
	}

	got := calls()
	if len(got) != 1 {
		t.Fatalf("host calls = %d, want 1", len(got))
	}
	if got[0].Method != pluginabi.MethodHostHTTPDo {
		t.Fatalf("method = %q, want %q", got[0].Method, pluginabi.MethodHostHTTPDo)
	}
	if id, _ := got[0].Body["host_callback_id"].(string); id != "cb-do-1" {
		t.Fatalf("host_callback_id = %q, want cb-do-1 (wire = %+v)", id, got[0].Body)
	}
}

// TestHostHTTPDoStreamSendsCallbackID 验证流式出站同样带上 callback id。
func TestHostHTTPDoStreamSendsCallbackID(t *testing.T) {
	restoreHostAPI := installFakeHostAPI(t)
	defer restoreHostAPI()

	calls := captureHostCalls(t, map[string]any{
		"status_code": 200,
		"headers":     map[string][]string{},
		"stream_id":   "stream-1",
	})

	host := &abi.Host{CallbackID: "cb-stream-1"}
	ctx := withHost(context.Background(), host)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.invalid/v2/chat/completions", strings.NewReader(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	stream, _, _, err := hostHTTPDoStream(req)
	if err != nil {
		t.Fatalf("hostHTTPDoStream() error = %v", err)
	}
	if stream != nil {
		stream.Close()
	}

	got := calls()
	if len(got) == 0 {
		t.Fatal("no host call recorded")
	}
	if got[0].Method != pluginabi.MethodHostHTTPDoStream {
		t.Fatalf("method = %q, want %q", got[0].Method, pluginabi.MethodHostHTTPDoStream)
	}
	if id, _ := got[0].Body["host_callback_id"].(string); id != "cb-stream-1" {
		t.Fatalf("host_callback_id = %q, want cb-stream-1", id)
	}
}
