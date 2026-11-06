package main

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
)

// captureEmitWire 把 SDK 的宿主传输替换成记录器，返回最近一次
// host.stream.emit 的请求体。测试结束时自动恢复。
//
// 记录点选在 abi.HostCall 之下（也就是真正的 wire），而不是 hostStreamEmitRaw
// 这个 seam：seam 只负责拼参数，真正决定 JSON 形态的是 SDK。只有观察到
// wire 才能钉住「error 必须走专用字段」这个契约。
func captureEmitWire(t *testing.T) func() []byte {
	t.Helper()
	var mu sync.Mutex
	var wire []byte
	abi.SetHostCaller(func(method string, request []byte) ([]byte, error) {
		if method != pluginabi.MethodHostStreamEmit {
			envelope, _ := json.Marshal(map[string]any{"ok": true, "result": map[string]any{}})
			return envelope, nil
		}
		mu.Lock()
		wire = append([]byte(nil), request...)
		mu.Unlock()
		envelope, _ := json.Marshal(map[string]any{"ok": true, "result": map[string]any{}})
		return envelope, nil
	})
	t.Cleanup(func() { abi.ClearHostCaller() })
	return func() []byte {
		mu.Lock()
		defer mu.Unlock()
		return append([]byte(nil), wire...)
	}
}

// TestStreamEmitErrorUsesTheDedicatedErrorField 钉住让上游失败能穿过流桥的
// wire 形态。
//
// 宿主的 stream.emit RPC 有两条通道：payload（流式数据）与 error（一次失败
// 尝试的终止错误）。只有非空 error 会变成宿主侧的 chunk.Err，而只有
// chunk.Err 会作为该次尝试的真实结局到达 conductor（readStreamBootstrap）。
// 把错误编码成 {"error":...} 的 payload 帧（0.9.14 之前的形态）是无法翻译的
// 数据：conductor 看到的是「流关闭且没有 payload」，于是合成 empty_stream +
// Retryable，客户端真正该看到的失败信息（"upstream 429: 您的使用量已超出
// 频率限制"）就丢了。
//
// 这次事故的背景：workbuddy 的每个账号都回 429（code 6004，按模型计的当日
// 频率限制），但客户端只看到 empty_stream，且 conductor 因为 empty_stream
// 可重试而把所有账号重跑了一遍。
func TestStreamEmitErrorUsesTheDedicatedErrorField(t *testing.T) {
	readWire := captureEmitWire(t)

	streamEmitError("stream-9", `upstream 429: {"code":6004,"msg":"您的使用量已超出频率限制，将在 2026-10-19 22:55:50 UTC+8 重置"}`, 429)

	wire := readWire()
	if len(wire) == 0 {
		t.Fatal("no host.stream.emit call was made")
	}
	var decoded map[string]any
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("emit request is not JSON: %v\n%s", err, wire)
	}
	got, _ := decoded["error"].(string)
	if got == "" {
		t.Fatalf("error must travel in the dedicated \"error\" field, got request: %s", wire)
	}
	if !strings.Contains(got, "429") {
		t.Fatalf("error message lost the upstream status: %q", got)
	}
	if payload, present := decoded["payload"]; present && payload != nil && payload != "" {
		t.Fatalf("error must not be encoded as a payload frame: %s", wire)
	}
	if decoded["stream_id"] != "stream-9" {
		t.Fatalf("stream_id lost: %s", wire)
	}
}

// TestStreamEmitErrorRedactsSecrets 保持 A-37：emit 请求是宿主可见的日志面，
// 原始上游响应体不能带着 Bearer token 穿过去。
func TestStreamEmitErrorRedactsSecrets(t *testing.T) {
	readWire := captureEmitWire(t)

	streamEmitError("stream-9", `upstream 401: {"token":"Bearer eyJhbGciOiJSUzI1NiJ9.secret"}`, 401)

	wire := readWire()
	var decoded map[string]any
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("emit request is not JSON: %v", err)
	}
	got, _ := decoded["error"].(string)
	if strings.Contains(got, "eyJhbGci") {
		t.Fatalf("JWT leaked through the error path: %s", got)
	}
	if !strings.Contains(got, "upstream 401") {
		t.Fatalf("status prefix lost during redaction: %s", got)
	}
}

// TestStreamEmitKeepsPayloadChannelForData 钉住 wire 契约的另一半：普通数据块
// 只走 payload。若将来把数据塞进 error 字段，宿主会把每个 chunk 都当成终止
// 错误。
func TestStreamEmitKeepsPayloadChannelForData(t *testing.T) {
	readWire := captureEmitWire(t)

	if err := streamEmit("stream-4", []byte(`data: {"choices":[]}`)); err != nil {
		t.Fatalf("streamEmit: %v", err)
	}

	wire := readWire()
	var decoded struct {
		StreamID string `json:"stream_id"`
		Payload  []byte `json:"payload"`
		Error    string `json:"error"`
	}
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("emit request is not JSON: %v", err)
	}
	if string(decoded.Payload) != `data: {"choices":[]}` {
		t.Fatalf("data chunk lost its payload field: %s", wire)
	}
	if decoded.Error != "" {
		t.Fatalf("a data chunk must not set the error field: %s", wire)
	}
}
