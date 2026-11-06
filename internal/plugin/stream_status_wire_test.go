package main

import (
	"encoding/json"
	"testing"
)

// 终端错误必须把上游 HTTP 状态一起发给宿主。
//
// 宿主 callHostStreamEmit 只有收到 status 才会把错误包成实现 StatusCode() 的
// 类型；不带 status 时宿主只能兜底 500，客户端看到的限流响应就是
// "internal_server_error" 而不是 429，也就无法据此退避。
func TestStreamEmitErrorCarriesStatusOver(t *testing.T) {
	var captured map[string]any
	original := hostStreamEmitRaw
	hostStreamEmitRaw = func(streamID string, payload []byte, errorMessage string, status int) error {
		body, _ := json.Marshal(struct {
			StreamID string `json:"stream_id"`
			Payload  []byte `json:"payload,omitempty"`
			Error    string `json:"error,omitempty"`
			Status   int    `json:"status,omitempty"`
		}{streamID, payload, errorMessage, status})
		_ = json.Unmarshal(body, &captured)
		return nil
	}
	defer func() { hostStreamEmitRaw = original }()

	streamEmitError("stream-1", "upstream 429: rate limited", 429)

	if captured["status"] != float64(429) {
		t.Fatalf("status = %#v，应为 429", captured["status"])
	}
	if captured["error"] != "upstream 429: rate limited" {
		t.Fatalf("error = %#v", captured["error"])
	}
}

// 无 HTTP 状态的错误（empty_stream、transport）不能凭空带一个状态码，
// 否则客户端会把网络故障当成上游的 4xx/5xx。
func TestStreamEmitErrorOmitsZeroStatus(t *testing.T) {
	var captured map[string]any
	original := hostStreamEmitRaw
	hostStreamEmitRaw = func(streamID string, payload []byte, errorMessage string, status int) error {
		body, _ := json.Marshal(struct {
			StreamID string `json:"stream_id"`
			Payload  []byte `json:"payload,omitempty"`
			Error    string `json:"error,omitempty"`
			Status   int    `json:"status,omitempty"`
		}{streamID, payload, errorMessage, status})
		_ = json.Unmarshal(body, &captured)
		return nil
	}
	defer func() { hostStreamEmitRaw = original }()

	streamEmitError("stream-2", "upstream stream read error: EOF", 0)

	if _, exists := captured["status"]; exists {
		t.Fatalf("status 不该出现：%#v", captured)
	}
}
