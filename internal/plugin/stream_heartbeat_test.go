package main

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// SSE 注释行（上游每条流都以 ": heartbeat" 开头）不是数据帧。
// 早期实现把它当成「第一个 payload」，两个后果：
//  1. 首 token 看门狗被解除，只发心跳的流永远不会触发 empty_stream；
//  2. emitted 被置 true，于是整次尝试被判为「成功、零输出」——
//     客户端看到一个没有任何内容的空回复。
func TestHasFirstTokenPayloadIgnoresComments(t *testing.T) {
	for _, raw := range []string{": heartbeat", ": ping", "data: : heartbeat"} {
		if hasFirstTokenPayload(raw) {
			t.Fatalf("hasFirstTokenPayload(%q) = true, want false", raw)
		}
	}
	// 非 JSON 行同样不是可翻译的负载。
	for _, raw := range []string{"hello", "[DONE]x", "{\"broken\""} {
		if hasFirstTokenPayload(raw) {
			t.Fatalf("hasFirstTokenPayload(%q) = true, want false", raw)
		}
	}
}

// 只发心跳然后关流的上游（额度耗尽时的真实形态）必须判为失败，
// 且一个帧都不该到达客户端。
func TestHeartbeatOnlyStreamIsEmptyStream(t *testing.T) {
	stream := newFakeUpstreamStream(4)
	stream.chunks <- []byte(": heartbeat\n\n")
	close(stream.chunks)

	oldOpen := openPumpStream
	openPumpStream = func(*http.Request) (upstreamHTTPStream, int, http.Header, error) {
		return stream, http.StatusOK, nil, nil
	}
	t.Cleanup(func() { openPumpStream = oldOpen })
	var emitted [][]byte
	oldEmit := emitStreamChunk
	emitStreamChunk = func(_ string, p []byte) error { emitted = append(emitted, p); return nil }
	t.Cleanup(func() { emitStreamChunk = oldEmit })

	req, _ := http.NewRequest(http.MethodPost, "https://x.invalid", nil)
	em, _, err := pumpAttemptWithTimeout(context.Background(), req, "s1", false,
		"m", "m", "a", "u", time.Now(), &sseUsageCollector{}, 5*time.Second)
	if err == nil {
		t.Fatal("只发心跳的流必须判为失败，而不是成功、零输出")
	}
	if !isEmptyStreamError(err) {
		t.Fatalf("应判为 empty_stream，实际: %v", err)
	}
	if em {
		t.Fatal("不得标记为已 emit")
	}
	if len(emitted) != 0 {
		t.Fatalf("心跳不得下发，实际 %d 帧: %q", len(emitted), emitted)
	}
}

// 心跳之后跟着真实内容：仍然正常成功，且心跳本身不被下发。
func TestHeartbeatThenContentSucceeds(t *testing.T) {
	stream := newFakeUpstreamStream(4)
	stream.chunks <- []byte(": heartbeat\n\n")
	stream.chunks <- []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
	close(stream.chunks)

	oldOpen := openPumpStream
	openPumpStream = func(*http.Request) (upstreamHTTPStream, int, http.Header, error) {
		return stream, http.StatusOK, nil, nil
	}
	t.Cleanup(func() { openPumpStream = oldOpen })
	var emitted [][]byte
	oldEmit := emitStreamChunk
	emitStreamChunk = func(_ string, p []byte) error { emitted = append(emitted, p); return nil }
	t.Cleanup(func() { emitStreamChunk = oldEmit })

	req, _ := http.NewRequest(http.MethodPost, "https://x.invalid", nil)
	em, _, err := pumpAttemptWithTimeout(context.Background(), req, "s2", false,
		"m", "m", "a", "u", time.Now(), &sseUsageCollector{}, 5*time.Second)
	if err != nil {
		t.Fatalf("正常流不得判失败: %v", err)
	}
	if !em {
		t.Fatal("有内容时必须标记为已 emit")
	}
	if len(emitted) != 1 {
		t.Fatalf("应只下发内容帧，实际 %d 帧: %q", len(emitted), emitted)
	}
}

// 同步路径同样不得把心跳下发。
func TestAggregateSSEWithCollectorSkipsComments(t *testing.T) {
	stream := ": heartbeat\ndata: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n"
	chunks, err := aggregateSSEWithCollector(strings.NewReader(stream), false, nil)
	if err != nil {
		t.Fatalf("不该报错: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("应只保留内容帧，实际 %d 帧", len(chunks))
	}
	for _, c := range chunks {
		if isStreamComment(string(c.Payload)) {
			t.Fatalf("心跳被下发: %q", c.Payload)
		}
	}
}
