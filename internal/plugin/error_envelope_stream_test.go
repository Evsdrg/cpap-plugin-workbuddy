package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// 端到端：上游在 200 的流里回一个限流信封时，pumpAttemptWithTimeout
// 必须把这次尝试认定为失败，并且错误内容保留上游原文（含重置时间）。
// 修复前它会把这个信封当 chunk 交给宿主，客户端最终看到空回复。
func TestPumpAttemptTreatsBusinessEnvelopeAsError(t *testing.T) {
	stream := newFakeUpstreamStream(4)
	stream.chunks <- []byte("data: {\"code\":6004,\"msg\":\"usage exceeds frequency limit, reset at 2026-10-20 14:46:26 UTC+8\"}\n\n")
	close(stream.chunks)

	oldOpen := openPumpStream
	openPumpStream = func(*http.Request) (upstreamHTTPStream, int, http.Header, error) {
		return stream, http.StatusOK, nil, nil
	}
	t.Cleanup(func() { openPumpStream = oldOpen })

	var emittedPayloads [][]byte
	oldEmit := emitStreamChunk
	emitStreamChunk = func(_ string, payload []byte) error {
		emittedPayloads = append(emittedPayloads, payload)
		return nil
	}
	t.Cleanup(func() { emitStreamChunk = oldEmit })

	request, err := http.NewRequest(http.MethodPost, "https://workbuddy.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	emitted, status, err := pumpAttemptWithTimeout(context.Background(), request, "stream-1", false,
		"deepseek-v4.1-flash", "deepseek-v4.1-flash", "idx-1", "uid-1", time.Now(),
		&sseUsageCollector{}, 5*time.Second)
	if err == nil {
		t.Fatalf("限流信封必须让本次尝试失败，实际 err=nil（emitted=%v status=%d）", emitted, status)
	}
	if emitted {
		t.Fatal("错误信封不得标记为已 emit")
	}
	if len(emittedPayloads) != 0 {
		t.Fatalf("错误信封不得作为 chunk 下发，实际下发了 %d 个: %q", len(emittedPayloads), emittedPayloads)
	}
	if !businessErrorRecognized(err) {
		t.Fatalf("错误必须可被识别为限流，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "reset at") && !strings.Contains(err.Error(), "6004") {
		t.Fatalf("错误应保留上游原文（含重置时间），实际: %v", err)
	}
}

func businessErrorRecognized(err error) bool {
	if err == nil {
		return false
	}
	var ue *upstreamStatusError
	return errors.As(err, &ue) && (ue.code == 6004 || isRateLimitResponse(0, err.Error()))
}
