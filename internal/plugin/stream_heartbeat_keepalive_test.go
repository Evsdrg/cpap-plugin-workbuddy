package main

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// 心跳续命：持续心跳 + 迟迟不出首 token 的合法长思考流，
// 不应被看门狗误杀；心跳本身仍不得下发。
func TestHeartbeatsKeepWatchdogAliveButAreNotEmitted(t *testing.T) {
	stream := newFakeUpstreamStream(64)
	oldOpen := openPumpStream
	openPumpStream = func(*http.Request) (upstreamHTTPStream, int, http.Header, error) {
		return stream, http.StatusOK, nil, nil
	}
	t.Cleanup(func() { openPumpStream = oldOpen })
	var emitted [][]byte
	oldEmit := emitStreamChunk
	emitStreamChunk = func(_ string, p []byte) error { emitted = append(emitted, p); return nil }
	t.Cleanup(func() { emitStreamChunk = oldEmit })

	// 每 60ms 一个心跳，看门狗 200ms：总时长远超阈值，但心跳持续续期。
	go func() {
		for i := 0; i < 8; i++ {
			time.Sleep(60 * time.Millisecond)
			stream.chunks <- []byte(": heartbeat\n\n")
		}
		stream.chunks <- []byte("data: {\"choices\":[{\"delta\":{\"content\":\"late\"}}]}\n\n")
		close(stream.chunks)
	}()

	req, _ := http.NewRequest(http.MethodPost, "https://x.invalid", nil)
	em, _, err := pumpAttemptWithTimeout(context.Background(), req, "s1", false,
		"m", "m", "a", "u", time.Now(), &sseUsageCollector{}, 200*time.Millisecond)
	t.Logf("  emitted=%v err=%v 下发帧数=%d", em, err, len(emitted))
	if err != nil {
		t.Fatalf("心跳续期应让长思考流活下来，实际判失败: %v", err)
	}
	for _, f := range emitted {
		if isStreamComment(string(f)) {
			t.Fatalf("心跳不得下发: %q", f)
		}
	}
}
