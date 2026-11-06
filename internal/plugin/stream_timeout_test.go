package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

type fakeUpstreamStream struct {
	chunks chan []byte
	closed chan struct{}
	once   sync.Once
}

func newFakeUpstreamStream(capacity int) *fakeUpstreamStream {
	return &fakeUpstreamStream{chunks: make(chan []byte, capacity), closed: make(chan struct{})}
}

func (s *fakeUpstreamStream) Read() ([]byte, bool, error) {
	select {
	case chunk, ok := <-s.chunks:
		if !ok {
			return nil, true, nil
		}
		return chunk, false, nil
	default:
	}
	select {
	case <-s.closed:
		return nil, true, errors.New("stream closed")
	case chunk, ok := <-s.chunks:
		if !ok {
			return nil, true, nil
		}
		return chunk, false, nil
	}
}

func (s *fakeUpstreamStream) Close() {
	s.once.Do(func() { close(s.closed) })
}

func TestHasFirstTokenPayload(t *testing.T) {
	for _, raw := range []string{
		`{"choices":[{"delta":{"role":"assistant"}}]}`,
		`{"choices":[{"delta":{"content":""}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{}}]}}]}`,
	} {
		if hasFirstTokenPayload(raw) {
			t.Fatalf("hasFirstTokenPayload(%s) = true, want false", raw)
		}
	}
	for _, raw := range []string{
		`{"choices":[{"delta":{"content":"hello"}}]}`,
		`{"choices":[{"delta":{"reasoning_content":"thinking"}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"x\":"}}]}}]}`,
	} {
		if !hasFirstTokenPayload(raw) {
			t.Fatalf("hasFirstTokenPayload(%s) = false, want true", raw)
		}
	}
}

func TestPumpAttemptTimesOutOnRoleOnlyChunksAndClosesStream(t *testing.T) {
	stream := newFakeUpstreamStream(1)
	stream.chunks <- []byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
	oldOpen := openPumpStream
	openPumpStream = func(*http.Request) (upstreamHTTPStream, int, http.Header, error) {
		return stream, http.StatusOK, nil, nil
	}
	t.Cleanup(func() { openPumpStream = oldOpen })
	oldEmit := emitStreamChunk
	emitStreamChunk = func(string, []byte) error { return nil }
	t.Cleanup(func() { emitStreamChunk = oldEmit })

	request, err := http.NewRequest(http.MethodPost, "https://workbuddy.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = pumpAttemptWithTimeout(context.Background(), request, "stream-1", false,
		"deepseek-v4.1-flash", "deepseek-v4.1-flash", "idx-1", "uid-1", time.Now(),
		&sseUsageCollector{}, 20*time.Millisecond)
	if !isFirstTokenTimeout(err) {
		t.Fatalf("pump error = %v, want first-token timeout after role-only frame", err)
	}
	select {
	case <-stream.closed:
	case <-time.After(time.Second):
		t.Fatal("watchdog did not close upstream after role-only frame")
	}
}

func TestPumpAttemptBuffersRoleUntilFirstTokenThenPreservesOrder(t *testing.T) {
	stream := newFakeUpstreamStream(3)
	stream.chunks <- []byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
	stream.chunks <- []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
	close(stream.chunks)
	oldOpen := openPumpStream
	openPumpStream = func(*http.Request) (upstreamHTTPStream, int, http.Header, error) {
		return stream, http.StatusOK, nil, nil
	}
	t.Cleanup(func() { openPumpStream = oldOpen })
	var mu sync.Mutex
	var emitted [][]byte
	oldEmit := emitStreamChunk
	emitStreamChunk = func(_ string, payload []byte) error {
		mu.Lock()
		emitted = append(emitted, append([]byte(nil), payload...))
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { emitStreamChunk = oldEmit })

	request, err := http.NewRequest(http.MethodPost, "https://workbuddy.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	gotOutput, status, err := pumpAttemptWithTimeout(context.Background(), request, "stream-1", false,
		"deepseek-v4.1-flash", "deepseek-v4.1-flash", "idx-1", "uid-1", time.Now(),
		&sseUsageCollector{}, 100*time.Millisecond)
	if err != nil || !gotOutput || status != http.StatusOK {
		t.Fatalf("pump = emitted %v status %d err %v", gotOutput, status, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(emitted) != 2 || !bytes.Contains(emitted[0], []byte(`"role":"assistant"`)) || !bytes.Contains(emitted[1], []byte(`"content":"hello"`)) {
		t.Fatalf("emitted chunks = %q, want role then content", emitted)
	}
}

// TestPumpAttemptEmitsTerminalUsageFrameAfterFirstToken guards the regression
// where every post-first-token frame was parked in the pre-token buffer: the
// terminal chunk (finish_reason + usage) then never reached the client, and the
// host recorded zero tokens for the whole request.
func TestPumpAttemptEmitsTerminalUsageFrameAfterFirstToken(t *testing.T) {
	stream := newFakeUpstreamStream(4)
	stream.chunks <- []byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
	stream.chunks <- []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
	stream.chunks <- []byte("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1234,\"completion_tokens\":56,\"total_tokens\":1290}}\n\n")
	stream.chunks <- []byte("data: [DONE]\n\n")
	close(stream.chunks)
	oldOpen := openPumpStream
	openPumpStream = func(*http.Request) (upstreamHTTPStream, int, http.Header, error) {
		return stream, http.StatusOK, nil, nil
	}
	t.Cleanup(func() { openPumpStream = oldOpen })
	var mu sync.Mutex
	var emitted [][]byte
	oldEmit := emitStreamChunk
	emitStreamChunk = func(_ string, payload []byte) error {
		mu.Lock()
		emitted = append(emitted, append([]byte(nil), payload...))
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { emitStreamChunk = oldEmit })

	request, err := http.NewRequest(http.MethodPost, "https://workbuddy.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	collector := &sseUsageCollector{}
	gotOutput, status, err := pumpAttemptWithTimeout(context.Background(), request, "stream-1", false,
		"deepseek-v4.1-flash", "deepseek-v4.1-flash", "idx-1", "uid-1", time.Now(),
		collector, time.Second)
	if err != nil || !gotOutput || status != http.StatusOK {
		t.Fatalf("pump = emitted %v status %d err %v", gotOutput, status, err)
	}
	mu.Lock()
	defer mu.Unlock()
	var sawUsage bool
	for _, chunk := range emitted {
		if bytes.Contains(chunk, []byte(`"usage"`)) {
			sawUsage = true
		}
	}
	if !sawUsage {
		t.Fatalf("terminal usage frame was not emitted; emitted = %q", emitted)
	}
	if detail := collector.detail(); detail.InputTokens != 1234 || detail.OutputTokens != 56 {
		t.Fatalf("collector detail = %+v, want 1234/56 from the terminal frame", detail)
	}
}

func TestHostHTTPStreamCloseUnblocksConcurrentRead(t *testing.T) {
	readStarted := make(chan struct{})
	readReturned := make(chan struct{})
	closed := make(chan struct{})
	var closeOnce sync.Once

	stream := &hostHTTPStream{
		streamID: "test-stream",
		readChunk: func() ([]byte, error) {
			close(readStarted)
			<-closed
			close(readReturned)
			return nil, errors.New("stream closed")
		},
		closeFn: func() { closeOnce.Do(func() { close(closed) }) },
	}
	readResult := make(chan error, 1)
	go func() {
		_, _, err := stream.Read()
		readResult <- err
	}()
	<-readStarted
	stream.Close()
	select {
	case err := <-readResult:
		if err == nil {
			t.Fatal("Read() error = nil, want closed stream")
		}
	case <-time.After(time.Second):
		t.Fatal("Close() failed to unblock concurrent Read()")
	}
	<-readReturned
}
