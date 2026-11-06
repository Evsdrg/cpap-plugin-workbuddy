// stream.go owns the upstream SSE data plane: emitting cleaned chunks back to
// the host stream (streamEmit/close), pumping the upstream SSE in a goroutine
// with 429 failover (pumpWithFailover), collecting it synchronously
// (collectStreamAttempt in main.go), and the SSE-frame helpers that re-frame,
// filter, and aggregate chunks.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// streamEmit pushes one chunk payload to the host stream. Returns an error if
// the host rejected it (e.g. the client already disconnected and the stream
// was closed), which the pump uses to stop reading a dead upstream.
func streamEmit(streamID string, payload []byte) error {
	if streamID == "" {
		return fmt.Errorf("no stream id")
	}
	return hostStreamEmitRaw(streamID, payload, "", 0)
}

// streamEmitError pushes the terminal error of a failed stream to the host.
//
// The error MUST travel in the emit request's dedicated "error" field, not as
// a payload frame: the host turns a non-empty error field into a chunk carrying
// chunk.Err (pluginhost/stream_bridge.go), and the conductor's bootstrap read
// (readStreamBootstrap) treats chunk.Err as the attempt's real outcome — so the
// client receives "upstream 429: ..." instead of the host synthesizing
// empty_stream, and rate-limit cooldowns fire on the real status.
//
// Encoding the error as a {"error":...} payload frame (the pre-0.9.14 shape)
// loses it: the payload frame translates to nothing, the stream then closes
// with no error, and the conductor reports empty_stream + Retryable, which
// re-runs the whole plugin call and hides the upstream's own message.
func streamEmitError(streamID, message string, status int) {
	if streamID == "" {
		return
	}
	// A-37：绝不把可能含 Bearer/JWT 的上游原始响应体原样发出。
	_ = hostStreamEmitRaw(streamID, nil, redactSecrets(message), status)
}

func streamClose(streamID string) {
	if streamID == "" {
		return
	}
	// 错误已由 streamEmitError 交付；这次 close 只负责结束流。若在这里再带
	// 错误，宿主会把它追加到同一个队列，客户端会看到同一条失败两次。
	_, _ = abi.HostCall(pluginabi.MethodHostStreamClose, map[string]any{"stream_id": streamID})
}

func streamHeaders() http.Header {
	h := http.Header{}
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	return h
}

// pumpWithFailover is the async-stream pump wrapped in the 429 failover loop
// (v0.8.8). Each eligible account is tried in order; a rate limit observed
// BEFORE the first chunk is emitted marks the account and moves to the next.
// Once any chunk reached the client the stream is committed and never
// replayed. Usage is recorded once per inbound request: success on the
// answering account, one failure line when every attempt was rate-limited.
func pumpWithFailover(ctx context.Context, cancel context.CancelFunc, req executorStreamRequest, body []byte, upstreamModel, authUID string, started time.Time, sseFramed bool, cands []*failoverCandidate) {
	if cancel != nil {
		defer cancel()
	}
	streamID := req.StreamID
	closed := false
	closeOnce := func() {
		if closed {
			return
		}
		closed = true
		streamClose(streamID)
	}
	defer closeOnce()

	collector := &sseUsageCollector{}
	fin := runStreamFailover(ctx, cands, req.Model, func(cand *failoverCandidate) (bool, int, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointChatFor(cand.Auth), bytes.NewReader(body))
		if err != nil {
			return false, 0, err
		}
		backendHeaders(httpReq, cand.Auth, canonicalSessionID(req.Metadata))
		return pumpAttemptWithTimeout(ctx, httpReq, streamID, sseFramed, req.Model, upstreamModel, cand.AuthID, candUID(cand, authUID), started, collector, firstTokenTimeout)
	})

	switch fin.Outcome {
	case outcomeSuccess:
		publishUsage(req.Model, upstreamModel, candUID(findCandidate(cands, fin.AuthID), authUID), started, collector.detail(), false, 0, "")
		recordUsageMap(req.Model, upstreamModel, req.AuthID, authUID, started, collector.lastMap(), false)
		onFailoverSuccess(req.AuthID, fin.AuthID, authUID)
	case outcomeModelCooldown:
		publishUsage(req.Model, upstreamModel, authUID, started, usage.Detail{}, true, fin.Status, fin.Body)
		recordUsageMap(req.Model, upstreamModel, req.AuthID, authUID, started, collector.lastMap(), true)
		streamEmitError(streamID, "model quota presumed exhausted until the next Asia/Shanghai day", 0)
	case outcomeRateLimited:
		// 所有可用账号都在发出任何 chunk 之前被限流。
		//
		// fin.Body 存的已经是渲染好的错误文本（failover 把 err.Error() 放进 Body，
		// 而 upstreamStatusError.Error() 自带 "upstream %d: " 前缀）。这里再拼一次
		// 会得到 "upstream 429: upstream 429: {...}"，所以统一交给
		// failoverErrorMessage 渲染，它保证只加一次前缀。
		publishUsage(req.Model, upstreamModel, authUID, started, usage.Detail{}, true, fin.Status, fin.Body)
		recordUsageMap(req.Model, upstreamModel, req.AuthID, authUID, started, collector.lastMap(), true)
		streamEmitError(streamID, failoverErrorMessage(fin.Status, fin.Body, fin.Err), fin.Status)
	default: // outcomeStop
		if fin.Emitted {
			// Chunks were already streamed when the upstream failed — the
			// client has partial data. Record the attempt as failed (upstream
			// may have billed tokens seen in the usage chunks) and emit the
			// terminal error frame.
			publishUsage(req.Model, upstreamModel, authUID, started, collector.detail(), true, fin.Status, fin.Body)
			recordUsageMap(req.Model, upstreamModel, req.AuthID, authUID, started, collector.lastMap(), true)
			if fin.Status > 0 {
				streamEmitError(streamID, failoverErrorMessage(fin.Status, fin.Body, fin.Err), fin.Status)
			} else if fin.Err != nil {
				streamEmitError(streamID, fmt.Sprintf("upstream stream read error: %v", fin.Err), 0)
			}
			return
		}
		publishUsage(req.Model, upstreamModel, authUID, started, usage.Detail{}, true, fin.Status, fin.Body)
		recordUsageMap(req.Model, upstreamModel, req.AuthID, authUID, started, collector.lastMap(), true)
		if fin.Status > 0 && !isRateLimitResponse(fin.Status, fin.Body) {
			go reconcileByUID(authUID, fin.Status, fin.Body)
		}
		streamEmitError(streamID, failoverErrorMessage(fin.Status, fin.Body, fin.Err), fin.Status)
	}
}

// failoverErrorMessage formats the client-facing error for a failed stream:
// upstream HTTP errors keep their status prefix, plain errors (empty_stream,
// transport) surface their message.
func failoverErrorMessage(status int, body string, err error) string {
	// Body 有两种来源：上游原始响应体（未渲染），以及 failover 存的 err.Error()
	// （upstreamStatusError 自带 "upstream %d: " 前缀）。后者不能再拼一次，
	// 否则客户端会看到 "upstream 429: upstream 429: {...}"。
	body = strings.TrimSpace(body)
	if status > 0 {
		if strings.HasPrefix(body, fmt.Sprintf("upstream %d:", status)) || strings.HasPrefix(body, "upstream code ") {
			return truncateRedacted(body, 200)
		}
		if body != "" {
			return fmt.Sprintf("upstream %d: %s", status, truncateRedacted(body, 200))
		}
	}
	if err != nil {
		return err.Error()
	}
	if body != "" {
		return truncateRedacted(body, 200)
	}
	return "workbuddy stream request failed"
}

// pumpAttempt runs one upstream SSE pump against one account. Returns
// (emitted, statusCode, err): emitted reports whether any chunk reached the
// client, statusCode the upstream HTTP status (0 for transport failures).
// A 200 stream that closes without any data chunk is surfaced as
// errEmptyStream so the failover loop switches to the next account (this
// account's quota for the model is spent) — never retrying the same one, and
// never quarantining it before emptyStreamStrikeThreshold consecutive strikes.
// The client gets a real error instead of a silent success.
var openPumpStream = func(req *http.Request) (upstreamHTTPStream, int, http.Header, error) {
	return hostHTTPDoStream(req)
}

var emitStreamChunk = streamEmit

// hostStreamEmitRaw is the seam under every stream emit: production performs
// the real host RPC, tests replace it to observe the raw request wire without a
// cgo host. streamEmit and streamEmitError both build their request bodies in
// production code, so tests exercise the actual wire shape.
var hostStreamEmitRaw = func(streamID string, payload []byte, errorMessage string, status int) error {
	request := map[string]any{"stream_id": streamID}
	if len(payload) > 0 {
		request["payload"] = payload
	}
	if errorMessage != "" {
		request["error"] = errorMessage
	}
	// status 让宿主把终端错误映射回上游真实状态码（429 保持 429 而不是 500），
	// 客户端才能据状态码决定退避；0 表示无 HTTP 状态（transport、empty_stream 等）。
	if status > 0 {
		request["status"] = status
	}
	_, err := abi.HostCall(pluginabi.MethodHostStreamEmit, request)
	return err
}

func pumpAttempt(httpReq *http.Request, streamID string, sseFramed bool, requestedModel, upstreamModel, authID, authUID string, started time.Time, collector *sseUsageCollector) (bool, int, error) {
	return pumpAttemptWithTimeout(context.Background(), httpReq, streamID, sseFramed, requestedModel, upstreamModel, authID, authUID, started, collector, firstTokenTimeout)
}

func pumpAttemptWithTimeout(ctx context.Context, httpReq *http.Request, streamID string, sseFramed bool, requestedModel, upstreamModel, authID, authUID string, started time.Time, collector *sseUsageCollector, timeout time.Duration) (bool, int, error) {
	stream, statusCode, _, err := openPumpStream(httpReq)
	if err != nil {
		return false, 0, fmt.Errorf("http_error: %w", err)
	}
	defer stream.Close()
	if statusCode >= 400 {
		errPayload, _ := io.ReadAll(newHostStreamReader(stream))
		return false, statusCode, fmt.Errorf("upstream %d: %s", statusCode, truncateRedacted(string(errPayload), 200))
	}
	reader := newHostStreamReader(stream)
	lines := make(chan streamLineResult)
	stopRead := make(chan struct{})
	go scanStreamLines(reader, lines, stopRead)
	firstTokenTimer := time.NewTimer(timeout)
	firstTokenTimeoutCh := firstTokenTimer.C
	defer firstTokenTimer.Stop()
	emitted := false
	firstTokenSeen := false
	var preTokenChunks [][]byte
	for {
		select {
		case <-firstTokenTimeoutCh:
			close(stopRead)
			stream.Close()
			return false, statusCode, &firstTokenTimeoutError{model: requestedModel, resetAt: nextChinaMidnight(time.Now())}
		case line, ok := <-lines:
			if !ok {
				if !emitted {
					return false, statusCode, errEmptyStream
				}
				return emitted, statusCode, nil
			}
			if line.err != nil {
				return emitted, 0, line.err
			}
			content := stripDataPrefix(line.text)
			if content == "" || content == "[DONE]" {
				continue
			}
			// SSE 注释（上游的 ": heartbeat"）既不是数据帧，也不是首 token。
			// 但它证明上游连接还活着：把首 token 看门狗续期，避免把
			// 长时间思考（实测有 290s 才出首 token 的合法请求）误判为超时。
			// 「只发心跳然后关流」不会被这段放过——关流走下面的 !ok 分支，
			// 那里仍然返回 empty_stream。
			if isStreamComment(content) {
				if !firstTokenSeen && firstTokenTimeoutCh != nil {
					if !firstTokenTimer.Stop() {
						select {
						case <-firstTokenTimer.C:
						default:
						}
					}
					firstTokenTimer.Reset(timeout)
				}
				continue
			}
			// 上游会把限流（6004）/积分耗尽（14018）等业务错误塞进 200 的流里，
			// 形式是 {"code":...} 或 {"error":{...}} 的 JSON 信封。它不是
			// chat.completion.chunk，协议转换器不认识，透传出去就是「空回复」。
			// 在这里识别并作为本次尝试的错误返回，交给 failover 按真实原因处理。
			if env, _ := businessErrorOf(content); env != nil {
				close(stopRead)
				stream.Close()
				return emitted, statusCode, businessErrorAsError(env)
			}
			collector.feed(content)
			cleaned := cleanChunkJSON(content)
			if cleaned == "" {
				continue
			}
			if sseFramed {
				cleaned = "data: " + cleaned
			}
			// Buffer empty-shell frames only while the watchdog is armed. After
			// the first real output every frame is forwarded as-is: the terminal
			// frame carries finish_reason and the usage counters, and holding it
			// back would strip usage from the wire (host records zero tokens).
			if !firstTokenSeen {
				if !hasFirstTokenPayload(content) {
					preTokenChunks = append(preTokenChunks, []byte(cleaned))
					continue
				}
				for _, buffered := range preTokenChunks {
					if err := emitStreamChunk(streamID, buffered); err != nil {
						close(stopRead)
						stream.Close()
						return emitted, 0, fmt.Errorf("stream_emit: %w", err)
					}
					emitted = true
				}
				preTokenChunks = nil
				firstTokenSeen = true
				if !firstTokenTimer.Stop() {
					select {
					case <-firstTokenTimer.C:
					default:
					}
				}
				firstTokenTimeoutCh = nil
			}
			if err := emitStreamChunk(streamID, []byte(cleaned)); err != nil {
				close(stopRead)
				stream.Close()
				return emitted, 0, fmt.Errorf("stream_emit: %w", err)
			}
			emitted = true
		}
	}
}

type streamLineResult struct {
	text string
	err  error
}

func scanStreamLines(reader *hostStreamReader, out chan<- streamLineResult, stop <-chan struct{}) {
	defer close(out)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		select {
		case <-stop:
			return
		case out <- streamLineResult{text: scanner.Text()}:
		}
	}
	if err := scanner.Err(); err != nil {
		select {
		case <-stop:
		case out <- streamLineResult{err: err}:
		}
	}
}

// isStreamComment reports whether a line is an SSE comment rather than a data
// frame. CodeBuddy opens every chat stream with ": heartbeat", and our
// line-oriented reader sees it as a standalone line. Comments carry no data:
// they must never be emitted as a payload frame, and a stream that only ever
// produced comments has produced nothing at all.
func isStreamComment(s string) bool {
	return strings.HasPrefix(strings.TrimSpace(s), ":")
}

func hasFirstTokenPayload(raw string) bool {
	// SSE 注释（": heartbeat"）不是数据；非 JSON 行同样不是可翻译的负载。
	// 早期实现把「非空字符串」当作有 payload，于是心跳会解除首 token 看门狗
	// 并把整次尝试标记为「成功」——上游只发心跳就关流时，客户端收到的是
	// 一个没有任何内容的空回复。宿主自己的 IsChatTokenEvent 也只认 JSON
	// 里的 choices/finish_reason，这里与它对齐。
	if isStreamComment(raw) {
		return false
	}
	var payload map[string]any
	if json.Unmarshal([]byte(raw), &payload) != nil {
		return false
	}
	choices, _ := payload["choices"].([]any)
	for _, rawChoice := range choices {
		choice, _ := rawChoice.(map[string]any)
		delta, _ := choice["delta"].(map[string]any)
		for _, key := range []string{"content", "reasoning_content", "refusal"} {
			if text, ok := delta[key].(string); ok && strings.TrimSpace(text) != "" {
				return true
			}
		}
		if function, ok := delta["function_call"].(map[string]any); ok {
			if hasNonEmptyString(function, "name") || hasNonEmptyString(function, "arguments") {
				return true
			}
		}
		if calls, ok := delta["tool_calls"].([]any); ok {
			for _, rawCall := range calls {
				call, _ := rawCall.(map[string]any)
				function, _ := call["function"].(map[string]any)
				if hasNonEmptyString(function, "name") || hasNonEmptyString(function, "arguments") {
					return true
				}
			}
		}
	}
	return false
}

func hasNonEmptyString(values map[string]any, key string) bool {
	if values == nil {
		return false
	}
	value, _ := values[key].(string)
	return strings.TrimSpace(value) != ""
}

// clientNeedsSSEFrame reports whether chunk payloads must carry their own
// "data: " SSE framing. CPA's chat-completions passthrough adds the prefix
// itself, but every cross-format response translator (claude/gemini/codex/...)
// only consumes payloads already framed as "data: " lines. The host hands the
// plugin the inbound request path in Metadata, so we frame chunks ourselves for
// any entry path other than the native OpenAI chat-completions one.
func clientNeedsSSEFrame(metadata map[string]any) bool {
	path, _ := metadata["request_path"].(string)
	switch strings.ToLower(strings.TrimSpace(path)) {
	case "/v1/chat/completions", "/v1/completions":
		return false
	default:
		return true
	}
}

// aggregateSSEWithCollector reads an upstream SSE stream and emits one chunk
// per data event. Empty tool-call shells are stripped and the trailing [DONE]
// is dropped (the host appends its own stream terminator). When sseFramed is
// true each payload is emitted as a "data: " line for cross-format
// translators; otherwise the payload is the raw JSON object and the host
// chat-completions writer adds the framing itself. A mid-stream read error
// aborts collection and is returned so the caller records the attempt as
// failed. The collector, when non-nil, observes raw upstream chunks for usage
// extraction.
func aggregateSSEWithCollector(r io.Reader, sseFramed bool, collector *sseUsageCollector) ([]pluginapi.ExecutorStreamChunk, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var chunks []pluginapi.ExecutorStreamChunk
	for scanner.Scan() {
		content := stripDataPrefix(scanner.Text())
		if content == "" || content == "[DONE]" || isStreamComment(content) {
			continue
		}
		// 同 pumpAttemptWithTimeout：业务错误信封必须变成错误，
		// 不能作为 chunk 交给宿主（否则跨协议转换后就是空回复）。
		if env, _ := businessErrorOf(content); env != nil {
			return nil, businessErrorAsError(env)
		}
		if collector != nil {
			collector.feed(content)
		}
		cleaned := cleanChunkJSON(content)
		if cleaned == "" {
			continue
		}
		if sseFramed {
			cleaned = "data: " + cleaned
		}
		chunks = append(chunks, pluginapi.ExecutorStreamChunk{Payload: []byte(cleaned)})
	}
	if err := scanner.Err(); err != nil {
		return chunks, fmt.Errorf("upstream stream read error: %w", err)
	}
	return chunks, nil
}

// cleanChunkJSON strips only the known-problematic empty tool-call shells
// from choice deltas: a null/empty function_call and an empty tool_calls array
// (CodeBuddy emits these on the terminal chunk, and strict clients interpret
// them as a truncated tool call). Other empty-but-legal values are preserved:
// content:"" is a valid delta (pure tool-call chunk) and the role-only first
// chunk must survive so clients can establish the message role.
func cleanChunkJSON(s string) string {
	var obj map[string]any
	if json.Unmarshal([]byte(s), &obj) != nil {
		return s
	}
	changed := false
	if choices, ok := obj["choices"].([]any); ok {
		for _, c := range choices {
			choice, ok := c.(map[string]any)
			if !ok {
				continue
			}
			delta, ok := choice["delta"].(map[string]any)
			if !ok {
				continue
			}
			if v, present := delta["function_call"]; present && isEmptyValue(v) {
				delete(delta, "function_call")
				changed = true
			}
			if v, present := delta["tool_calls"]; present {
				if arr, isArr := v.([]any); isArr && len(arr) == 0 {
					delete(delta, "tool_calls")
					changed = true
				}
			}
			// Upstream often pads terminal/noop deltas with empty noise fields
			// that clients ignore but pollute wire size / some parsers.
			for _, noise := range []string{"extra_fields", "refusal", "reasoning_content"} {
				if v, present := delta[noise]; present && isEmptyValue(v) {
					delete(delta, noise)
					changed = true
				}
			}
			// Drop a fully-empty delta ONLY when the choice carries no other
			// signal (no finish_reason): e.g. {"delta":{"function_call":null}}
			// reduced to {}. A delta with role/content:"" is meaningful and
			// never reaches this branch (those fields are preserved above).
			if len(delta) == 0 {
				if fr, _ := choice["finish_reason"].(string); fr == "" {
					return ""
				}
			}
		}
	}
	if !changed {
		return s
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return s
	}
	return string(out)
}

// aggregateCompletion folds a full upstream SSE stream into one
// chat.completion object. Legacy wrapper kept for tests; production callers
// use aggregateCompletionCounted so empty streams can be distinguished.
func aggregateCompletion(r io.Reader, model string) ([]byte, error) {
	out, _, err := aggregateCompletionCounted(r, model)
	return out, err
}

// aggregateCompletionCounted folds a full upstream SSE stream into one
// chat.completion object and reports whether any data chunk was observed.
// sawData=false means the upstream closed before producing a single chunk
// (callers decide whether that is an error).
func aggregateCompletionCounted(r io.Reader, model string) ([]byte, bool, error) {
	var content, reasoning, role, respModel, respID, finish string
	var created int64
	var usage map[string]any
	sawData := false
	// tool_calls arrive as streaming deltas: each chunk carries an index plus a
	// partial call (id/type/function.name on the first delta, argument text
	// fragments afterwards). Merge by index instead of appending raw fragments
	// so the folded completion holds whole calls.
	toolCalls := map[int]map[string]any{}
	var toolOrder []int
	var scanErr error

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		data := stripDataPrefix(scanner.Text())
		if data == "" || data == "[DONE]" {
			continue
		}
		sawData = true
		var chunk map[string]any
		if json.Unmarshal([]byte(data), &chunk) != nil {
			continue
		}
		if v, ok := chunk["id"].(string); ok && v != "" {
			respID = v
		}
		if v, ok := chunk["model"].(string); ok && v != "" {
			respModel = v
		}
		if v, ok := chunk["created"].(float64); ok {
			created = int64(v)
		}
		if v, ok := chunk["usage"].(map[string]any); ok {
			usage = v
		}
		choices, _ := chunk["choices"].([]any)
		for _, c := range choices {
			choice, _ := c.(map[string]any)
			if delta, ok := choice["delta"].(map[string]any); ok {
				if v, ok := delta["role"].(string); ok && v != "" {
					role = v
				}
				if v, ok := delta["content"].(string); ok {
					content += v
				}
				if v, ok := delta["reasoning_content"].(string); ok {
					reasoning += v
				}
				if tcs, ok := delta["tool_calls"].([]any); ok {
					for _, tc := range tcs {
						call, ok := tc.(map[string]any)
						if !ok {
							continue
						}
						idx := 0
						if v, ok := call["index"].(float64); ok {
							idx = int(v)
						}
						merged, seen := toolCalls[idx]
						if !seen {
							merged = map[string]any{"index": idx}
							toolCalls[idx] = merged
							toolOrder = append(toolOrder, idx)
						}
						mergeToolCallDelta(merged, call)
					}
				}
			}
			if v, ok := choice["finish_reason"].(string); ok && v != "" {
				finish = v
			}
		}
	}
	if err := scanner.Err(); err != nil {
		scanErr = err
	}
	// A mid-stream read failure means the folded completion is truncated. The
	// host discards the payload entirely when the plugin returns an error
	// (sdk/api/handlers executeWithPluginExecutor), so fail fast here instead
	// of assembling a partial completion nobody can safely consume.
	if scanErr != nil {
		return nil, sawData, fmt.Errorf("upstream stream read error: %w", scanErr)
	}

	message := map[string]any{"role": firstNonEmpty(role, "assistant"), "content": content}
	if reasoning != "" {
		message["reasoning_content"] = reasoning
	}
	if len(toolOrder) > 0 {
		sort.Ints(toolOrder)
		calls := make([]map[string]any, 0, len(toolOrder))
		for _, idx := range toolOrder {
			calls = append(calls, toolCalls[idx])
		}
		message["tool_calls"] = calls
	}
	if created == 0 {
		created = time.Now().Unix()
	}
	result := map[string]any{
		"id":      firstNonEmpty(respID, "chatcmpl-workbuddy"),
		"object":  "chat.completion",
		"created": created,
		"model":   firstNonEmpty(respModel, model),
		"choices": []map[string]any{{
			"index":         0,
			"message":       message,
			"finish_reason": firstNonEmpty(finish, "stop"),
		}},
	}
	if usage != nil {
		result["usage"] = usage
	}
	out, err := json.Marshal(result)
	if err != nil {
		return nil, sawData, err
	}
	return out, sawData, nil
}

// mergeToolCallDelta folds one streaming tool_call fragment into the merged
// call: scalar fields (id/type) are taken when first seen, function.name is
// concatenated (upstream may split it), and function.arguments text fragments
// are appended in arrival order.
func mergeToolCallDelta(merged, delta map[string]any) {
	for _, k := range []string{"id", "type"} {
		if _, present := merged[k]; !present {
			if v, ok := delta[k].(string); ok && v != "" {
				merged[k] = v
			}
		}
	}
	dfn, _ := delta["function"].(map[string]any)
	if dfn == nil {
		return
	}
	mfn, _ := merged["function"].(map[string]any)
	if mfn == nil {
		mfn = map[string]any{}
		merged["function"] = mfn
	}
	if v, ok := dfn["name"].(string); ok && v != "" {
		cur, _ := mfn["name"].(string)
		mfn["name"] = cur + v
	}
	if v, ok := dfn["arguments"].(string); ok && v != "" {
		cur, _ := mfn["arguments"].(string)
		mfn["arguments"] = cur + v
	}
}

func stripDataPrefix(s string) string {
	s = strings.TrimSpace(s)
	for strings.HasPrefix(s, "data:") {
		s = strings.TrimSpace(strings.TrimPrefix(s, "data:"))
	}
	return s
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
