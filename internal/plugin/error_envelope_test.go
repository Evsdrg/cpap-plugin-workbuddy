package main

import (
	"errors"
	"strings"
	"testing"
)

// 上游在流里返回业务错误信封（{"code":6004,"msg":"..."}）时，
// 它不是 chat.completion.chunk，协议转换器不认识它——
// 如果当成普通 chunk 透传，客户端只会看到一个无法翻译的负载，
// 表现为「空回复」。这里钉死两条路径都必须把它识别为错误。
func TestAggregateSSEWithCollectorRejectsErrorEnvelope(t *testing.T) {
	// 上游把限流错误塞进 200 的流里（CodeBuddy 在部分 realm/账号下如此）。
	stream := strings.Join([]string{
		`: heartbeat`,
		`data: {"code":6004,"msg":"usage exceeds frequency limit, reset at 2026-10-20 14:46:26 UTC+8"}`,
		`data: [DONE]`,
	}, "\n")

	chunks, err := aggregateSSEWithCollector(strings.NewReader(stream), false, nil)
	if err == nil {
		t.Fatalf("错误信封必须被识别为错误，实际返回 %d 个 chunk: %q", len(chunks), chunks)
	}
	if !isRateLimitResponse(0, err.Error()) {
		t.Fatalf("应识别为限流，实际错误: %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("错误信封不得变成 chunk，实际 %d 个", len(chunks))
	}
}

func TestAggregateSSEWithCollectorRejectsErrorObject(t *testing.T) {
	stream := `data: {"error":{"code":14018,"message":"credits exhausted"}}`
	if _, err := aggregateSSEWithCollector(strings.NewReader(stream), false, nil); err == nil {
		t.Fatal("error 对象同样必须被识别为错误")
	}
}

// 正常的空壳帧（只有 role、content 为空）不是错误——它必须继续被忽略而不是报错。
func TestAggregateSSEWithCollectorKeepsEmptyShell(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"id":"x","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":""}]}`,
		`data: [DONE]`,
	}, "\n")
	if _, err := aggregateSSEWithCollector(strings.NewReader(stream), false, nil); err != nil {
		t.Fatalf("空壳帧不是错误，却被判为: %v", err)
	}
}

func TestErrorEnvelopeDetection(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"限流信封", `{"code":6004,"msg":"usage exceeds frequency limit"}`, true},
		{"积分耗尽", `{"code":14018,"msg":"credits exhausted"}`, true},
		{"嵌套 error", `{"error":{"code":6004,"message":"rate limit"}}`, true},
		{"合法 chunk", `{"id":"x","object":"chat.completion.chunk","choices":[]}`, false},
		{"code 为 0", `{"code":0,"msg":"ok"}`, false},
		{"非 JSON", `hello`, false},
		{"空", ``, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := businessErrorOf(c.in)
			if (got != nil) != c.want {
				t.Fatalf("businessErrorOf(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

var _ = errors.New
