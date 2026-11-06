package main

import (
	"errors"
	"strings"
	"testing"
)

// 客户端只应看到一次 "upstream 429:" 前缀。
// 历史缺陷：failover 把 err.Error()（已含前缀）存进 Body，stream 层又拼一次，
// 客户端收到 "upstream 429: upstream 429: {...}"。
func TestFailoverErrorMessageDoesNotDoublePrefix(t *testing.T) {
	rendered := (&upstreamStatusError{status: 429, body: `{"code":6004,"msg":"frequency limit"}`}).Error()
	if !strings.HasPrefix(rendered, "upstream 429:") {
		t.Fatalf("前置条件：上游错误应自带前缀，实际 %q", rendered)
	}
	got := failoverErrorMessage(429, rendered, errors.New(rendered))
	if strings.Count(got, "upstream 429:") != 1 {
		t.Fatalf("前缀应只出现一次，实际 %q", got)
	}
	if !strings.Contains(got, "6004") {
		t.Fatalf("上游原文必须保留，实际 %q", got)
	}
}

// Body 是原始响应体（未渲染）时仍要补一次前缀。
func TestFailoverErrorMessagePrefixesRawBody(t *testing.T) {
	got := failoverErrorMessage(429, `{"code":6004}`, nil)
	if !strings.HasPrefix(got, "upstream 429: ") {
		t.Fatalf("原始体应补前缀，实际 %q", got)
	}
	if strings.Contains(got, "upstream 429: upstream") {
		t.Fatalf("不得重复，实际 %q", got)
	}
}

// 业务错误码形态（"upstream code 6004: ..."）同样不能重复。
func TestFailoverErrorMessageKeepsBusinessCodeForm(t *testing.T) {
	body := `upstream code 6004: {"code":6004,"msg":"frequency limit"}`
	got := failoverErrorMessage(200, body, nil)
	if strings.Count(got, "upstream code 6004") != 1 {
		t.Fatalf("业务码前缀不得重复，实际 %q", got)
	}
}
