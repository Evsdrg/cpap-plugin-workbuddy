package main

import (
	"encoding/json"
	"testing"
)

// 上游要求首条消息必须是 system，否则返回 code 11128
// "first message is not system prompt"。这条链路的真实触发路径是：
//
//	客户端 /v1/responses 的 input[0] 是 role=developer
//	  → 宿主 responses→chat 转换把 developer 降级成 user
//	  → messages[0] 变成 user，且整条 messages 里一条 system 都没有
//	  → 插件原样转发，上游 400
//
// 因此这里钉住的是「首条不是 system 就补一条」，而不是「完全没有 system 才补」。
func TestEnsureSystemMessageInjectsWhenFirstMessageIsNotSystem(t *testing.T) {
	cases := []struct {
		name      string
		payload   string
		wantFirst string
		wantCount int
	}{
		{
			name:      "首条是 user（responses developer 被降级后的真实形态）",
			payload:   `{"messages":[{"role":"user","content":"系统提示词"},{"role":"user","content":"hi"}]}`,
			wantFirst: "system",
			wantCount: 3,
		},
		{
			name:      "首条是 assistant",
			payload:   `{"messages":[{"role":"assistant","content":"先前的回复"},{"role":"user","content":"hi"}]}`,
			wantFirst: "system",
			wantCount: 3,
		},
		{
			name:      "首条已是 system：不重复注入",
			payload:   `{"messages":[{"role":"system","content":"已有"},{"role":"user","content":"hi"}]}`,
			wantFirst: "system",
			wantCount: 2,
		},
		{
			name:      "system 不在首位但存在：保持原样（重排会改对话语义）",
			payload:   `{"messages":[{"role":"user","content":"hi"},{"role":"system","content":"后置"}]}`,
			wantFirst: "system",
			wantCount: 3,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var obj map[string]any
			if err := json.Unmarshal([]byte(tc.payload), &obj); err != nil {
				t.Fatal(err)
			}
			ensureSystemMessageInPlace(obj, &storedAuth{})
			messages, _ := obj["messages"].([]any)
			if len(messages) != tc.wantCount {
				t.Fatalf("messages count = %d, want %d", len(messages), tc.wantCount)
			}
			first, _ := messages[0].(map[string]any)
			if role, _ := first["role"].(string); role != tc.wantFirst {
				t.Fatalf("messages[0].role = %q, want %q", role, tc.wantFirst)
			}
		})
	}
}

// CN 域账号同样需要这条保护：实测中 6 次 11128 失败全部落在
// domain=www.codebuddy.cn 的账号上。旧实现只在 Global 域生效，正好漏掉它们。
func TestEnsureSystemMessageAppliesToCNDomain(t *testing.T) {
	for _, domain := range []string{"www.codebuddy.cn", "www.codebuddy.ai", "www.workbuddy.ai", ""} {
		t.Run("domain="+domain, func(t *testing.T) {
			var obj map[string]any
			_ = json.Unmarshal([]byte(`{"messages":[{"role":"user","content":"hi"}]}`), &obj)
			sa := &storedAuth{}
			sa.Auth.Domain = domain
			if !ensureSystemMessageInPlace(obj, sa) {
				t.Fatalf("ensureSystemMessageInPlace returned false for domain %q", domain)
			}
			messages, _ := obj["messages"].([]any)
			first, _ := messages[0].(map[string]any)
			if role, _ := first["role"].(string); role != "system" {
				t.Fatalf("messages[0].role = %q, want system", role)
			}
		})
	}
}

// 空 messages 或无 messages 字段时不应崩溃，也不应凭空造一条消息。
func TestEnsureSystemMessageHandlesEmptyInput(t *testing.T) {
	for _, payload := range []string{`{}`, `{"messages":[]}`, `{"messages":"not-an-array"}`} {
		var obj map[string]any
		_ = json.Unmarshal([]byte(payload), &obj)
		if ensureSystemMessageInPlace(obj, &storedAuth{}) {
			t.Fatalf("payload %s should not be modified", payload)
		}
	}
}

// 完整链路：宿主转换后的真实 payload（首条 user、无 system）经
// prepareUpstreamBody 后，首条必须是 system。
func TestPrepareUpstreamBodyFixesDeveloperDowngrade(t *testing.T) {
	payload := []byte(`{
		"model":"deepseek-v4.1-flash",
		"messages":[
			{"role":"user","content":[{"type":"text","text":"你是一个编码助手"}]},
			{"role":"user","content":[{"type":"text","text":"say OK"}]}
		],
		"stream":true
	}`)
	sa := &storedAuth{}
	sa.Auth.Domain = "www.codebuddy.cn"

	out := prepareUpstreamBody(payload, nil, sa, "deepseek-v4.1-flash")
	var obj map[string]any
	if err := json.Unmarshal(out, &obj); err != nil {
		t.Fatalf("prepareUpstreamBody returned invalid JSON: %v", err)
	}
	messages, _ := obj["messages"].([]any)
	if len(messages) < 3 {
		t.Fatalf("messages count = %d, want >= 3 (system + 2 user)", len(messages))
	}
	first, _ := messages[0].(map[string]any)
	if role, _ := first["role"].(string); role != "system" {
		t.Fatalf("messages[0].role = %q, want system", role)
	}
	// 原有的两条 user 消息必须原样保留在 system 之后。
	if role, _ := messages[1].(map[string]any)["role"].(string); role != "user" {
		t.Fatalf("messages[1].role = %q, want user (original content must be preserved)", role)
	}
}

// 已有 system 首条时，prepareUpstreamBody 不得改变消息数量。
func TestPrepareUpstreamBodyKeepsExistingSystem(t *testing.T) {
	payload := []byte(`{
		"model":"deepseek-v4.1-flash",
		"messages":[
			{"role":"system","content":"已有系统提示"},
			{"role":"user","content":"hi"}
		]
	}`)
	sa := &storedAuth{}
	sa.Auth.Domain = "www.codebuddy.cn"

	out := prepareUpstreamBody(payload, nil, sa, "deepseek-v4.1-flash")
	var obj map[string]any
	_ = json.Unmarshal(out, &obj)
	messages, _ := obj["messages"].([]any)
	if len(messages) != 2 {
		t.Fatalf("messages count = %d, want 2 (no injection when system is already first)", len(messages))
	}
}
