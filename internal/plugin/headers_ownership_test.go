package main

import (
	"net/http"
	"strings"
	"testing"
)

// 端点归属是这套头的核心约束：CLI 协议线（chat / auth）与桌面端功能
// （billing / 签到 / 模型目录）使用两套互不相同的客户端身份，任何一边
// 串到另一边都会让上游按错误的客户端类型处理。
func TestCLIChatHeadersMatchCLIClient(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "https://www.codebuddy.ai/v2/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	sa := &storedAuth{
		Auth:    storedTokens{AccessToken: "tok", Domain: "www.codebuddy.ai"},
		Account: storedAccount{UID: "uid-1", EnterpriseID: "ent-1"},
	}
	backendHeaders(req, sa, "")

	// CLI 的 axiosToFetchAdapter 会设置的固定头。
	for name, want := range map[string]string{
		"X-IDE-Type":      "CLI",
		"X-IDE-Name":      "CLI",
		"X-IDE-Version":   cliVersion,
		"X-Agent-Intent":  "craft",
		"X-Agent-Runtime": "other",
		"X-Product":       "SaaS",
		"User-Agent":      cliUserAgent,
		"Content-Type":    "application/json",
		"Accept":          "application/json",
		"Authorization":   "Bearer tok",
		"X-User-Id":       "uid-1",
		"X-Enterprise-Id": "ent-1",
	} {
		if got := req.Header.Get(name); got != want {
			t.Errorf("chat header %s = %q, want %q", name, got, want)
		}
	}

	// CLI 模型请求不发桌面端浏览器式头，也不发认证端点的 X-No-* 抑制头。
	for _, name := range []string{
		"Origin", "Referer", "X-Requested-With",
		"X-No-Authorization", "X-No-User-Id", "X-No-Enterprise-Id", "X-No-Department-Info",
		"X-Private-Data", "X-Tenant-Id", "X-Domain", "X-Department-Info",
	} {
		if got := req.Header.Get(name); got != "" {
			t.Errorf("chat must not send desktop/auth header %s, got %q", name, got)
		}
	}

	// 会话四元组必须存在，且 Message-ID 与 Request-ID 同值（CLI 行为）。
	for _, name := range []string{"X-Conversation-ID", "X-Conversation-Request-ID", "X-Conversation-Message-ID", "X-Request-ID"} {
		if req.Header.Get(name) == "" {
			t.Errorf("chat header %s is missing", name)
		}
	}
	if a, b := req.Header.Get("X-Conversation-Message-ID"), req.Header.Get("X-Request-ID"); a != b {
		t.Errorf("X-Conversation-Message-ID = %q, X-Request-ID = %q; CLI keeps them identical", a, b)
	}
}

func TestCLIAuthHeadersSuppressIdentity(t *testing.T) {
	cases := []struct {
		name         string
		suppressAuth bool
		wantNoAuth   string
	}{
		{"auth/state and auth/token", true, "true"},
		{"login/account", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, "https://copilot.tencent.com/v2/plugin/auth/token?state=x", nil)
			cliTransportHeaders(req)
			cliSuppressIdentityHeaders(req, tc.suppressAuth)

			if got := req.Header.Get("X-No-Authorization"); got != tc.wantNoAuth {
				t.Errorf("X-No-Authorization = %q, want %q", got, tc.wantNoAuth)
			}
			// 后三个头在两种场景下都要置 true（CLI 用字符串 "true"，不是 "1"）。
			for _, name := range []string{"X-No-User-Id", "X-No-Enterprise-Id", "X-No-Department-Info"} {
				if got := req.Header.Get(name); got != "true" {
					t.Errorf("%s = %q, want \"true\"", name, got)
				}
			}
			if got := req.Header.Get("User-Agent"); got != cliUserAgent {
				t.Errorf("auth User-Agent = %q, want CLI UA %q", got, cliUserAgent)
			}
		})
	}
}

func TestDesktopHeadersMatchDesktopClient(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "https://www.codebuddy.ai/v2/billing/meter/daily-checkin", nil)
	sa := &storedAuth{
		Auth:    storedTokens{AccessToken: "tok", Domain: "www.codebuddy.ai"},
		Account: storedAccount{UID: "uid-1", EnterpriseID: "ent-1"},
	}
	billingHeaders(req, sa)

	for name, want := range map[string]string{
		"User-Agent":       desktopUA,
		"Origin":           originRefererGlobal,
		"Referer":          originRefererGlobal + "/",
		"X-Requested-With": "XMLHttpRequest",
		"Authorization":    "Bearer tok",
		"X-User-Id":        "uid-1",
		"X-Enterprise-Id":  "ent-1",
		"X-Tenant-Id":      "ent-1",
		"X-Domain":         "www.codebuddy.ai",
	} {
		if got := req.Header.Get(name); got != want {
			t.Errorf("desktop header %s = %q, want %q", name, got, want)
		}
	}

	// 桌面端不使用 CLI 身份头。
	for _, name := range []string{"X-IDE-Type", "X-IDE-Name", "X-IDE-Version", "X-Agent-Intent", "X-Agent-Runtime", "X-Private-Data"} {
		if got := req.Header.Get(name); got != "" {
			t.Errorf("desktop must not send CLI header %s, got %q", name, got)
		}
	}
}

func TestDesktopHeadersUseCNOriginForCNAccount(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "https://copilot.tencent.com/v2/billing/meter/daily-checkin", nil)
	billingHeaders(req, &storedAuth{Auth: storedTokens{AccessToken: "tok", Domain: "www.codebuddy.cn"}})
	if got := req.Header.Get("Origin"); got != originReferer {
		t.Errorf("CN Origin = %q, want %q", got, originReferer)
	}
	if got := req.Header.Get("User-Agent"); got != desktopUA {
		t.Errorf("CN desktop User-Agent = %q, want %q", got, desktopUA)
	}
}

// cache_retention 是 CLI 协议线特有的请求体字段，桌面端没有。
func TestPrepareUpstreamBodyInjectsCacheRetention(t *testing.T) {
	payload := []byte(`{"model":"deepseek-v4.1-flash","messages":[{"role":"system","content":"s"}]}`)
	out := prepareUpstreamBody(payload, nil, &storedAuth{}, "deepseek-v4.1-flash")
	if !strings.Contains(string(out), `"cache_retention":"long"`) {
		t.Fatalf("cache_retention missing from body: %s", out)
	}

	// 调用方显式给出时不得覆盖。
	explicit := []byte(`{"model":"m","messages":[],"cache_retention":"short"}`)
	out2 := prepareUpstreamBody(explicit, nil, &storedAuth{}, "m")
	if !strings.Contains(string(out2), `"cache_retention":"short"`) {
		t.Fatalf("explicit cache_retention overwritten: %s", out2)
	}
	if strings.Contains(string(out2), `"long"`) {
		t.Fatalf("body carries both cache_retention values: %s", out2)
	}
}
