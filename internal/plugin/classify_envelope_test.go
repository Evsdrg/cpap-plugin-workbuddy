package main

import (
	"context"
	"testing"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
)

// 上游业务错误码与 HTTP 状态码互相独立。CodeBuddy 把限流/额度错误塞进 HTTP 200
// 的响应体（见 error_envelope.go），此时 status 是 0 或 200，只有 code 能表达
// 失败性质。只按 status 分类会让这类错误落到 outcomeStop：账号不切换，上游原文
// 直接漏给客户端。
func TestClassifyUpstreamFailureUsesBusinessCode(t *testing.T) {
	cases := []struct {
		name         string
		status, code int
		body         string
		want         attemptOutcome
	}{
		{"HTTP429 限流", 429, 0, "", outcomeRateLimited},
		{"HTTP402 积分耗尽", 402, 0, "", outcomeAccountExhausted},
		{"HTTP200 信封 6004", 200, codeFrequencyLimit, `{"code":6004,"msg":"usage exceeds frequency limit"}`, outcomeRateLimited},
		{"HTTP200 信封 14018", 200, codeCreditsExhausted, `{"code":14018,"msg":"credits exhausted"}`, outcomeAccountExhausted},
		{"HTTP0 信封 14018", 0, codeCreditsExhausted, `credits exhausted`, outcomeAccountExhausted},
		{"HTTP0 信封 6004", 0, codeFrequencyLimit, `frequency limit`, outcomeRateLimited},
		{"文案含积分不足", 400, 0, `{"msg":"积分不足"}`, outcomeAccountExhausted},
		// 模型级权限/请求构造/上游暂不可用与账号额度无关：换号也不会成功，
		// 应立即把真实错误交给客户端。
		{"11102 模型无权限", 400, 11102, `model not allowed`, outcomeStop},
		{"11128 请求构造错误", 400, 11128, `first message is not system prompt`, outcomeStop},
		{"11134 上游暂不可用", 500, 11134, `provider temporarily unavailable`, outcomeStop},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyUpstreamFailure(tc.status, tc.code, tc.body); got != tc.want {
				t.Fatalf("classifyUpstreamFailure(%d, %d) = %s, want %s",
					tc.status, tc.code, failoverOutcomeName(got), failoverOutcomeName(tc.want))
			}
		})
	}
}

// classifyNonStream 必须把「HTTP 200 + 业务信封」识别成换号，而不是 outcomeStop。
func TestClassifyNonStreamRoutesEnvelopeToFailover(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want attemptOutcome
	}{
		{"6004 限流", `{"code":6004,"msg":"usage exceeds frequency limit"}`, outcomeRateLimited},
		{"14018 积分耗尽", `{"code":14018,"msg":"credits exhausted"}`, outcomeAccountExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, _ := businessErrorOf(tc.raw)
			if env == nil {
				t.Fatalf("businessErrorOf(%s) = nil", tc.raw)
			}
			outcome, _, _, _ := classifyNonStream(nil, businessErrorAsError(env))
			if outcome != tc.want {
				t.Fatalf("classifyNonStream = %s, want %s", failoverOutcomeName(outcome), failoverOutcomeName(tc.want))
			}
		})
	}
}

// 流式路径同样必须换号：runStreamFailover 收到 (emitted=false, status=200,
// businessError) 时不能直接 outcomeStop。
func TestRunStreamFailoverRoutesEnvelopeToNextAccount(t *testing.T) {
	makeCand := func(id string) *failoverCandidate {
		return &failoverCandidate{AuthID: id, RecordID: id, Auth: &storedAuth{}, RateModel: "deepseek-v4.1-flash"}
	}

	for _, tc := range []struct {
		name string
		raw  string
		want attemptOutcome
	}{
		{"6004 限流", `{"code":6004,"msg":"usage exceeds frequency limit"}`, outcomeRateLimited},
		{"14018 积分耗尽", `{"code":14018,"msg":"credits exhausted"}`, outcomeAccountExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 关掉同账号重试：本测试只关心「是否换号」，不需要等待退避。
			setSameAccountRetryForTest(t, 0, 0)
			defer setSameAccountRetryForTest(t, 3, 2*time.Second)
			env, _ := businessErrorOf(tc.raw)
			attemptErr := businessErrorAsError(env)
			// 每个子用例用独立账号 ID：上一用例的限流冷却写在全局表里，
			// 复用同一 ID 会被冷却挡住，导致 attempted=0。
			cands := []*failoverCandidate{makeCand(tc.name + "-a"), makeCand(tc.name + "-b")}
			attempted := 0
			res := runStreamFailover(
				withHost(context.Background(), &abi.Host{}),
				cands,
				"deepseek-v4.1-flash",
				func(cand *failoverCandidate) (bool, int, error) {
					attempted++
					// 两个账号都返回同样的额度/限流信封。
					return false, 200, attemptErr
				},
			)
			if attempted < 2 {
				t.Fatalf("attempted %d accounts, want >= 2 (must fail over instead of stopping on the first)", attempted)
			}
			if res.Outcome != tc.want {
				t.Fatalf("runStreamFailover outcome = %s, want %s", failoverOutcomeName(res.Outcome), failoverOutcomeName(tc.want))
			}
		})
	}
}

// setSameAccountRetryForTest 临时改写同账号重试策略，返回时由调用方恢复。
func setSameAccountRetryForTest(t *testing.T, count int, delay time.Duration) {
	t.Helper()
	retryPolicyMu.Lock()
	defer retryPolicyMu.Unlock()
	sameAccountRetryCount = count
	sameAccountRetryDelay = delay
}
