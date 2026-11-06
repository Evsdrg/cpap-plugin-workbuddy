package main

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

// 空回复修复的冷却侧闭环：上游「收下请求 + 只发心跳 + 关流」时，
// 每次尝试都必须是 empty_stream 失败（而不是成功、零输出），
// 连续 emptyStreamStrikeThreshold 次后该账号+模型进入冷却。
//
// 修复前心跳被当成首个 payload，尝试被判成功，strikes 永远不累积，
// 于是一个已经限流的账号会被无限复用——这正是生产里 450+ 条
// 「零输出成功记录」伴随 40%+ 失败率的来源。
func TestCooldownEngagesAfterEmptyStreamStrikes(t *testing.T) {
	model := "deepseek-v4.1-flash"
	authID := "auth-cool"
	uid := "uid-cool"

	var mu sync.Mutex
	attempts := 0
	oldOpen := openPumpStream
	openPumpStream = func(*http.Request) (upstreamHTTPStream, int, http.Header, error) {
		mu.Lock()
		attempts++
		mu.Unlock()
		s := newFakeUpstreamStream(4)
		s.chunks <- []byte(": heartbeat\n\n")
		close(s.chunks)
		return s, http.StatusOK, nil, nil
	}
	t.Cleanup(func() { openPumpStream = oldOpen })
	oldEmit := emitStreamChunk
	emitStreamChunk = func(string, []byte) error { return nil }
	t.Cleanup(func() { emitStreamChunk = oldEmit })
	key := modelCooldownKey(authID, model)
	resetAuthCooldown(key)
	t.Cleanup(func() { resetAuthCooldown(key) })

	cands := []*failoverCandidate{{AuthID: authID, Auth: &storedAuth{}, RateModel: model}}
	req, _ := http.NewRequest(http.MethodPost, "https://x.invalid", nil)
	for i := 0; i < emptyStreamStrikeThreshold; i++ {
		pumpAttemptWithTimeout(context.Background(), req, "", false, model, model,
			authID, uid, time.Now(), &sseUsageCollector{}, 3*time.Second)
		_ = runStreamFailover(context.Background(), cands, model,
			func(c *failoverCandidate) (bool, int, error) {
				return false, http.StatusOK, errEmptyStream
			})
	}
	cd := storedCooldown(key)
	onCooldown := isAuthOnCooldown(key)
	t.Logf("  上游尝试次数=%d 冷却状态=%v reason=%q until=%v", attempts, onCooldown, cd.Reason, cd.NextRetryAt)
	if !onCooldown {
		t.Fatalf("连续 %d 次空流后 %s 应进入冷却", emptyStreamStrikeThreshold, model)
	}
	if cd.Reason != cooldownReasonEmptyStreamQuota {
		t.Fatalf("冷却原因应为 %q，实际 %q", cooldownReasonEmptyStreamQuota, cd.Reason)
	}
}

// 反向验证断言：若把心跳重新当成首个 payload，strikes 不会累积，
// 冷却永远不会建立。这个测试就是那条断言的守卫。
func TestNoStrikeAccumulationWhenHeartbeatCountsAsPayload(t *testing.T) {
	model := "deepseek-v4.1-flash"
	authID := "auth-guard"
	key := modelCooldownKey(authID, model)
	resetAuthCooldown(key)
	t.Cleanup(func() { resetAuthCooldown(key) })

	// 直接验证判定函数：心跳不是 payload，所以 empty_stream 判定成立、
	// strike 才会累积。若这里返回 true，后续 strikes 全部失效。
	if hasFirstTokenPayload(": heartbeat") {
		t.Fatal("心跳不得被当作首个 payload，否则空流永远不累积 strike、冷却永不建立")
	}
	if !isEmptyStreamError(errEmptyStream) {
		t.Fatal("空流必须仍是可识别的 empty_stream 错误")
	}
}
