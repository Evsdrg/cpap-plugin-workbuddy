package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// withSameAccountRetry pins the same-account retry policy for one test and
// restores the previous value afterwards.
func withSameAccountRetry(t *testing.T, count int, delay time.Duration) {
	t.Helper()
	retryPolicyMu.RLock()
	prevCount, prevDelay := sameAccountRetryCount, sameAccountRetryDelay
	retryPolicyMu.RUnlock()
	setSameAccountRetry(count, delay)
	t.Cleanup(func() { setSameAccountRetry(prevCount, prevDelay) })
}

// disableSameAccountRetry restores the pre-0.9.3 immediate-failover behavior
// for tests that assert candidate ordering across accounts.
func disableSameAccountRetry(t *testing.T) {
	t.Helper()
	withSameAccountRetry(t, 0, 0)
}

func TestModelCooldownIsScopedToAccountAndModel(t *testing.T) {
	resetModelCooldown("idx-a", "deepseek-v4.1-flash")
	defer resetModelCooldown("idx-a", "deepseek-v4.1-flash")
	until := nextChinaMidnight(time.Now())
	markModelCooldown("idx-a", "deepseek-v4.1-flash", until, "first token timeout")
	if !isModelOnCooldown("idx-a", "deepseek-v4.1-flash") {
		t.Fatal("target auth/model should be cooled")
	}
	if isModelOnCooldown("idx-a", "gpt-6-astra") {
		t.Fatal("another model on same auth must remain available")
	}
	if isModelOnCooldown("idx-b", "deepseek-v4.1-flash") {
		t.Fatal("same model on another auth must remain available")
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	want := time.Date(time.Now().In(loc).Year(), time.Now().In(loc).Month(), time.Now().In(loc).Day()+1, 0, 0, 0, 0, loc)
	if !until.Equal(want) {
		t.Fatalf("nextChinaMidnight() = %v, want %v", until, want)
	}
}

func TestBuildFailoverPool_SkipsOnlyCooledModel(t *testing.T) {
	files := []pluginapi.HostAuthFileEntry{
		testAuthEntry("idx-1", "uid-1", false),
		testAuthEntry("idx-2", "uid-2", false),
	}
	creds := map[string]*storedAuth{
		"idx-1": {Auth: storedTokens{Domain: "https://www.workbuddy.ai"}, Account: storedAccount{UID: "uid-1"}},
		"idx-2": {Auth: storedTokens{Domain: "https://www.workbuddy.ai"}, Account: storedAccount{UID: "uid-2"}},
	}
	stubAuthSources(t, files, creds)
	markModelCooldown("idx-1", "deepseek-v4.1-flash", nextChinaMidnight(time.Now()), "exhausted")
	defer resetModelCooldown("idx-1", "deepseek-v4.1-flash")

	pool := buildFailoverPool("uid-1", "deepseek-v4.1-flash")
	if pool.Primary != nil && pool.Primary.AuthID == "idx-1" {
		t.Fatalf("cooled routed auth remains primary: %+v", pool.Primary)
	}
	if len(pool.Others) != 1 || pool.Others[0].AuthID != "idx-2" {
		t.Fatalf("same-model pool others = %+v, want [idx-2]", pool.Others)
	}
	otherModelPool := buildFailoverPool("uid-1", "gpt-6-astra")
	if otherModelPool.Primary == nil || otherModelPool.Primary.AuthID != "idx-1" {
		t.Fatalf("other-model primary = %+v, want idx-1", otherModelPool.Primary)
	}
}

func TestRunStreamFailover_FirstTokenTimeoutCoolsModelAndSwitches(t *testing.T) {
	withSameAccountRetry(t, 3, 0)
	resetModelCooldown("idx-1", "deepseek-v4.1-flash")
	defer resetModelCooldown("idx-1", "deepseek-v4.1-flash")
	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var attempts []string
	fin := runStreamFailover(context.Background(), cands, "deepseek-v4.1-flash", func(cand *failoverCandidate) (bool, int, error) {
		attempts = append(attempts, cand.AuthID)
		if cand.AuthID == "idx-1" {
			return false, 200, &firstTokenTimeoutError{model: "deepseek-v4.1-flash", resetAt: nextChinaMidnight(time.Now())}
		}
		return false, 0, nil
	})
	if fin.Outcome != outcomeSuccess || fin.AuthID != "idx-2" {
		t.Fatalf("failover result = %+v, want success on idx-2", fin)
	}
	if len(attempts) != 2 || attempts[0] != "idx-1" || attempts[1] != "idx-2" {
		t.Fatalf("attempts = %v, want [idx-1 idx-2]", attempts)
	}
	if !isModelOnCooldown("idx-1", "deepseek-v4.1-flash") {
		t.Fatal("timed-out account/model should be cooled")
	}
	if isModelOnCooldown("idx-1", "gpt-6-astra") {
		t.Fatal("cooldown must not affect another model")
	}
}

// An empty stream means the account's quota for this model is spent, so the loop
// moves on to the next account. What it must NOT do yet is quarantine: a single
// observation is indistinguishable from a network reset, and the penalty is a
// full day of lost traffic for that account+model pair.
//
// The previous version of this test asserted the opposite ("still does not
// switch"), on the premise that an empty stream was a request-level fault. That
// premise is wrong — see the errEmptyStream comment — and holding to it meant an
// exhausted account was never skipped, so the user got nothing until midnight.
func TestRunStreamFailover_EmptyStreamSwitchesButDoesNotQuarantineEarly(t *testing.T) {
	withSameAccountRetry(t, 0, 0)
	resetStrikes()
	resetModelCooldown("idx-1", "deepseek-v4.1-flash")
	defer resetModelCooldown("idx-1", "deepseek-v4.1-flash")
	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	fin := runStreamFailover(context.Background(), cands, "deepseek-v4.1-flash", func(cand *failoverCandidate) (bool, int, error) {
		order = append(order, cand.AuthID)
		return false, 200, errEmptyStream
	})
	// Every account is tried exactly once: the exhausted one is skipped, not
	// retried in place.
	if len(order) != 2 || order[0] != "idx-1" || order[1] != "idx-2" {
		t.Fatalf("attempt order = %v, want [idx-1 idx-2] (one pass, no same-account retry)", order)
	}
	if fin.Outcome != outcomeStop {
		t.Fatalf("outcome = %v, want outcomeStop", fin.Outcome)
	}
	if !isEmptyStreamError(fin.Err) {
		t.Fatalf("fin.Err = %v, want errEmptyStream surfaced", fin.Err)
	}
	if isModelOnCooldown("idx-1", "deepseek-v4.1-flash") {
		t.Fatal("a single empty stream must not set model cooldown; the verdict needs consecutive strikes")
	}
	if isModelOnCooldown("idx-2", "deepseek-v4.1-flash") {
		t.Fatal("idx-2 was quarantined though it was never implicated")
	}
}

func TestRunStreamFailover_EmittedTimeoutDoesNotSwitch(t *testing.T) {
	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	attempts := 0
	fin := runStreamFailover(context.Background(), cands, "deepseek-v4.1-flash", func(*failoverCandidate) (bool, int, error) {
		attempts++
		return true, 200, &firstTokenTimeoutError{model: "deepseek-v4.1-flash", resetAt: nextChinaMidnight(time.Now())}
	})
	if attempts != 1 || !fin.Emitted || fin.AuthID != "idx-1" {
		t.Fatalf("attempts=%d result=%+v, want committed stop", attempts, fin)
	}
}

func TestParseRateLimitResetAt_WithUTC8(t *testing.T) {
	body := `usage exceeds frequency limit, please try again later, reset at 2026-09-06 06:01:58 UTC+8`
	got := parseRateLimitResetAt(body)
	want := time.Date(2026, 9, 6, 6, 1, 58, 0, time.FixedZone("UTC+8", 8*60*60))
	if !got.Equal(want) {
		t.Fatalf("parseRateLimitResetAt = %v, want %v", got, want)
	}
}

func TestParseRateLimitResetAt_OtherOffset(t *testing.T) {
	got := parseRateLimitResetAt("reset at 2026-09-06 02:30:00 UTC+2")
	want := time.Date(2026, 9, 6, 2, 30, 0, 0, time.FixedZone("UTC+2", 2*60*60))
	if !got.Equal(want) {
		t.Fatalf("parseRateLimitResetAt = %v, want %v", got, want)
	}
}

func TestParseRateLimitResetAt_NoMatchReturnsZero(t *testing.T) {
	if !parseRateLimitResetAt("some other error").IsZero() {
		t.Fatal("expected zero time for body without reset marker")
	}
	if !parseRateLimitResetAt("").IsZero() {
		t.Fatal("expected zero time for empty body")
	}
}

func TestIsRateLimitResponse(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   bool
	}{
		{429, "", true},
		{429, `{"code":6004,"msg":"usage exceeds frequency limit"}`, true},
		{200, `{"code":6004,"msg":"usage exceeds frequency limit"}`, true},
		{200, "rate limit exceeded", true},
		{403, "too many requests", true},
		{402, "insufficient credit", false},
		{200, "insufficient credits", false},
		{500, "internal server error", false},
		{200, "ok", false},
	}
	for _, c := range cases {
		if got := isRateLimitResponse(c.status, c.body); got != c.want {
			t.Errorf("isRateLimitResponse(%d, %q) = %v, want %v", c.status, c.body, got, c.want)
		}
	}
}

func TestCooldownLifecycle(t *testing.T) {
	resetAuthCooldown("auth-a")
	if isAuthOnCooldown("auth-a") {
		t.Fatal("fresh account must not be on cooldown")
	}
	markAuthCooldown("auth-a", time.Now().Add(10*time.Minute))
	if !isAuthOnCooldown("auth-a") {
		t.Fatal("account must be on cooldown after mark")
	}
	resetAuthCooldown("auth-a")
	if isAuthOnCooldown("auth-a") {
		t.Fatal("reset must clear cooldown")
	}
	// Expired cooldown clears lazily on read. (markAuthCooldown refuses past
	// reset times — they fall back to the default 30m window — so simulate the
	// passage of time by writing an expired entry directly.)
	cooldownMu.Lock()
	cooldowns["auth-b"] = authCooldown{NextRetryAt: time.Now().Add(-time.Second), Reason: "429"}
	cooldownMu.Unlock()
	if isAuthOnCooldown("auth-b") {
		t.Fatal("expired cooldown must clear lazily")
	}
	// Empty id is a no-op.
	markAuthCooldown("", time.Now().Add(time.Minute))
	if isAuthOnCooldown("") {
		t.Fatal("empty auth id must never be on cooldown")
	}
}

// TestMarkCooldown_PastResetFallsBackToDefaultWindow locks that a reset time
// already in the past (clock skew) still yields the 30-minute cooldown.
func TestMarkCooldown_PastResetFallsBackToDefaultWindow(t *testing.T) {
	resetAuthCooldown("auth-c")
	defer resetAuthCooldown("auth-c")
	markAuthCooldown("auth-c", time.Now().Add(-5*time.Minute))
	if !isAuthOnCooldown("auth-c") {
		t.Fatal("past reset time must fall back to the default cooldown window")
	}
}

// storedCooldown reads one cooldown entry under the lock (test helper).
func storedCooldown(key string) authCooldown {
	cooldownMu.Lock()
	defer cooldownMu.Unlock()
	return cooldowns[key]
}

// TestMarkCooldown_WeakerVerdictNeverShortensStrongerOne locks the severity
// ordering documented on cooldownReasonRank: a soft 429 that arrives AFTER a
// first-token-timeout verdict (until the next China midnight) must not
// overwrite it or shorten the window. Regression: the rank helper existed but
// was never wired into markAuthCooldownReason, so only credits_exhausted was
// protected and a later 429 collapsed the rest-of-day cooldown to ≤10 minutes.
func TestMarkCooldown_WeakerVerdictNeverShortensStrongerOne(t *testing.T) {
	resetModelCooldown("idx-rank", "deepseek-v4.1-flash")
	defer resetModelCooldown("idx-rank", "deepseek-v4.1-flash")
	key := modelCooldownKey("idx-rank", "deepseek-v4.1-flash")

	until := nextChinaMidnight(time.Now())
	markModelCooldown("idx-rank", "deepseek-v4.1-flash", until, cooldownReasonFirstTokenTimeout)

	// A soft rate limit observed later, with an imminent reset.
	markModelCooldown("idx-rank", "deepseek-v4.1-flash", time.Now().Add(2*time.Minute), "429")

	got := storedCooldown(key)
	if got.Reason != cooldownReasonFirstTokenTimeout {
		t.Fatalf("reason = %q, want %q", got.Reason, cooldownReasonFirstTokenTimeout)
	}
	if !got.NextRetryAt.Equal(until) {
		t.Fatalf("until = %s, want %s (stronger verdict was shortened)", got.NextRetryAt, until)
	}
}

// TestMarkCooldown_StrongerVerdictStillReplacesWeakerOne is the converse: a
// first-token verdict must still be able to replace an active soft 429, or the
// watchdog could never escalate.
func TestMarkCooldown_StrongerVerdictStillReplacesWeakerOne(t *testing.T) {
	resetModelCooldown("idx-rank2", "deepseek-v4.1-flash")
	defer resetModelCooldown("idx-rank2", "deepseek-v4.1-flash")
	key := modelCooldownKey("idx-rank2", "deepseek-v4.1-flash")

	markModelCooldown("idx-rank2", "deepseek-v4.1-flash", time.Now().Add(time.Minute), "429")
	until := nextChinaMidnight(time.Now())
	markModelCooldown("idx-rank2", "deepseek-v4.1-flash", until, cooldownReasonFirstTokenTimeout)

	got := storedCooldown(key)
	if got.Reason != cooldownReasonFirstTokenTimeout || !got.NextRetryAt.Equal(until) {
		t.Fatalf("stored = %+v, want reason=%q until=%s", got, cooldownReasonFirstTokenTimeout, until)
	}
}

// stubAuthSources replaces the host auth list/get providers for one test.
func stubAuthSources(t *testing.T, files []pluginapi.HostAuthFileEntry, creds map[string]*storedAuth) {
	t.Helper()
	prevList, prevGet := authListSource, authGetSource
	authListSource = func() ([]pluginapi.HostAuthFileEntry, error) { return files, nil }
	authGetSource = func(idx string) (*storedAuth, error) {
		if sa, ok := creds[idx]; ok {
			return sa, nil
		}
		return nil, errors.New("no cred")
	}
	invalidateAuthListCache()
	t.Cleanup(func() {
		authListSource, authGetSource = prevList, prevGet
		invalidateAuthListCache()
	})
}

func testAuthEntry(idx, uid string, disabled bool) pluginapi.HostAuthFileEntry {
	return pluginapi.HostAuthFileEntry{
		AuthIndex: idx,
		ID:        uid,
		Name:      "workbuddy-" + uid + ".json",
		Disabled:  disabled,
		Status:    "ok",
	}
}

func TestBuildFailoverPool_RoutedAndOthers(t *testing.T) {
	files := []pluginapi.HostAuthFileEntry{
		testAuthEntry("idx-1", "uid-1", false),
		testAuthEntry("idx-2", "uid-2", false),
		testAuthEntry("idx-3", "uid-3", true), // disabled — skipped
	}
	creds := map[string]*storedAuth{
		"idx-1": {Auth: storedTokens{Domain: "https://www.workbuddy.ai"}, Account: storedAccount{UID: "uid-1"}},
		"idx-2": {Auth: storedTokens{Domain: "https://www.workbuddy.ai"}, Account: storedAccount{UID: "uid-2"}},
	}
	stubAuthSources(t, files, creds)

	// Routing to idx-1: primary = idx-1, one fallback = idx-2 (idx-3 disabled).
	pool := buildFailoverPool("uid-1", "deepseek-v4.1-flash")
	if pool.Primary == nil || pool.Primary.AuthID != "idx-1" {
		t.Fatalf("primary = %+v, want idx-1", pool.Primary)
	}
	if len(pool.Others) != 1 || pool.Others[0].AuthID != "idx-2" {
		t.Fatalf("others = %+v, want [idx-2]", pool.Others)
	}
}

func TestBuildFailoverPool_SkipsCooldown(t *testing.T) {
	files := []pluginapi.HostAuthFileEntry{
		testAuthEntry("idx-1", "uid-1", false),
		testAuthEntry("idx-2", "uid-2", false),
	}
	creds := map[string]*storedAuth{
		"idx-1": {Auth: storedTokens{Domain: "https://www.workbuddy.ai"}, Account: storedAccount{UID: "uid-1"}},
		"idx-2": {Auth: storedTokens{Domain: "https://www.workbuddy.ai"}, Account: storedAccount{UID: "uid-2"}},
	}
	stubAuthSources(t, files, creds)

	markAuthCooldown("idx-2", time.Now().Add(5*time.Minute))
	defer resetAuthCooldown("idx-2")

	pool := buildFailoverPool("uid-1", "deepseek-v4.1-flash")
	if len(pool.Others) != 0 {
		t.Fatalf("cooldown fallback must be excluded, others = %+v", pool.Others)
	}
}

// TestRunFailover_RetriesOn429 verifies the retry order and cooldown marking
// when the routed account answers 429 and the fallback succeeds.
func TestRunFailover_RetriesOn429(t *testing.T) {
	disableSameAccountRetry(t)
	resetAuthCooldown("idx-1")
	resetAuthCooldown("idx-2")
	defer func() { resetAuthCooldown("idx-1"); resetAuthCooldown("idx-2") }()

	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	fin := runFailover(context.Background(), cands, func(cand *failoverCandidate) (attemptOutcome, int, string, error) {
		order = append(order, cand.AuthID)
		if cand.AuthID == "idx-1" {
			return outcomeRateLimited, 429, `usage exceeds frequency limit, reset at 2099-01-01 00:00:00 UTC+8`, errors.New("upstream 429")
		}
		return outcomeSuccess, 0, "", nil
	})
	if fin.Outcome != outcomeSuccess || fin.AuthID != "idx-2" {
		t.Fatalf("fin = %+v, want success on idx-2", fin)
	}
	if len(order) != 2 || order[0] != "idx-1" || order[1] != "idx-2" {
		t.Fatalf("attempt order = %v, want [idx-1 idx-2]", order)
	}
	if !isAuthOnCooldown("idx-1") {
		t.Fatal("rate-limited primary must be marked on cooldown")
	}
}

// TestRunFailover_All429 verifies the loop exhausts candidates, marks each,
// and reports the last failure.
func TestRunFailover_All429(t *testing.T) {
	disableSameAccountRetry(t)
	resetAuthCooldown("idx-1")
	resetAuthCooldown("idx-2")
	defer func() { resetAuthCooldown("idx-1"); resetAuthCooldown("idx-2") }()

	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	fin := runFailover(context.Background(), cands, func(cand *failoverCandidate) (attemptOutcome, int, string, error) {
		order = append(order, cand.AuthID)
		return outcomeRateLimited, 429, "rate limited", errors.New("upstream 429")
	})
	if fin.Outcome != outcomeRateLimited {
		t.Fatalf("fin.Outcome = %v, want outcomeRateLimited", fin.Outcome)
	}
	if len(order) != 2 {
		t.Fatalf("attempt order = %v, want both tried", order)
	}
	if !isAuthOnCooldown("idx-1") || !isAuthOnCooldown("idx-2") {
		t.Fatal("both accounts must be on cooldown")
	}
}

// TestRunFailover_StopsOnBusinessError verifies non-rate-limit failures are
// never retried on another account.
func TestRunFailover_StopsOnBusinessError(t *testing.T) {
	disableSameAccountRetry(t)
	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	fin := runFailover(context.Background(), cands, func(cand *failoverCandidate) (attemptOutcome, int, string, error) {
		order = append(order, cand.AuthID)
		return outcomeStop, 402, "insufficient credit", errors.New("upstream 402")
	})
	if fin.Outcome != outcomeStop || len(order) != 1 || order[0] != "idx-1" {
		t.Fatalf("fin = %+v order = %v, want immediate stop on idx-1", fin, order)
	}
}

// TestRunFailover_EmptyStreamNoRetry locks the revision requirement: an
// empty_stream (upstream closed before first payload) is NOT an account
// problem — retrying it would double-bill credits. The loop must stop after
// the first attempt and must NOT mark the account on cooldown.
func TestRunFailover_EmptyStreamNoRetry(t *testing.T) {
	disableSameAccountRetry(t)
	resetAuthCooldown("idx-1")
	resetAuthCooldown("idx-2")
	defer func() { resetAuthCooldown("idx-1"); resetAuthCooldown("idx-2") }()

	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	fin := runFailover(context.Background(), cands, func(cand *failoverCandidate) (attemptOutcome, int, string, error) {
		order = append(order, cand.AuthID)
		return outcomeStop, 0, "", errEmptyStream
	})
	if fin.Outcome != outcomeStop {
		t.Fatalf("fin.Outcome = %v, want outcomeStop", fin.Outcome)
	}
	if len(order) != 1 || order[0] != "idx-1" {
		t.Fatalf("attempt order = %v, want single attempt on idx-1 (no retry on empty_stream)", order)
	}
	if !isEmptyStreamError(fin.Err) {
		t.Fatalf("fin.Err = %v, want errEmptyStream surfaced", fin.Err)
	}
	if isAuthOnCooldown("idx-1") || isAuthOnCooldown("idx-2") {
		t.Fatal("empty_stream must NOT mark accounts on cooldown")
	}
}

// TestClassifyNonStream_EmptyStreamIsStop verifies the classifier used by the
// non-stream executor path treats errEmptyStream as a stop (never rate-limit).
func TestClassifyNonStream_EmptyStreamIsStop(t *testing.T) {
	outcome, status, body, err := classifyNonStream(nil, errEmptyStream)
	if outcome != outcomeStop {
		t.Fatalf("outcome = %v, want outcomeStop", outcome)
	}
	if status != 0 || body != "" {
		t.Fatalf("status/body = %d/%q, want 0/empty", status, body)
	}
	if !isEmptyStreamError(err) {
		t.Fatalf("err = %v, want errEmptyStream", err)
	}
}

// TestRunStreamFailover_NoRetryAfterChunkEmitted locks the invariant that a
// stream which already emitted chunks is never replayed on another account.
func TestRunStreamFailover_NoRetryAfterChunkEmitted(t *testing.T) {
	disableSameAccountRetry(t)
	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	fin := runStreamFailover(context.Background(), cands, "deepseek-v4.1-flash", func(cand *failoverCandidate) (bool, int, error) {
		order = append(order, cand.AuthID)
		return true, 200, errors.New("mid-stream read error after chunks")
	})
	if fin.Outcome != outcomeStop || !fin.Emitted {
		t.Fatalf("fin = %+v, want stop with emitted=true", fin)
	}
	if len(order) != 1 {
		t.Fatalf("attempt order = %v, want single attempt", order)
	}
}

// TestRunStreamFailover_RetriesPreChunk429 verifies a 429 observed before the
// first chunk triggers a fallback attempt.
func TestRunStreamFailover_RetriesPreChunk429(t *testing.T) {
	disableSameAccountRetry(t)
	resetAuthCooldown("idx-1")
	resetAuthCooldown("idx-2")
	defer func() { resetAuthCooldown("idx-1"); resetAuthCooldown("idx-2") }()

	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	fin := runStreamFailover(context.Background(), cands, "deepseek-v4.1-flash", func(cand *failoverCandidate) (bool, int, error) {
		order = append(order, cand.AuthID)
		if cand.AuthID == "idx-1" {
			return false, 429, errors.New(`upstream 429: usage exceeds frequency limit`)
		}
		return false, 0, nil
	})
	if fin.Outcome != outcomeSuccess || fin.AuthID != "idx-2" {
		t.Fatalf("fin = %+v, want success on idx-2", fin)
	}
	if len(order) != 2 || order[0] != "idx-1" || order[1] != "idx-2" {
		t.Fatalf("attempt order = %v, want [idx-1 idx-2]", order)
	}
}

// TestRunStreamFailover_EmptyStreamNoRetry locks the revision requirement for
// the async path: empty_stream before any chunk is NOT retried on another
// account (double-billing) and does NOT mark cooldown.
// "No retry" here means no SAME-ACCOUNT retry. Switching to another account is
// the point: the first account is out of quota for this model and another one may
// still have some. The invariant that matters is that one account is never
// re-attempted in a loop, because that both wastes time and would look like the
// account is being punished for a quota wall.
func TestRunStreamFailover_EmptyStreamNoSameAccountRetry(t *testing.T) {
	disableSameAccountRetry(t)
	resetStrikes()
	resetAuthCooldown("idx-1")
	resetAuthCooldown("idx-2")
	defer func() { resetAuthCooldown("idx-1"); resetAuthCooldown("idx-2") }()

	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	fin := runStreamFailover(context.Background(), cands, "deepseek-v4.1-flash", func(cand *failoverCandidate) (bool, int, error) {
		order = append(order, cand.AuthID)
		return false, 0, errEmptyStream
	})
	if fin.Outcome != outcomeStop {
		t.Fatalf("fin.Outcome = %v, want outcomeStop", fin.Outcome)
	}
	// One pass over the candidates, each exactly once.
	if len(order) != 2 || order[0] != "idx-1" || order[1] != "idx-2" {
		t.Fatalf("attempt order = %v, want [idx-1 idx-2] (one attempt per account, no same-account retry)", order)
	}
	if !isEmptyStreamError(fin.Err) {
		t.Fatalf("fin.Err = %v, want errEmptyStream surfaced", fin.Err)
	}
	if fin.Emitted {
		t.Fatal("empty_stream attempt must not be marked emitted")
	}
	if isAuthOnCooldown("idx-1") || isAuthOnCooldown("idx-2") {
		t.Fatal("empty_stream must NOT mark accounts on cooldown (below the strike threshold)")
	}
}

// TestIsEmptyStreamError covers the sentinel check.
func TestIsEmptyStreamError(t *testing.T) {
	if !isEmptyStreamError(errEmptyStream) {
		t.Fatal("errEmptyStream must be recognized")
	}
	if isEmptyStreamError(errors.New("other")) || isEmptyStreamError(nil) {
		t.Fatal("non-empty-stream errors must not be recognized")
	}
}

func TestUpstreamStatusError_Formatting(t *testing.T) {
	e := &upstreamStatusError{status: 429, body: fmt.Sprintf("usage exceeds frequency limit reset at %s", time.Now().Format("2006-01-02 15:04:05")) + " UTC+8"}
	msg := e.Error()
	if len(msg) == 0 || !strings.Contains(msg, "upstream 429") {
		t.Fatalf("unexpected error format: %q", msg)
	}
}

// TestRunFailover_SameAccountRetriesBeforeSwitching covers the 0.9.3 policy:
// a rate-limited account is retried on the SAME account (retry_per_account extra
// attempts) before the loop moves to the next candidate.
func TestRunFailover_SameAccountRetriesBeforeSwitching(t *testing.T) {
	resetAuthCooldown("idx-1")
	resetAuthCooldown("idx-2")
	defer func() { resetAuthCooldown("idx-1"); resetAuthCooldown("idx-2") }()
	withSameAccountRetry(t, 3, 0) // 3 extra attempts, no sleep

	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	fin := runFailover(context.Background(), cands, func(cand *failoverCandidate) (attemptOutcome, int, string, error) {
		order = append(order, cand.AuthID)
		if cand.AuthID == "idx-1" {
			return outcomeRateLimited, 429, "rate limited", errors.New("upstream 429")
		}
		return outcomeSuccess, 0, "", nil
	})
	if fin.Outcome != outcomeSuccess || fin.AuthID != "idx-2" {
		t.Fatalf("fin = %+v, want success on idx-2", fin)
	}
	// idx-1 tried 1 + 3 = 4 times, then idx-2 once.
	want := []string{"idx-1", "idx-1", "idx-1", "idx-1", "idx-2"}
	if len(order) != len(want) {
		t.Fatalf("attempt order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("attempt order = %v, want %v", order, want)
		}
	}
	if !isAuthOnCooldown("idx-1") {
		t.Fatal("exhausted primary must be on cooldown")
	}
}

// TestRunFailover_SameAccountSuccessShortCircuits locks that a retry which
// succeeds on the same account never touches a fallback.
func TestRunFailover_SameAccountSuccessShortCircuits(t *testing.T) {
	resetAuthCooldown("idx-1")
	resetAuthCooldown("idx-2")
	defer func() { resetAuthCooldown("idx-1"); resetAuthCooldown("idx-2") }()
	withSameAccountRetry(t, 3, 0)

	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	fin := runFailover(context.Background(), cands, func(cand *failoverCandidate) (attemptOutcome, int, string, error) {
		order = append(order, cand.AuthID)
		// Fail once, then succeed — on the same account.
		if len(order) == 1 {
			return outcomeRateLimited, 429, "rate limited", errors.New("upstream 429")
		}
		return outcomeSuccess, 0, "", nil
	})
	if fin.Outcome != outcomeSuccess || fin.AuthID != "idx-1" {
		t.Fatalf("fin = %+v, want success on idx-1", fin)
	}
	if len(order) != 2 || order[0] != "idx-1" || order[1] != "idx-1" {
		t.Fatalf("attempt order = %v, want two attempts on idx-1 only", order)
	}
	if isAuthOnCooldown("idx-1") {
		t.Fatal("account that recovered on retry must not be on cooldown")
	}
}

// TestRunFailover_RetryDisabledSwitchesImmediately locks the retry_per_account=0
// escape hatch back to pre-0.9.3 behavior.
func TestRunFailover_RetryDisabledSwitchesImmediately(t *testing.T) {
	resetAuthCooldown("idx-1")
	resetAuthCooldown("idx-2")
	defer func() { resetAuthCooldown("idx-1"); resetAuthCooldown("idx-2") }()
	withSameAccountRetry(t, 0, 0)

	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	_ = runFailover(context.Background(), cands, func(cand *failoverCandidate) (attemptOutcome, int, string, error) {
		order = append(order, cand.AuthID)
		return outcomeRateLimited, 429, "rate limited", errors.New("upstream 429")
	})
	if len(order) != 2 || order[0] != "idx-1" || order[1] != "idx-2" {
		t.Fatalf("attempt order = %v, want one attempt per account", order)
	}
}

// TestRunStreamFailover_SameAccountRetryThenSuccess covers the streaming path.
func TestRunStreamFailover_SameAccountRetryThenSuccess(t *testing.T) {
	resetAuthCooldown("idx-1")
	resetAuthCooldown("idx-2")
	defer func() { resetAuthCooldown("idx-1"); resetAuthCooldown("idx-2") }()
	withSameAccountRetry(t, 2, 0)

	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}
	var order []string
	fin := runStreamFailover(context.Background(), cands, "deepseek-v4.1-flash", func(cand *failoverCandidate) (bool, int, error) {
		order = append(order, cand.AuthID)
		if len(order) <= 2 {
			return false, 429, errors.New("upstream 429: frequency limit")
		}
		return false, 0, nil
	})
	if fin.Outcome != outcomeSuccess || fin.AuthID != "idx-1" {
		t.Fatalf("fin = %+v, want success on idx-1 after retry", fin)
	}
	if len(order) != 3 {
		t.Fatalf("attempt order = %v, want 3 attempts on idx-1", order)
	}
	for _, id := range order {
		if id != "idx-1" {
			t.Fatalf("attempt order = %v, want only idx-1", order)
		}
	}
}

// TestSetSameAccountRetry_ClampsNegative locks the config clamp.
func TestSetSameAccountRetry_ClampsNegative(t *testing.T) {
	withSameAccountRetry(t, -5, -time.Second)
	if got := sameAccountRetryLimit(); got != 0 {
		t.Fatalf("sameAccountRetryLimit() = %d, want 0 after negative clamp", got)
	}
	if got := sameAccountRetryDelayValue(); got != 0 {
		t.Fatalf("sameAccountRetryDelayValue() = %v, want 0 after negative clamp", got)
	}
}

// TestRunFailover_ContextCancelledStopsRetries locks that a cancelled context
// aborts the wait between same-account attempts instead of retrying further.
// The cancel happens inside the first attempt: admission itself already refuses
// to start upstream I/O on a done context, so cancelling before the call would
// exercise that guard instead of the inter-attempt sleep.
func TestRunFailover_ContextCancelledStopsRetries(t *testing.T) {
	resetAuthCooldown("idx-1")
	defer resetAuthCooldown("idx-1")
	withSameAccountRetry(t, 3, time.Hour) // long delay so cancellation is observable

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cands := []*failoverCandidate{{AuthID: "idx-1", Auth: &storedAuth{}}}
	var order []string
	fin := runFailover(ctx, cands, func(cand *failoverCandidate) (attemptOutcome, int, string, error) {
		order = append(order, cand.AuthID)
		cancel() // client goes away while this attempt is in flight
		return outcomeRateLimited, 429, "rate limited", errors.New("upstream 429")
	})
	if len(order) != 1 {
		t.Fatalf("attempt order = %v, want single attempt before cancellation", order)
	}
	if fin.Outcome != outcomeRateLimited {
		t.Fatalf("fin.Outcome = %v, want outcomeRateLimited", fin.Outcome)
	}
}
