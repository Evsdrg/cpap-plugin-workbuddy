package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// admissionCandidates returns two enabled candidates with distinct UIDs and
// clears their cooldown state before and after the test.
func admissionCandidates(t *testing.T) []*failoverCandidate {
	t.Helper()
	cands := []*failoverCandidate{
		{AuthID: "adm-a", RecordID: "record-a", Aliases: []string{"workbuddy-uid-a.json"},
			Auth: &storedAuth{Auth: storedTokens{Domain: "https://www.codebuddy.ai"}, Account: storedAccount{UID: "uid-a"}}},
		{AuthID: "adm-b", RecordID: "record-b", Aliases: []string{"workbuddy-uid-b.json"},
			Auth: &storedAuth{Auth: storedTokens{Domain: "https://www.codebuddy.ai"}, Account: storedAccount{UID: "uid-b"}}},
	}
	for _, c := range cands {
		resetAllCooldowns(c)
	}
	t.Cleanup(func() {
		for _, c := range cands {
			resetAllCooldowns(c)
		}
	})
	return cands
}

// resetAllCooldowns clears every cooldown entry a candidate carries: both the
// account-wide keys (credits_exhausted) and the per-model keys that
// resetCandidateCooldown deliberately leaves alone.
func resetAllCooldowns(c *failoverCandidate) {
	for _, id := range candidateCooldownIDs(c) {
		resetAuthCooldown(id)
		if c.RateModel != "" {
			resetModelCooldown(id, c.RateModel)
		}
	}
}

// resetModelCooldownFor clears one account+model quarantine for every cooldown
// identity of a candidate (a cooldown is written to each identity).
func resetModelCooldownFor(c *failoverCandidate, model string) {
	for _, id := range candidateCooldownIDs(c) {
		resetModelCooldown(id, model)
	}
}

// readyProbe pushes a cooling candidate's probe instant into the past so the
// next admission may try it. Production advances ProbeAt only by waiting.
func readyProbe(t *testing.T, c *failoverCandidate) {
	t.Helper()
	cooldownMu.Lock()
	for _, id := range candidateCooldownIDs(c) {
		for _, key := range []string{id, modelCooldownKey(id, c.RateModel)} {
			if cd, ok := cooldowns[key]; ok {
				cd.ProbeAt = time.Now().Add(-time.Second)
				cooldowns[key] = cd
			}
		}
	}
	cooldownMu.Unlock()
}

// TestAdmitCandidateAttemptSkipsBusyAccountInsteadOfQueueing locks the
// admission contract: a second concurrent chat for the SAME upstream account is
// refused with errAccountBusy (the caller tries another account) rather than
// queued behind the first. One in-flight chat per account is what keeps our own
// parallelism from tripping CodeBuddy's per-account 6004 frequency limit.
func TestAdmitCandidateAttemptSkipsBusyAccountInsteadOfQueueing(t *testing.T) {
	cands := admissionCandidates(t)
	c := cands[0]

	release, err := admitCandidateAttempt(context.Background(), c)
	if err != nil || release == nil {
		t.Fatalf("first admission: release=%v err=%v", release != nil, err)
	}

	if r, e := admitCandidateAttempt(context.Background(), c); r != nil || !errors.Is(e, errAccountBusy) {
		t.Fatalf("second admission: acquired=%v err=%v, want errAccountBusy", r != nil, e)
	}
	if candidateOnCooldown(c) {
		t.Fatal("a busy refusal must not quarantine the account")
	}

	// Another account of the same realm is unaffected and can answer now.
	other, err := admitCandidateAttempt(context.Background(), cands[1])
	if err != nil || other == nil {
		t.Fatalf("other account admission: release=%v err=%v", other != nil, err)
	}
	other()

	release()
	release() // idempotent: a double release must not corrupt the count or deadlock

	final, err := admitCandidateAttempt(context.Background(), c)
	if err != nil || final == nil {
		t.Fatalf("account stayed busy after release: release=%v err=%v", final != nil, err)
	}
	final()
	chatGateMu.Lock()
	_, retained := chatGates[accountGateKey(c)]
	chatGateMu.Unlock()
	if retained {
		t.Fatal("idle gate leaked after the in-flight count returned to zero")
	}
}

// TestAccountGateKeyUsesUIDNotAuthIndex locks that two auth records resolving to
// the same upstream account (renamed file, re-imported credential) share ONE
// concurrency gate — otherwise the cap could be doubled against one quota.
func TestAccountGateKeyUsesUIDNotAuthIndex(t *testing.T) {
	same := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{Auth: storedTokens{Domain: "https://www.codebuddy.ai"}, Account: storedAccount{UID: "uid-x"}}},
		{AuthID: "idx-2", Auth: &storedAuth{Auth: storedTokens{Domain: "https://www.codebuddy.ai"}, Account: storedAccount{UID: "uid-x"}}},
	}
	if accountGateKey(same[0]) != accountGateKey(same[1]) {
		t.Fatalf("same UID produced different gate keys: %q vs %q", accountGateKey(same[0]), accountGateKey(same[1]))
	}
	first, err := admitCandidateAttempt(context.Background(), same[0])
	if err != nil || first == nil {
		t.Fatalf("first admission failed: %v", err)
	}
	defer first()
	if r, e := admitCandidateAttempt(context.Background(), same[1]); r != nil || !errors.Is(e, errAccountBusy) {
		t.Fatalf("second auth record for the same UID: acquired=%v err=%v, want errAccountBusy", r != nil, e)
	}
}

// TestModelSoftLimitKeepsAccountOtherModelsRoutable locks the upstream v0.9.7
// 6004 semantics on the real quarantine path (markCandidateUnavailable, not the
// test-only markModelCooldown helper): a soft rate limit removes only the
// account+model pair, while the same account still serves every other model.
func TestModelSoftLimitKeepsAccountOtherModelsRoutable(t *testing.T) {
	const cooledModel = "deepseek-v4.1-flash"
	const otherModel = "gpt-6-astra"

	cands := admissionCandidates(t)
	stubAuthSources(t,
		[]pluginapi.HostAuthFileEntry{testAuthEntry("adm-a", "uid-a", false), testAuthEntry("adm-b", "uid-b", false)},
		map[string]*storedAuth{"adm-a": cands[0].Auth, "adm-b": cands[1].Auth})

	routed := &failoverCandidate{AuthID: "adm-a", RecordID: "record-a", Auth: cands[0].Auth, RateModel: cooledModel}
	markCandidateUnavailable(routed, outcomeRateLimited, `{"code":6004,"msg":"usage exceeds frequency limit"}`)

	// The cooling model loses its routed account and falls to the peer.
	cooledPool := buildFailoverPool("adm-a", cooledModel)
	invalidateAuthListCache()
	cooled := cooledPool.ordered(nil)
	for _, c := range cooled {
		if c.AuthID == "adm-a" {
			t.Fatalf("cooled account+model still eligible: %+v", cooled)
		}
	}
	if len(cooled) != 1 || cooled[0].AuthID != "adm-b" {
		t.Fatalf("cooled-model pool = %+v, want only adm-b", cooled)
	}

	// Another model on the SAME account stays routed — the whole point of the
	// upstream 6004 message ("switch to the other models to continue").
	otherPool := buildFailoverPool("adm-a", otherModel)
	invalidateAuthListCache()
	if otherPool.Primary == nil || otherPool.Primary.AuthID != "adm-a" {
		t.Fatalf("other-model primary = %+v, want adm-a", otherPool.Primary)
	}
}

// TestCreditExhaustionStaysAccountWide is the converse: an explicit credit
// rejection is an account problem, so it quarantines the account for EVERY
// model (not just the model that happened to be requested).
func TestCreditExhaustionStaysAccountWide(t *testing.T) {
	cands := admissionCandidates(t)
	stubAuthSources(t,
		[]pluginapi.HostAuthFileEntry{testAuthEntry("adm-a", "uid-a", false), testAuthEntry("adm-b", "uid-b", false)},
		map[string]*storedAuth{"adm-a": cands[0].Auth, "adm-b": cands[1].Auth})

	routed := &failoverCandidate{AuthID: "adm-a", RecordID: "record-a", Auth: cands[0].Auth, RateModel: "deepseek-v4.1-flash"}
	markCandidateUnavailable(routed, outcomeAccountExhausted, `{"code":14018,"msg":"Credits exhausted"}`)

	for _, model := range []string{"deepseek-v4.1-flash", "gpt-6-astra"} {
		pool := buildFailoverPool("adm-a", model)
		invalidateAuthListCache()
		for _, c := range pool.ordered(nil) {
			if c.AuthID == "adm-a" {
				t.Fatalf("credit-exhausted account still eligible for %s: %+v", model, pool.ordered(nil))
			}
		}
	}

	// Credit exhaustion is never probe-eligible, even when every peer is
	// cooling as well.
	markCandidateUnavailable(&failoverCandidate{AuthID: "adm-b", RecordID: "record-b", Auth: cands[1].Auth, RateModel: "gpt-6-astra"}, outcomeRateLimited, "")
	pool := buildFailoverPool("adm-b", "gpt-6-astra")
	if pool.Primary != nil && pool.Primary.AuthID == "adm-a" {
		t.Fatal("credit-exhausted account was resurrected as the recovery probe")
	}
}

// TestPoolKeepsOneRecoveryProbeWhenEveryAccountSoftCools locks the probe
// reservation: once every eligible account of the realm is soft-cooling, the
// pool keeps exactly ONE probe instead of failing the request outright.
func TestPoolKeepsOneRecoveryProbeWhenEveryAccountSoftCools(t *testing.T) {
	const model = "deepseek-v4.1-flash"
	cands := admissionCandidates(t)
	stubAuthSources(t,
		[]pluginapi.HostAuthFileEntry{testAuthEntry("adm-a", "uid-a", false), testAuthEntry("adm-b", "uid-b", false)},
		map[string]*storedAuth{"adm-a": cands[0].Auth, "adm-b": cands[1].Auth})

	for _, c := range cands {
		markCandidateUnavailable(&failoverCandidate{AuthID: c.AuthID, RecordID: c.RecordID, Auth: c.Auth, RateModel: model}, outcomeRateLimited, "")
	}
	// Re-query so the cooldown state is visible to the pool builder.
	invalidateAuthListCache()
	pool := buildFailoverPool("adm-a", model)
	if pool.Primary == nil {
		t.Fatal("all-cooling pool has no recovery probe; the provider would answer errors for the whole window")
	}
	if !pool.Primary.RecoveryProbe {
		t.Fatalf("probe flag not set on pool primary: %+v", pool.Primary)
	}
	if len(pool.Others) != 0 {
		t.Fatalf("probe pool must keep exactly one candidate, got others=%+v", pool.Others)
	}

	// Without any cooldown the pool must NOT mark a probe.
	resetModelCooldownFor(cands[0], model)
	resetModelCooldownFor(cands[1], model)
	invalidateAuthListCache()
	clean := buildFailoverPool("adm-a", model)
	if clean.Primary != nil && clean.Primary.RecoveryProbe {
		t.Fatal("probe flag set without any active cooldown")
	}
}

// TestConcurrentProbeIsSingleFlight locks that a burst arriving while every
// account is soft-cooling sends only ONE upstream probe: the next backoff is
// reserved under the cooldown lock before the I/O starts, so ten concurrent
// callers cannot all stampede a rate-limited account.
func TestConcurrentProbeIsSingleFlight(t *testing.T) {
	const model = "deepseek-v4.1-flash"
	cands := admissionCandidates(t)
	c := cands[0]
	c.RateModel = model

	markCandidateUnavailable(c, outcomeRateLimited, "")
	readyProbe(t, c)
	c.RecoveryProbe = true

	var calls atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release, err := admitCandidateAttempt(context.Background(), c)
			if err != nil || release == nil {
				return // skipped: another slot owns the probe
			}
			calls.Add(1)
			time.Sleep(10 * time.Millisecond)
			release()
		}()
	}
	close(start)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("probe storm: %d concurrent upstream attempts, want exactly 1", got)
	}
	c.RecoveryProbe = false
}

// TestFailedProbeDoesNotResetCooldown locks that a probe which is itself rate
// limited keeps the account cooling, while a successful one lifts the
// quarantine — the two halves of the recovery contract.
func TestFailedProbeDoesNotResetCooldown(t *testing.T) {
	const model = "deepseek-v4.1-flash"
	cands := admissionCandidates(t)
	c := cands[0]
	c.RateModel = model

	markCandidateUnavailable(c, outcomeRateLimited, "")
	readyProbe(t, c)
	c.RecoveryProbe = true

	release, err := admitCandidateAttempt(context.Background(), c)
	if err != nil || release == nil {
		t.Fatalf("probe admission failed: release=%v err=%v", release != nil, err)
	}
	release()
	// The probe consumed its reservation: the next attempt backs off again.
	if !candidateOnCooldown(c) {
		t.Fatal("probe cleared the cooldown without any upstream success")
	}
	if ready := candidateProbeReady(c); ready {
		t.Fatal("probe reservation was not renewed, so the next caller would stampede")
	}

	// A proved-successful attempt lifts the model-scoped quarantine.
	markCandidateUnavailable(c, outcomeRateLimited, "")
	resetCandidateCooldown(c)
	if isModelOnCooldown(c.AuthID, model) {
		t.Fatal("successful attempt did not clear the model quarantine")
	}
}

// TestRateModelResolvesAliasNotClientModel locks the rate-scope key: an alias
// (client-facing name) must resolve to the real upstream model id before it
// becomes part of a cooldown key, otherwise an alias would create a fresh,
// unthrottled entry for a model that is already rate-limited.
func TestRateModelResolvesAliasNotClientModel(t *testing.T) {
	const upstream = "gpt-6.1-sol"
	const clientAlias = "sol"

	// Seed the alias cache the way host config loading does.
	modelAliasCache.Lock()
	prev := modelAliasCache.byAlias
	modelAliasCache.byAlias = map[string]string{clientAlias: upstream}
	modelAliasCache.Unlock()
	t.Cleanup(func() {
		modelAliasCache.Lock()
		modelAliasCache.byAlias = prev
		modelAliasCache.Unlock()
	})

	if got := candidateRateModel(clientAlias, nil); got != upstream {
		t.Fatalf("candidateRateModel(%q) = %q, want %q", clientAlias, got, upstream)
	}
	if got := candidateRateModel("  ", nil); got != "" {
		t.Fatalf("blank model must yield an empty rate scope, got %q", got)
	}

	// The alias and its target must share ONE cooldown entry: cooling the
	// resolved id must hide the client alias too.
	resetModelCooldown("adm-alias", upstream)
	resetModelCooldown("adm-alias", clientAlias)
	defer func() {
		resetModelCooldown("adm-alias", upstream)
		resetModelCooldown("adm-alias", clientAlias)
	}()

	markModelCooldown("adm-alias", candidateRateModel(clientAlias, nil), time.Now().Add(10*time.Minute), "429")
	if !isModelOnCooldown("adm-alias", upstream) {
		t.Fatal("cooling via the client alias did not cool the resolved upstream model")
	}
	if isModelOnCooldown("adm-alias", clientAlias) {
		t.Fatal("the alias key itself was used instead of the resolved upstream id")
	}
}
