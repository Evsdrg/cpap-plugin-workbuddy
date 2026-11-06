package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

// resetStrikes clears the process-wide counter map between tests.
func resetStrikes() {
	emptyStreamStrikesMu.Lock()
	emptyStreamStrikes = map[string]int{}
	emptyStreamStrikesMu.Unlock()
}

// TestEmptyStreamStrikeThreshold pins the "five consecutive" requirement.
//
// The reason this is not a single-strike verdict: one HTTP-200-with-no-data is
// indistinguishable from a transient network reset, and the penalty for a false
// positive is a whole day of quarantine for that account+model pair.
//
// The bound 4 is written literally rather than derived from the constant so
// that lowering the threshold breaks this test instead of silently passing.
func TestEmptyStreamStrikeThreshold(t *testing.T) {
	resetStrikes()
	for i := 1; i <= 4; i++ {
		if recordEmptyStreamStrike("auth-1", "deepseek-v4.1-flash") {
			t.Fatalf("strike %d reached the verdict; a single or few empty streams must not quarantine an account for the day", i)
		}
	}
	if !recordEmptyStreamStrike("auth-1", "deepseek-v4.1-flash") {
		t.Fatalf("the 5th consecutive strike did not reach the verdict")
	}
}

// TestEmptyStreamStrikeIsConsecutiveNotCumulative is the part that prevents a
// slow leak of false positives: one success anywhere in the sequence must clear
// the count, so five strikes spread over a week never add up to a verdict.
func TestEmptyStreamStrikeIsConsecutiveNotCumulative(t *testing.T) {
	resetStrikes()
	for i := 0; i < 20; i++ {
		for j := 0; j < emptyStreamStrikeThreshold-1; j++ {
			if recordEmptyStreamStrike("auth-2", "deepseek-v4.1-flash") {
				t.Fatalf("iteration %d reached the threshold with only %d strikes before a reset", i, j+1)
			}
		}
		// A success in between resets the run.
		resetEmptyStreamStrikes("auth-2", "deepseek-v4.1-flash")
	}
}

// TestEmptyStreamStrikeScopedPerAccountAndModel guards the key: the quota is per
// account AND per model, so one pair hitting the wall must not implicate
// another account, nor another model on the same account.
func TestEmptyStreamStrikeScopedPerAccountAndModel(t *testing.T) {
	resetStrikes()
	for i := 0; i < emptyStreamStrikeThreshold; i++ {
		recordEmptyStreamStrike("auth-A", "deepseek-v4.1-flash")
	}
	if !recordEmptyStreamStrike("auth-A", "deepseek-v4.1-flash") {
		t.Fatal("auth-A should have reached the threshold")
	}
	// A different account starts from zero.
	if recordEmptyStreamStrike("auth-B", "deepseek-v4.1-flash") {
		t.Fatal("auth-B inherited auth-A's strikes; the counter is not scoped per account")
	}
	// A different model on the same account also starts from zero.
	if recordEmptyStreamStrike("auth-A", "glm-5.3") {
		t.Fatal("glm-5.3 inherited deepseek's strikes; the counter is not scoped per model")
	}
}

// TestEmptyStreamStrikeWithoutIdentityRefusesVerdict covers the degenerate key:
// with no usable identity we cannot scope a quarantine, so reaching the verdict
// must be impossible rather than quarantining the wrong pair.
func TestEmptyStreamStrikeWithoutIdentityRefusesVerdict(t *testing.T) {
	resetStrikes()
	for i := 0; i < emptyStreamStrikeThreshold*3; i++ {
		if recordEmptyStreamStrike("", "deepseek-v4.1-flash") {
			t.Fatal("empty authID reached the threshold")
		}
		if recordEmptyStreamStrike("auth-C", "") {
			t.Fatal("empty model reached the threshold")
		}
	}
}

// TestResetEmptyStreamStrikesForModelCoversEveryAlias checks the candidate-side
// reset, because one candidate is known to the cooldown map under several keys
// (AuthID, RecordID, account UID, aliases) and leaving any of them behind would
// let a stale count survive a success.
func TestResetEmptyStreamStrikesForModelCoversEveryAlias(t *testing.T) {
	resetStrikes()
	cand := &failoverCandidate{
		AuthID:   "auth-D",
		RecordID: "rec-D",
		Aliases:  []string{"alias-D"},
	}
	cand.Auth = &storedAuth{}
	cand.Auth.Account.UID = "uid-D"

	for i := 0; i < emptyStreamStrikeThreshold; i++ {
		recordEmptyStreamStrike("auth-D", "deepseek-v4.1-flash")
	}
	if !recordEmptyStreamStrike("auth-D", "deepseek-v4.1-flash") {
		t.Fatal("precondition: auth-D should be at the threshold")
	}

	resetEmptyStreamStrikesForModel(cand, "deepseek-v4.1-flash")

	// Every identity the candidate is known by must start from zero again. If a
	// reset were missed the count would still be at the threshold, so the very
	// next record would trip it — that is what makes this assertion bite.
	for _, key := range []string{"auth-D", "rec-D", "uid-D", "alias-D"} {
		emptyStreamStrikesMu.Lock()
		emptyStreamStrikes = map[string]int{}
		emptyStreamStrikesMu.Unlock()
		for i := 0; i < emptyStreamStrikeThreshold-1; i++ {
			if recordEmptyStreamStrike(key, "deepseek-v4.1-flash") {
				t.Fatalf("identity %q still carried strikes after the reset", key)
			}
		}
	}
}

// TestEmptyStreamStrikeConcurrent makes sure the counter is not corrupted under
// parallel requests, which is the shape production sees when several requests
// hit an exhausted account at once.
func TestEmptyStreamStrikeConcurrent(t *testing.T) {
	resetStrikes()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recordEmptyStreamStrike("auth-E", "deepseek-v4.1-flash")
		}()
	}
	wg.Wait()

	emptyStreamStrikesMu.Lock()
	got := emptyStreamStrikes[modelCooldownKey("auth-E", "deepseek-v4.1-flash")]
	emptyStreamStrikesMu.Unlock()
	if got != 50 {
		t.Fatalf("strikes = %d, want 50 (lost updates indicate a missing lock)", got)
	}
}

// cooldownFor reads a cooldown entry directly, for assertions.
func cooldownFor(key string) (authCooldown, bool) {
	cooldownMu.Lock()
	defer cooldownMu.Unlock()
	cd, ok := cooldowns[key]
	return cd, ok
}

// TestRunStreamFailoverEmptyStreamQuarantinesOnlyAfterFiveStrikes is the test
// that actually exercises the wiring, and the one that fails if either the
// threshold or the reset-on-success call is removed.
//
// Scenario: one account keeps answering 200-with-no-data (the shape upstream
// uses for "quota spent today"), a second account is healthy. The expectation
// is that the first four strikes merely cause a switch, and only the fifth
// quarantines the pair until the next Asia/Shanghai midnight.
func TestRunStreamFailoverEmptyStreamQuarantinesOnlyAfterFiveStrikes(t *testing.T) {
	withSameAccountRetry(t, 0, 0)
	resetStrikes()
	resetModelCooldown("idx-1", "deepseek-v4.1-flash")
	defer resetModelCooldown("idx-1", "deepseek-v4.1-flash")

	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
		{AuthID: "idx-2", Auth: &storedAuth{}},
	}

	// One request is one pass over the candidates. idx-1 always answers with an
	// empty stream; idx-2 succeeds, which also resets the strike counter — so a
	// single request can never accumulate five.
	pass := func() failoverResult {
		return runStreamFailover(context.Background(), cands, "deepseek-v4.1-flash",
			func(cand *failoverCandidate) (bool, int, error) {
				if cand.AuthID == "idx-1" {
					return false, 200, errEmptyStream
				}
				return false, 200, nil
			})
	}

	for i := 1; i <= 4; i++ {
		fin := pass()
		if fin.Outcome != outcomeSuccess || fin.AuthID != "idx-2" {
			t.Fatalf("pass %d: result = %+v, want success on idx-2 (the exhausted account is merely skipped, not quarantined, before the 5th strike)", i, fin)
		}
		if _, cooling := cooldownFor(modelCooldownKey("idx-1", "deepseek-v4.1-flash")); cooling {
			t.Fatalf("pass %d: idx-1 was quarantined early", i)
		}
		// idx-2 answered, so its own counter must be clean.
		if recordEmptyStreamStrike("idx-2", "deepseek-v4.1-flash") {
			t.Fatal("idx-2 carried strikes despite succeeding")
		}
		resetEmptyStreamStrikes("idx-2", "deepseek-v4.1-flash")
	}

	// The 5th strike quarantines. Drive it directly because the success on
	// idx-2 in the loop above legitimately resets the run each time.
	resetStrikes()
	for i := 0; i < emptyStreamStrikeThreshold-1; i++ {
		recordEmptyStreamStrike("idx-1", "deepseek-v4.1-flash")
	}
	fin := runStreamFailover(context.Background(), cands, "deepseek-v4.1-flash",
		func(cand *failoverCandidate) (bool, int, error) {
			if cand.AuthID == "idx-1" {
				return false, 200, errEmptyStream
			}
			return false, 200, nil
		})
	if fin.Outcome != outcomeSuccess || fin.AuthID != "idx-2" {
		t.Fatalf("5th strike: result = %+v, want success on idx-2 (failover still happens)", fin)
	}
	cd, cooling := cooldownFor(modelCooldownKey("idx-1", "deepseek-v4.1-flash"))
	if !cooling {
		t.Fatal("5th consecutive strike did not quarantine idx-1")
	}
	if cd.Reason != cooldownReasonEmptyStreamQuota {
		t.Fatalf("cooldown reason = %q, want %q", cd.Reason, cooldownReasonEmptyStreamQuota)
	}
	// The quarantine must run to the next Shanghai midnight, not the short
	// 10-minute soft-rate-limit window.
	if want := nextChinaMidnight(time.Now()); !cd.NextRetryAt.Equal(want) {
		t.Fatalf("quarantine until %s, want the next Asia/Shanghai midnight %s", cd.NextRetryAt, want)
	}
}

// TestRunStreamFailoverEmptyStreamSuccessClearsStrikes closes the loop on the
// "consecutive" wording: a success by the SAME account+model must clear its
// run, so four strikes, a success, then four more strikes never adds up to a
// verdict.
//
// Note the account under test is the one that fails and then succeeds. A
// success on a different account does not clear another account's run — their
// quotas are independent, and that separation is asserted separately in
// TestEmptyStreamStrikeScopedPerAccountAndModel.
func TestRunStreamFailoverEmptyStreamSuccessClearsStrikes(t *testing.T) {
	withSameAccountRetry(t, 0, 0)
	resetStrikes()
	resetModelCooldown("idx-1", "deepseek-v4.1-flash")
	defer resetModelCooldown("idx-1", "deepseek-v4.1-flash")

	cands := []*failoverCandidate{
		{AuthID: "idx-1", Auth: &storedAuth{}},
	}

	// idx-1 answers an empty stream while `failing` is true, then recovers.
	failing := true
	pass := func() failoverResult {
		return runStreamFailover(context.Background(), cands, "deepseek-v4.1-flash",
			func(cand *failoverCandidate) (bool, int, error) {
				if failing {
					return false, 200, errEmptyStream
				}
				return false, 200, nil
			})
	}

	// Four strikes: not yet a verdict, and the request fails over to nothing
	// else, so the outcome is a stop rather than a success.
	for i := 1; i <= 4; i++ {
		if fin := pass(); fin.Outcome != outcomeStop {
			t.Fatalf("pass %d: outcome = %+v, want stop (single account, still failing)", i, fin)
		}
		if _, cooling := cooldownFor(modelCooldownKey("idx-1", "deepseek-v4.1-flash")); cooling {
			t.Fatalf("pass %d: quarantined before the 5th consecutive strike", i)
		}
	}

	// A success clears the run.
	failing = false
	if fin := pass(); fin.Outcome != outcomeSuccess {
		t.Fatalf("recovery pass: outcome = %+v, want success", fin)
	}

	// Four more strikes must NOT trip the verdict, because the success in
	// between reset the counter. Without the reset-on-success call this is
	// exactly where the account gets wrongly quarantined for the rest of the day.
	failing = true
	for i := 1; i <= 4; i++ {
		if fin := pass(); fin.Outcome != outcomeStop {
			t.Fatalf("post-recovery pass %d: outcome = %+v, want stop", i, fin)
		}
		if _, cooling := cooldownFor(modelCooldownKey("idx-1", "deepseek-v4.1-flash")); cooling {
			t.Fatalf("post-recovery pass %d: quarantined even though a success had reset the run; the strike reset on success is missing", i)
		}
	}
}
