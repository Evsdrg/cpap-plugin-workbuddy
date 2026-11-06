// attempt_admission.go bounds chat concurrency per account without a local
// waiting queue. A busy candidate is skipped so another enabled account in the
// same realm can answer immediately. The host may still enforce its own
// request scheduling before the plugin executor is called.
//
// The cap is deliberately 1 (upstream settled on 2 after observing a 51s
// upstream response blocking a second session for the full 30s queue timeout
// before dropping the queue entirely). One in-flight chat per account keeps
// CodeBuddy's per-account frequency limit (code 6004) from being triggered by
// our own parallelism: the second concurrent request is routed to another
// account instead of doubling up on one.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const maxAccountChatInFlight = 1

var errAccountBusy = errors.New("account chat capacity reached")

type accountChatGate struct{ inFlight int }

var chatGateMu sync.Mutex
var chatGates = map[string]*accountChatGate{}

// This is the same state used for admission, not a separately estimated load.
// The map is a snapshot so a management response never holds the gate lock
// while encoding or serving HTTP. Idle accounts are absent and count as zero.
func accountInFlightSnapshot() map[string]int {
	chatGateMu.Lock()
	defer chatGateMu.Unlock()
	out := make(map[string]int, len(chatGates))
	for key, gate := range chatGates {
		out[key] = gate.inFlight
	}
	return out
}

// candidateRateModel resolves the client-facing model to the actual upstream
// model id so a soft rate limit is scoped to what the upstream really ran.
// Aliases ("sol" → "gpt-5.6-sol") must not create a fresh, unthrottled key.
func candidateRateModel(model string, auth *storedAuth) string {
	if strings.TrimSpace(model) == "" {
		return ""
	}
	return resolveUpstreamModel(model, nil)
}

// Use the maximum expiry across every identity, and preserve hard-credit
// rejection even if another alias/model has a newer soft limit.
func candidateCooldownState(c *failoverCandidate) authCooldown {
	cooldownMu.Lock()
	defer cooldownMu.Unlock()
	return candidateCooldownStateLocked(c, time.Now())
}

func candidateCooldownStateLocked(c *failoverCandidate, now time.Time) authCooldown {
	var out authCooldown
	for _, id := range candidateCooldownIDs(c) {
		keys := []string{id}
		if c.RateModel != "" {
			keys = append(keys, modelCooldownKey(id, c.RateModel))
		}
		for _, key := range keys {
			cd, ok := cooldowns[key]
			if !ok {
				continue
			}
			if !cd.NextRetryAt.After(now) {
				delete(cooldowns, key)
				continue
			}
			if cd.NextRetryAt.After(out.NextRetryAt) {
				out.NextRetryAt = cd.NextRetryAt
			}
			if cd.ProbeAt.After(out.ProbeAt) {
				out.ProbeAt = cd.ProbeAt
			}
			if cd.Reason == "credits_exhausted" || out.Reason == "" {
				out.Reason = cd.Reason
			}
		}
	}
	return out
}

// candidateSoftCooldown reports a soft (probe-eligible) rate limit, as opposed
// to a hard credit exhaustion.
func candidateSoftCooldown(c *failoverCandidate) bool {
	cd := candidateCooldownState(c)
	return !cd.NextRetryAt.IsZero() && cd.Reason == "429"
}

// candidateProbeAt is when a cooling candidate may next be probed: the
// reserved probe instant, or the full expiry when no reservation exists.
func candidateProbeAt(c *failoverCandidate) time.Time {
	cd := candidateCooldownState(c)
	if cd.ProbeAt.IsZero() {
		return cd.NextRetryAt
	}
	return cd.ProbeAt
}

func candidateProbeReady(c *failoverCandidate) bool { return !candidateProbeAt(c).After(time.Now()) }

// attemptContext picks the context the failover loop should observe; the
// variadic form keeps older test callers source-compatible.
func attemptContext(contexts []context.Context) context.Context {
	if len(contexts) > 0 && contexts[0] != nil {
		return contexts[0]
	}
	return context.Background()
}

// accountGateKey identifies the real upstream account for admission: two auth
// records that resolve to the same UID (renamed file, re-imported credential)
// share one upstream quota and must share one gate.
func accountGateKey(c *failoverCandidate) string {
	if c.Auth != nil && c.Auth.Account.UID != "" {
		return accountRegion(c.Auth) + ":" + c.Auth.Account.UID
	}
	return c.AuthID
}

// admitCandidateAttempt acquires one chat slot for the candidate. Returns
// (release, nil) to proceed, (nil, nil) to skip without I/O (cooling and not a
// ready probe), or (nil, errAccountBusy) so the caller can try another account.
// The caller can immediately try another account when errAccountBusy is
// returned. On recheck, a newly quarantined account is skipped without an
// upstream call. Release is idempotent and removes idle gates. The slot
// serializes probe reservation with candidate admission.
func admitCandidateAttempt(ctx context.Context, c *failoverCandidate) (func(), error) {
	if c == nil || c.Auth == nil || c.AuthID == "" {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cd := candidateCooldownState(c)
	if !cd.NextRetryAt.IsZero() && (!c.RecoveryProbe || cd.Reason != "429" || !candidateProbeReady(c)) {
		return nil, nil
	}
	key := accountGateKey(c)
	chatGateMu.Lock()
	gate := chatGates[key]
	if gate == nil {
		gate = &accountChatGate{}
		chatGates[key] = gate
	}
	if gate.inFlight >= maxAccountChatInFlight {
		chatGateMu.Unlock()
		return nil, errAccountBusy
	}
	gate.inFlight++
	chatGateMu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			chatGateMu.Lock()
			gate.inFlight--
			if gate.inFlight == 0 && chatGates[key] == gate {
				delete(chatGates, key)
			}
			chatGateMu.Unlock()
		})
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	// Recheck and reserve under the SAME lock. Two in-flight slots must not
	// both pass a ready-probe check before either reserves the next backoff.
	cooldownMu.Lock()
	now := time.Now()
	cd = candidateCooldownStateLocked(c, now)
	if !cd.NextRetryAt.IsZero() {
		if !c.RecoveryProbe || cd.Reason != "429" || cd.ProbeAt.After(now) {
			cooldownMu.Unlock()
			release()
			return nil, nil
		}
		// Reserve the next probe before I/O. A second slot sees this
		// reservation and skips the attempt.
		for _, id := range candidateCooldownIDs(c) {
			keys := []string{id}
			if c.RateModel != "" {
				keys = append(keys, modelCooldownKey(id, c.RateModel))
			}
			for _, k := range keys {
				v, ok := cooldowns[k]
				if ok && v.Reason == "429" && v.NextRetryAt.After(now) {
					v.ProbeAt = now.Add(rateLimitProbeInterval)
					cooldowns[k] = v
				}
			}
		}
		cooldownMu.Unlock()
		fmt.Fprintf(os.Stderr, "[workbuddy] failover: probing rate-limited auth=%q model=%q\n", redactSecrets(c.AuthID), redactSecrets(c.RateModel))
		return release, nil
	}
	cooldownMu.Unlock()
	return release, nil
}
