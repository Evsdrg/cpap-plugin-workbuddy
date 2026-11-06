// failover.go implements non-destructive multi-account cooldowns for upstream
// rate limits (HTTP 429 / CodeBuddy code 6004 "usage exceeds frequency limit")
// and explicit account-credit exhaustion (code 14018 / 402).
//
// The CPA host's built-in cooldown+retry only applies to in-host executors;
// c-shared plugins execute upstream requests themselves, so a 429 never
// reaches the host selector and no failover happens. This file keeps an
// in-plugin cooldown map and marks accounts (or one account+model pair) so the
// executor retry loop can pick another candidate for the SAME request.
//
// Scope model (mirrors upstream v0.9.7):
//   - soft rate limits (429/6004) are scoped to the ACCOUNT + upstream MODEL,
//     because the 6004 body itself says "switch to the other models to
//     continue"; the account keeps serving its other models.
//   - credit exhaustion is ACCOUNT-wide ("credits_exhausted") and is never
//     probed early — only a real reset (or manual clear) lifts it.
//
// Keys are attacker-uncontrolled internal strings; both account identity keys
// and "model:<id>:<model>" keys live in ONE map so the lazy-expiry and
// probe-reservation logic stays in one place.
package main

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// authCooldown is one cooldown entry (account-wide or account+model).
type authCooldown struct {
	NextRetryAt time.Time // may be retried after this instant
	Reason      string    // "429", "credits_exhausted" or cooldownReasonFirstTokenTimeout
	ProbeAt     time.Time // soft 429 may be probed before the full reset
}

// cooldownReasonFirstTokenTimeout is the fork's first-token watchdog verdict:
// the account's daily quota for one model is presumed exhausted until the next
// Asia/Shanghai midnight. It ranks above a soft 429 (which must never shorten
// it) and is never probe-eligible.
const cooldownReasonFirstTokenTimeout = "first_token_timeout"

// cooldownReasonEmptyStreamQuota is the verdict for an upstream that answered
// HTTP 200 but closed the stream before the first chunk. Upstream expresses
// "this account's quota for this model is used up today" that way rather than
// with a 4xx, so it is indistinguishable from a transient network reset on a
// single observation — hence the consecutive-strike requirement in
// recordEmptyStreamStrike.
const cooldownReasonEmptyStreamQuota = "empty_stream_quota"

// cooldownReasonRank orders verdicts by severity so a late, weaker observation
// cannot overwrite or shorten a stronger active one:
// credits_exhausted (2) > first_token_timeout / empty_stream_quota (1) > soft 429 (0).
func cooldownReasonRank(reason string) int {
	switch reason {
	case "credits_exhausted":
		return 2
	case cooldownReasonFirstTokenTimeout, cooldownReasonEmptyStreamQuota:
		return 1
	default:
		return 0
	}
}

var (
	cooldownMu sync.Mutex
	cooldowns  = map[string]authCooldown{} // key: account identity or model:<id>:<model>
)

// Soft rate limits recover; never let one burst freeze a model for the whole
// default window. Credit exhaustion keeps the long backoff.
const maxRateLimitCooldown = 10 * time.Minute
const rateLimitProbeInterval = 30 * time.Second

// modelCooldownKey scopes a soft rate limit to one account+upstream model.
func modelCooldownKey(authID, model string) string {
	authID = strings.TrimSpace(authID)
	model = strings.ToLower(strings.TrimSpace(model))
	if authID == "" || model == "" {
		return ""
	}
	return "model:" + authID + ":" + model
}

// markModelCooldown quarantines one cooldown identity + upstream model pair
// until resetAt. It is the model-scoped counterpart of markAuthCooldownReason:
// the account keeps serving its other models, which is what upstream 6004
// ("switch to the other models to continue") actually means. reason must be an
// internal verdict ("429" / cooldownReasonFirstTokenTimeout /
// "credits_exhausted").
func markModelCooldown(authID, model string, resetAt time.Time, reason string) {
	markAuthCooldownReason(modelCooldownKey(authID, model), resetAt, reason)
}

// isModelOnCooldown reports whether one cooldown identity + model pair is
// still cooling. The account-wide key is deliberately NOT consulted: a soft
// rate limit scoped to one model must not hide that account's other models.
func isModelOnCooldown(authID, model string) bool {
	key := modelCooldownKey(authID, model)
	if key == "" {
		return false
	}
	return isAuthOnCooldown(key)
}

// resetModelCooldown clears one cooldown identity + model quarantine, leaving
// that identity's other models and any account-wide verdict untouched.
func resetModelCooldown(authID, model string) bool {
	return resetAuthCooldown(modelCooldownKey(authID, model))
}

// 清除一个身份下的账号级与模型级冷却，并同步清除空流连续计数。
func resetCooldownsForIdentities(identities []string) int {
	identitySet := make(map[string]struct{}, len(identities))
	for _, identity := range identities {
		if identity = strings.TrimSpace(identity); identity != "" {
			identitySet[identity] = struct{}{}
		}
	}
	if len(identitySet) == 0 {
		return 0
	}
	matches := func(key string) bool {
		if _, ok := identitySet[key]; ok {
			return true
		}
		for identity := range identitySet {
			if strings.HasPrefix(key, "model:"+identity+":") {
				return true
			}
		}
		return false
	}
	cooldownMu.Lock()
	cleared := 0
	for key := range cooldowns {
		if matches(key) {
			delete(cooldowns, key)
			cleared++
		}
	}
	cooldownMu.Unlock()
	emptyStreamStrikesMu.Lock()
	for key := range emptyStreamStrikes {
		if matches(key) {
			delete(emptyStreamStrikes, key)
		}
	}
	emptyStreamStrikesMu.Unlock()
	return cleared
}

// defaultCooldownDuration mirrors the CPA built-in 30-minute cooldown used for
// soft rate limits when the upstream reset time cannot be parsed.
const defaultCooldownDuration = 30 * time.Minute

// markAuthCooldown records a soft rate limit for the account (account-wide key
// or, when callers pass a composite key, the account+model key).
func markAuthCooldown(authID string, resetAt time.Time) {
	markAuthCooldownReason(authID, resetAt, "429")
}

// markAuthCooldownReason records a cooldown with an explicit reason. A weaker
// verdict never shortens a stronger active one (see cooldownReasonRank): a soft
// 429 observed after a first-token-timeout or credit-exhaustion verdict is
// dropped, while a stronger verdict always replaces a weaker one.
func markAuthCooldownReason(authID string, resetAt time.Time, reason string) {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	now := time.Now()
	if resetAt.IsZero() || !resetAt.After(now) {
		resetAt = now.Add(defaultCooldownDuration)
	}
	if reason == "429" && resetAt.After(now.Add(maxRateLimitCooldown)) {
		resetAt = now.Add(maxRateLimitCooldown)
	}
	probeAt := now.Add(rateLimitProbeInterval)
	if resetAt.Before(probeAt) {
		probeAt = resetAt
	}
	cooldownMu.Lock()
	// A stronger active verdict must never be overwritten or shortened by a
	// weaker one: first_token_timeout (until the next China midnight) and
	// credits_exhausted outrank a soft 429, whose reset can be seconds away.
	old := cooldowns[authID]
	if old.NextRetryAt.After(now) && cooldownReasonRank(old.Reason) > cooldownReasonRank(reason) {
		cooldownMu.Unlock()
		return
	}
	cooldowns[authID] = authCooldown{NextRetryAt: resetAt, Reason: reason, ProbeAt: probeAt}
	cooldownMu.Unlock()
}

// isAuthOnCooldown reports whether the key is still inside its cooldown
// window. Expired entries are cleared lazily on read.
func isAuthOnCooldown(authID string) bool {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return false
	}
	cooldownMu.Lock()
	defer cooldownMu.Unlock()
	c, ok := cooldowns[authID]
	if !ok {
		return false
	}
	if time.Now().Before(c.NextRetryAt) {
		return true
	}
	delete(cooldowns, authID)
	return false
}

// authCooldownUntil returns the instant the key may be retried again, or the
// zero time when it is not cooling down.
func authCooldownUntil(authID string) time.Time {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return time.Time{}
	}
	cooldownMu.Lock()
	defer cooldownMu.Unlock()
	c, ok := cooldowns[authID]
	if !ok || !time.Now().Before(c.NextRetryAt) {
		return time.Time{}
	}
	return c.NextRetryAt
}

// resetAuthCooldown clears any cooldown for the key (used by tests, the
// success path, and the host-side cooldown reset).
func resetAuthCooldown(authID string) bool {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return false
	}
	cooldownMu.Lock()
	_, existed := cooldowns[authID]
	delete(cooldowns, authID)
	cooldownMu.Unlock()
	return existed
}

// rateLimitResetRe matches the CodeBuddy 429 message's reset timestamp, e.g.
// "usage exceeds frequency limit, please try again later, reset at
// 2026-09-06 06:01:58 UTC+8". Only UTC±H offsets are parsed; absent/other
// offsets fall back to the default cooldown.
var rateLimitResetRe = regexp.MustCompile(`reset\s+at\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s*(UTC(?:\+|-)\d{1,2})?`)

// parseRateLimitResetAt extracts the reset instant from an upstream 429 body.
// Returns the zero time when no parseable reset time is present (callers then
// fall back to defaultCooldownDuration).
func parseRateLimitResetAt(body string) time.Time {
	m := rateLimitResetRe.FindStringSubmatch(body)
	if len(m) < 2 {
		return time.Time{}
	}
	loc := time.FixedZone("UTC+8", 8*60*60) // CodeBuddy reports UTC+8 by default
	if len(m) > 2 && m[2] != "" {
		off := 0
		if h, err := strconv.Atoi(strings.TrimPrefix(strings.TrimPrefix(m[2], "UTC+"), "UTC-")); err == nil {
			if strings.HasPrefix(m[2], "UTC-") {
				off = -h
			} else {
				off = h
			}
		}
		loc = time.FixedZone(m[2], off*60*60)
	}
	t, err := time.ParseInLocation("2006-01-02 15:04:05", m[1], loc)
	if err != nil {
		return time.Time{}
	}
	return t
}

// Explicit upstream credit rejection is an account problem, unlike an empty
// HTTP 200 stream (possibly billed) or a transport error. Retry only when a
// non-success HTTP status is known. Code 14018 also works without English text
// ("credits exhausted" is localized in some realms).
var exhaustedCodeRe = regexp.MustCompile(`"code"\s*:\s*14018\b`)

// isAccountExhaustedResponse reports an explicit credit rejection that should
// skip the account and try the next one instead of stopping the request.
func isAccountExhaustedResponse(statusCode int, body string) bool {
	return statusCode >= 400 && (isHardCreditError(statusCode, body) || exhaustedCodeRe.MatchString(body))
}

// isRateLimitResponse reports whether an upstream chat response is a soft rate
// limit that should trigger failover: HTTP 429, CodeBuddy code 6004, or
// "frequency limit" / "rate limit" wording in the body. Hard credit errors
// (402/insufficient credit) never trigger failover.
func isRateLimitResponse(statusCode int, body string) bool {
	if isHardCreditError(statusCode, body) {
		return false
	}
	if statusCode == 429 {
		return true
	}
	lower := strings.ToLower(body)
	return strings.Contains(lower, "6004") ||
		strings.Contains(lower, "frequency limit") ||
		strings.Contains(lower, "rate limit") ||
		strings.Contains(lower, "too many requests")
}

// nextChinaMidnight returns the next Asia/Shanghai midnight — the daily-quota
// reset boundary used by the first-token watchdog.
func nextChinaMidnight(now time.Time) time.Time {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		loc = time.FixedZone("UTC+8", 8*60*60)
	}
	localNow := now.In(loc)
	return time.Date(localNow.Year(), localNow.Month(), localNow.Day()+1, 0, 0, 0, 0, loc)
}

// Same-account retry policy. When an account answers a rate limit the failover
// loop first re-attempts THAT SAME account up to sameAccountRetryCount extra
// times, sleeping sameAccountRetryDelay between attempts, before cooling the
// account down and moving on to the next candidate. Rationale: a CodeBuddy
// "frequency limit" is often a brief burst throttle that clears within seconds,
// so switching accounts immediately (the pre-0.9.3 behavior) burned a second
// account's quota for what one short wait would have recovered. Set
// retry_per_account = 0 in plugin config to restore immediate failover.
//
// Double-billing guard: only the pre-chunk rate-limit outcome is retried. The
// empty_stream and post-emit cases remain non-retryable (see runFailover /
// runStreamFailover), exactly as before.
var (
	retryPolicyMu         sync.RWMutex
	sameAccountRetryCount = 3
	sameAccountRetryDelay = 2 * time.Second
)

// sameAccountRetryLimit returns the number of extra same-account attempts.
func sameAccountRetryLimit() int {
	retryPolicyMu.RLock()
	defer retryPolicyMu.RUnlock()
	return sameAccountRetryCount
}

// sameAccountRetryDelayValue returns the delay between same-account attempts.
func sameAccountRetryDelayValue() time.Duration {
	retryPolicyMu.RLock()
	defer retryPolicyMu.RUnlock()
	return sameAccountRetryDelay
}

// setSameAccountRetry updates the policy. Negative values clamp to 0 (no
// same-account retry, i.e. the legacy immediate-failover behavior).
func setSameAccountRetry(count int, delay time.Duration) {
	if count < 0 {
		count = 0
	}
	if delay < 0 {
		delay = 0
	}
	retryPolicyMu.Lock()
	sameAccountRetryCount = count
	sameAccountRetryDelay = delay
	retryPolicyMu.Unlock()
}

// ---------------------------------------------------------------------------
// Consecutive-empty-stream accounting
// ---------------------------------------------------------------------------

// emptyStreamStrikeThreshold is how many consecutive HTTP-200-but-no-data
// observations one account+model must accumulate before it is treated as a
// daily-quota verdict.
//
// Five is deliberately high. A single empty stream is indistinguishable from a
// transient network reset or an upstream blip, and the penalty for a false
// positive is severe: the account+model pair is quarantined until the next
// Asia/Shanghai midnight, i.e. it stops receiving traffic for the rest of the
// day. Requiring five consecutive strikes makes a coincidence very unlikely
// while still catching a real quota wall quickly — a burst of parallel requests
// against an exhausted account reaches five within seconds.
const emptyStreamStrikeThreshold = 5

var (
	emptyStreamStrikesMu sync.Mutex
	emptyStreamStrikes   = map[string]int{} // key: modelCooldownKey(authID, model)
)

// recordEmptyStreamStrike counts one empty-stream observation for an
// account+model pair and reports whether the consecutive-strike threshold has
// now been reached. A success must call resetEmptyStreamStrikes so the count
// stays "consecutive" rather than "cumulative".
func recordEmptyStreamStrike(authID, model string) bool {
	key := modelCooldownKey(authID, model)
	if key == "" {
		// Without a usable identity we cannot scope a quarantine, so refusing to
		// reach the verdict is the only safe answer.
		return false
	}
	emptyStreamStrikesMu.Lock()
	defer emptyStreamStrikesMu.Unlock()
	emptyStreamStrikes[key]++
	return emptyStreamStrikes[key] >= emptyStreamStrikeThreshold
}

// resetEmptyStreamStrikes clears the counter for one account+model pair. Called
// whenever that pair answers successfully, which is what makes the count
// consecutive instead of cumulative.
func resetEmptyStreamStrikes(authID, model string) {
	key := modelCooldownKey(authID, model)
	if key == "" {
		return
	}
	emptyStreamStrikesMu.Lock()
	defer emptyStreamStrikesMu.Unlock()
	delete(emptyStreamStrikes, key)
}

// resetEmptyStreamStrikesForModel clears the counter for every identity of a
// candidate, because candidateCooldownIDs expands one candidate into the
// several keys the cooldown map knows it by.
func resetEmptyStreamStrikesForModel(cand *failoverCandidate, model string) {
	for _, key := range candidateCooldownIDs(cand) {
		resetEmptyStreamStrikes(key, model)
	}
}
