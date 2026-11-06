// retry.go implements the executor-side failover loop: when the account a
// request was routed to answers HTTP 429 / code 6004 (soft rate limit) or an
// explicit credit-exhaustion rejection, the plugin retries the SAME request on
// another enabled account before returning an error to the client. The list of
// eligible accounts comes from the host auth store (hostAuthList + hostAuthGet
// — the same source the panel/usage use), not from a new parallel mechanism.
//
// v0.9.8 follows upstream v0.9.7's 6004 change:
//   - soft rate limits are scoped to account+upstream model (candidate.RateModel)
//     so one model's frequency limit no longer removes the whole account;
//   - when every eligible account of the realm is soft-cooling, the pool keeps
//     one recovery probe (30s backoff) instead of failing the request;
//   - per-account chat concurrency is bounded (see attempt_admission.go) and a
//     busy account is skipped, never queued.
//
// The fork-local same-account retry policy (retry_per_account) is preserved: a
// soft 429 re-attempts the same account before quarantining it, so the
// quarantine is only published after those retries are exhausted.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/Evsdrg/cepvor-cpa-plinth/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// failoverCandidate is one eligible account.
type failoverCandidate struct {
	AuthID string
	// RecordID is the host auth record ID, an extra cooldown identity.
	RecordID string
	// Aliases are further host-list identities (the physical filename) that
	// must observe the same quarantine.
	Aliases []string
	Auth    *storedAuth
	// RateModel scopes soft rate limits to the actual upstream model. Credit
	// exhaustion remains account-wide. RecoveryProbe is only set by the pool
	// when every eligible account is cooling; admission still enforces backoff.
	RateModel     string
	RecoveryProbe bool
}

// failoverPool is a single-request snapshot of the eligible accounts.
type failoverPool struct {
	Primary *failoverCandidate // account the host routed to
	Others  []*failoverCandidate
}

// wbAuthListCached caches host auth list results briefly so the retry loop
// doesn't hammer host RPCs on every 429 (refresh ≤30s).
var (
	authListMu    sync.Mutex
	authListCache []pluginapi.HostAuthFileEntry
	authListAt    time.Time
	// authListSource is the host list provider. Overridden by tests to avoid
	// host RPC; production code never reassigns it.
	authListSource = hostAuthList
	// authGetSource is the host credential provider. Overridden by tests.
	authGetSource = hostAuthGet
)

func loadAuthList() ([]pluginapi.HostAuthFileEntry, error) {
	authListMu.Lock()
	defer authListMu.Unlock()
	if authListCache != nil && time.Since(authListAt) < 30*time.Second {
		return authListCache, nil
	}
	files, err := authListSource()
	if err != nil {
		return nil, err
	}
	authListCache = files
	authListAt = time.Now()
	return files, nil
}

// invalidateAuthListCache drops the cached host list so the next loadAuthList
// re-reads (called after auth enable/disable/delete changes).
func invalidateAuthListCache() {
	authListMu.Lock()
	authListCache = nil
	authListAt = time.Time{}
	authListMu.Unlock()
}

// listAuthEntryDisabled mirrors candidateDisabled for HostAuthFileEntry.
func listAuthEntryDisabled(f pluginapi.HostAuthFileEntry) bool {
	if f.Disabled {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(f.Status), "disabled")
}

// buildFailoverPool resolves the routed auth plus every other enabled
// workbuddy account the host knows about. Candidates flagged disabled or
// currently cooling down are excluded; when every candidate is under a SOFT
// rate limit the pool keeps exactly one recovery probe (the routed account
// when it is probe-ready, otherwise whichever recovers first) so a burst that
// rate-limited all accounts does not make the provider answer errors for the
// full cooldown window. Credit-exhausted accounts are never probed. When the
// host list is unavailable (tests / bridge down) the pool contains only the
// routed auth.
//
// model must be the RESOLVED upstream model id: soft rate limits are scoped to
// account+model, so the pool must filter and build keys by what the upstream
// really ran, not by the client-facing alias.
func buildFailoverPool(routedAuthID, model string) *failoverPool {
	pool := &failoverPool{}
	var cooling []*failoverCandidate
	files, err := loadAuthList()
	if err != nil {
		primary := &failoverCandidate{AuthID: routedAuthID, RecordID: routedAuthID}
		primary.RateModel = candidateRateModel(model, primary.Auth)
		if !candidateOnCooldown(primary) || candidateSoftCooldown(primary) {
			primary.RecoveryProbe = candidateOnCooldown(primary)
			pool.Primary = primary
		}
		return pool
	}
	wantName := ""
	if routedAuthID != "" {
		wantName = "workbuddy-" + routedAuthID + ".json"
	}
	routedMatched := false
	for _, f := range files {
		isRouted := routedAuthID != "" && (f.AuthIndex == routedAuthID || f.ID == routedAuthID ||
			f.Name == routedAuthID || listEntryMatchesUID(f, routedAuthID, wantName))
		if isRouted {
			routedMatched = true
		}
		if listAuthEntryDisabled(f) || f.AuthIndex == "" {
			continue
		}
		cand := &failoverCandidate{AuthID: f.AuthIndex, RecordID: f.ID}
		// The legacy single-file name is shared by every legacy account, so it
		// must never become a cooldown identity (one account's quarantine would
		// cool them all).
		if name := strings.TrimSpace(f.Name); name != "" && !isLegacyWorkbuddyAuthName(name) {
			cand.Aliases = []string{name}
		}
		if sa, getErr := authGetSource(f.AuthIndex); getErr == nil {
			cand.Auth = sa
		}
		cand.RateModel = candidateRateModel(model, cand.Auth)
		if candidateOnCooldown(cand) {
			if candidateSoftCooldown(cand) {
				if isRouted {
					cooling = append([]*failoverCandidate{cand}, cooling...)
				} else {
					cooling = append(cooling, cand)
				}
			}
			continue
		}
		if isRouted {
			pool.Primary = cand
			continue
		}
		pool.Others = append(pool.Others, cand)
	}
	if pool.Primary == nil && !routedMatched {
		// Routed account missing from the list (just added / bridge edge) —
		// synthesize so at least the first attempt happens with the storage
		// the executor already carries. When the list did contain the account,
		// an excluded/disabled/cooldown primary must stay excluded.
		primary := &failoverCandidate{AuthID: routedAuthID, RecordID: routedAuthID}
		primary.RateModel = candidateRateModel(model, primary.Auth)
		if !candidateOnCooldown(primary) {
			pool.Primary = primary
		}
	}
	if model != "" && pool.Primary == nil && len(pool.Others) == 0 && len(cooling) > 0 {
		pick := cooling[0]
		if !candidateProbeReady(pick) {
			for _, c := range cooling[1:] {
				if candidateProbeAt(c).Before(candidateProbeAt(pick)) {
					pick = c
				}
			}
		}
		pick.RecoveryProbe = true
		pool.Primary = pick
	}
	return pool
}

// ordered dereferences the pool into attemptable candidates. The primary is
// always first (its storage fallback comes from the executor request), then
// each fallback with the credential the host returned. Candidates whose auth
// could not be resolved are skipped.
func (p *failoverPool) ordered(primaryStorage []byte) []*failoverCandidate {
	var out []*failoverCandidate
	seen := map[string]bool{}
	add := func(c *failoverCandidate, storage []byte) {
		if c == nil || c.AuthID == "" || seen[c.AuthID] {
			return
		}
		seen[c.AuthID] = true
		auth := c.Auth
		aliases := append([]string(nil), c.Aliases...)
		if auth == nil && len(storage) > 0 {
			if sa, err := parseStored(storage); err == nil {
				auth = sa
				if len(aliases) == 0 {
					if name := authFileNameFor(sa); name != "" && !isLegacyWorkbuddyAuthName(name) {
						aliases = []string{name}
					}
				}
			}
		}
		out = append(out, &failoverCandidate{
			AuthID:        c.AuthID,
			RecordID:      c.RecordID,
			Aliases:       aliases,
			Auth:          auth,
			RateModel:     c.RateModel,
			RecoveryProbe: c.RecoveryProbe,
		})
	}
	add(p.Primary, primaryStorage)
	for _, c := range p.Others {
		add(c, nil)
	}
	return out
}

// upstreamAttempt performs ONE chat-completions round-trip (the upstream is
// always streamed; non-stream callers fold the SSE into a completion) and
// returns the folded completion payload. model is the fallback model name for
// the folded completion's "model" field.
//
// A 200 response that closes before any data chunk is treated as an error
// (errEmptyStream): the upstream accepted and billed the request but produced
// no output. Such attempts must NOT be retried on another account.
func upstreamAttempt(ctx context.Context, body []byte, model string, cand *failoverCandidate, metadata map[string]any) ([]byte, error) {
	if cand == nil || cand.Auth == nil {
		return nil, fmt.Errorf("no auth available")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointChatFor(cand.Auth), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	backendHeaders(httpReq, cand.Auth, canonicalSessionID(metadata))
	stream, statusCode, _, err := hostHTTPDoStream(httpReq)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	reader := newHostStreamReader(stream)
	if statusCode >= 400 {
		payload, _ := io.ReadAll(reader)
		return nil, &upstreamStatusError{status: statusCode, body: string(payload)}
	}
	completion, sawData, err := aggregateCompletionCounted(reader, model)
	if err != nil {
		return nil, err
	}
	if !sawData {
		return nil, errEmptyStream
	}
	return completion, nil
}

// upstreamStatusError carries an upstream non-2xx response through the
// failover loop so rate limits can be detected without parsing error strings.
type upstreamStatusError struct {
	status int
	body   string
	// code 是上游业务错误码（如 6004 限流、14018 积分耗尽）。
	// HTTP 状态码为 0 时，仅凭 code 也能判定失败性质。
	code int
}

func (e *upstreamStatusError) Error() string {
	if e.status > 0 {
		return fmt.Sprintf("upstream %d: %s", e.status, truncateRedacted(e.body, 200))
	}
	if e.code > 0 {
		return fmt.Sprintf("upstream code %d: %s", e.code, truncateRedacted(e.body, 200))
	}
	return fmt.Sprintf("upstream error: %s", truncateRedacted(e.body, 200))
}

func upstreamStatusOf(err error) (int, string) {
	var ue *upstreamStatusError
	if errors.As(err, &ue) {
		return ue.status, ue.body
	}
	return 0, ""
}

// upstreamCodeOf 取出上游业务错误码（6004 限流、14018 积分耗尽等）。
//
// 为什么必须有这个函数：CodeBuddy 会把限流/额度错误塞进 HTTP 200 的响应体，
// 这类错误的 HTTP 状态是 0，只靠 status 判断会一律落到 outcomeStop，
// 于是账号没有换、上游原文直接漏给客户端。
func upstreamCodeOf(err error) int {
	var ue *upstreamStatusError
	if errors.As(err, &ue) {
		return ue.code
	}
	return 0
}

// errEmptyStream marks an upstream that answered 200 but closed before any
// data chunk arrived.
//
// This is how the upstream reports "this account's quota for this model is spent
// today", so it IS an account problem and the failover loop moves on to the next
// account — a different one may still have quota. What it must never do is
// re-attempt the SAME account in a tight loop, and it must not quarantine the
// account on a single observation: a lone empty stream is indistinguishable from
// a transient network reset, so the quarantine verdict needs
// emptyStreamStrikeThreshold consecutive strikes.
//
// The earlier "never retried on another account" rule was written under the
// assumption that this was a request-level fault. That assumption turned out to
// be wrong, and it cost users a full day of unavailability: an exhausted account
// was never skipped, so every request failed until the quota reset.
var errEmptyStream = errors.New("empty_stream: upstream stream closed before first payload")

// isEmptyStreamError reports whether err is the plugin-side empty-stream
// marker (distinct from the host conductor's retryable empty_stream).
func isEmptyStreamError(err error) bool {
	return errors.Is(err, errEmptyStream)
}

type firstTokenTimeoutError struct {
	model   string
	resetAt time.Time
}

func (e *firstTokenTimeoutError) Error() string {
	return "first token timeout: model quota presumed exhausted"
}

var firstTokenTimeout = 3 * time.Minute

func isFirstTokenTimeout(err error) bool {
	var timeoutErr *firstTokenTimeoutError
	return errors.As(err, &timeoutErr)
}

// coolCandidateModel quarantines ONE account+model until resetAt — the
// first-token watchdog's verdict that the account's daily quota for that model
// is exhausted — and mirrors the state to the host model router. The local
// entry uses the first-token reason, so the 10-minute soft-rate-limit cap does
// not shorten the verdict and the entry is never probe-eligible.
// coolCandidateModel quarantines ONE account+model until resetAt.
//
// The two reason arguments are deliberately distinct and must not be merged:
//   - verdict is an internal severity token that cooldownReasonRank reads, so it
//     must be one of the cooldownReason* constants. Passing free text here would
//     silently rank a strong verdict as a soft 429 and let a later, weaker
//     observation shorten the quarantine.
//   - hostReason is the human-readable text mirrored to the host model router.
func coolCandidateModel(c *failoverCandidate, model string, resetAt time.Time, verdict, hostReason string) error {
	for _, key := range candidateCooldownIDs(c) {
		if modelCooldownKey(key, model) != "" {
			// The watchdog verdict outranks a soft 429; markModelCooldown's
			// severity guard keeps it from being shortened later.
			markModelCooldown(key, model, resetAt, verdict)
		}
	}
	return setHostModelCooldown(c.AuthID, model, resetAt, hostReason)
}

type attemptOutcome int

const (
	outcomeSuccess attemptOutcome = iota
	outcomeRateLimited
	outcomeAccountExhausted
	outcomeStop
	outcomeModelCooldown
)

// failoverResult is the loop verdict.
type failoverResult struct {
	Outcome attemptOutcome
	AuthID  string // account that produced the result
	Tried   []string
	Emitted bool // stream: at least one chunk reached the client
	// For outcomeStop / final failure:
	Status int
	Body   string
	Err    error
}

// failoverAttemptFn performs one attempt against cand and classifies it.
type failoverAttemptFn func(cand *failoverCandidate) (attemptOutcome, int, string, error)

// sleepWithContext waits for d, or returns false when ctx is done first.
func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// runFailover executes attemptFn across eligible accounts: the routed account
// first, then each fallback. Admission (one chat slot per account) gates every
// attempt: a busy account is skipped so another account can answer immediately
// — there is deliberately no local waiting queue. A soft rate-limited account
// is re-attempted on the SAME account up to sameAccountRetryLimit() extra times
// (sleeping sameAccountRetryDelayValue() between attempts); only after those
// retries are exhausted is the account quarantined (model-scoped for 429,
// account-wide for credit exhaustion) and the loop moves on. Any other failure
// stops the loop immediately.
//
// Retrying the same account first recovers brief "frequency limit" bursts
// without spending a second account's quota. Set retry_per_account = 0 to get
// the immediate-switch behavior (and quarantine-before-release, as upstream).
func runFailover(ctx context.Context, cands []*failoverCandidate, attemptFn failoverAttemptFn) failoverResult {
	res := failoverResult{Outcome: outcomeStop}
	extra := sameAccountRetryLimit()
	delay := sameAccountRetryDelayValue()
	busy := false
	for _, cand := range cands {
		if cand == nil || cand.Auth == nil || cand.AuthID == "" {
			continue
		}
		var (
			observed    bool
			lastOutcome attemptOutcome
			lastStatus  int
			lastBody    string
			lastErr     error
			cancelled   bool
		)
		for attemptIdx := 0; ; attemptIdx++ {
			if attemptIdx > 0 && !sleepWithContext(ctx, delay) {
				cancelled = true
				break
			}
			release, admissionErr := admitCandidateAttempt(ctx, cand)
			if errors.Is(admissionErr, errAccountBusy) {
				busy = true // another in-flight chat holds the account — skip it
				break
			}
			if admissionErr != nil {
				return admissionFailureResult(res, admissionErr)
			}
			if release == nil {
				break // quarantined (or probe already reserved) — no I/O
			}
			res.Tried = append(res.Tried, cand.AuthID)
			outcome, status, body, err := func() (attemptOutcome, int, string, error) {
				defer release()
				return attemptFn(cand)
			}()
			if outcome == outcomeSuccess {
				resetCandidateCooldown(cand)
				if len(res.Tried) > 1 {
					// 前几个账号都失败、这一下成功了：记下换过账号，否则排障时
					// 只能看到最终成功，看不出中间发生过切换。
					logging.Info(failoverHost(), "Workbuddy.failover：已切换到可用账号",
						logging.Auth(cand.AuthID),
						logging.Reason(fmt.Sprintf("此前已尝试 %d 个账号", len(res.Tried)-1)))
				}
				return failoverResult{Outcome: outcomeSuccess, AuthID: cand.AuthID, Tried: res.Tried}
			}
			if outcome == outcomeStop {
				return failoverResult{Outcome: outcomeStop, AuthID: cand.AuthID, Tried: res.Tried,
					Status: status, Body: body, Err: err}
			}
			observed = true
			lastOutcome, lastStatus, lastBody, lastErr = outcome, status, body, err
			// Hard credit exhaustion is not retried on the same account.
			if outcome == outcomeAccountExhausted || attemptIdx >= extra {
				break
			}
		}
		if observed {
			markCandidateUnavailable(cand, lastOutcome, lastBody)
			res.Outcome, res.AuthID = lastOutcome, cand.AuthID
			res.Status, res.Body, res.Err = lastStatus, lastBody, lastErr
			// 一个账号用尽或限流后被放弃，准备换下一个。这是排障时最重要的
			// 一行：谁被换掉、为什么、接下来还剩几个候选。
			logging.Warn(failoverHost(), "Workbuddy.failover：账号不可用，尝试下一个",
				logging.Auth(cand.AuthID),
				logging.State(failoverOutcomeName(lastOutcome)),
				logging.Err(lastErr))
		}
		if cancelled {
			// Client gone while waiting between same-account attempts: stop
			// without touching further accounts.
			return unavailableFailoverResult(res)
		}
	}
	return unavailableFailoverResult(res, busy)
}

// failoverHost 返回 failover 日志用的宿主句柄。
// failover 在 executor 的请求上下文里运行，但这里拿不到带 CallbackID 的句柄；
// 日志仍会带上 provider/auth_id，宿主会从它自己的回调上下文补 request_id。
func failoverHost() *abi.Host {
	return &abi.Host{}
}

// failoverOutcomeName 把内部枚举转成中文描述，避免日志里出现 Go 常量名。
func failoverOutcomeName(outcome attemptOutcome) string {
	switch outcome {
	case outcomeRateLimited:
		return "限流"
	case outcomeAccountExhausted:
		return "额度耗尽"
	case outcomeModelCooldown:
		return "模型冷却"
	case outcomeStop:
		return "不可重试的失败"
	case outcomeSuccess:
		return "成功"
	default:
		return "未知"
	}
}

// classifyNonStream classifies a folded (non-stream) attempt result.
func classifyNonStream(_ []byte, err error) (attemptOutcome, int, string, error) {
	if err == nil {
		return outcomeSuccess, 0, "", nil
	}
	status, body := upstreamStatusOf(err)
	code := upstreamCodeOf(err)
	if status == 0 && code == 0 {
		return outcomeStop, 0, "", err // transport error — account-independent
	}
	switch classifyUpstreamFailure(status, code, body) {
	case outcomeAccountExhausted:
		return outcomeAccountExhausted, status, body, err
	case outcomeRateLimited:
		return outcomeRateLimited, status, body, err
	default:
		return outcomeStop, status, body, err
	}
}

// 上游业务错误码（与 HTTP 状态码互相独立，见 error_envelope.go）。
const (
	codeFrequencyLimit   = 6004  // 账号频率限制
	codeCreditsExhausted = 14018 // 积分/额度耗尽
)

// classifyUpstreamFailure 决定一次上游失败应该换账号还是立即返回。
//
// 判据必须同时看 HTTP 状态码与业务错误码：CodeBuddy 的额度耗尽（14018）、
// 限流（6004）会以 HTTP 200 的信封形式出现，此时 status 是 0/200，只有 code
// 能表达失败性质。只用 status 判断会让这类错误落到 outcomeStop，于是账号不
// 切换、上游原文直接漏给客户端。
func classifyUpstreamFailure(status, code int, body string) attemptOutcome {
	if isAccountExhaustedResponse(status, body) || code == codeCreditsExhausted {
		return outcomeAccountExhausted
	}
	if isRateLimitResponse(status, body) || code == codeFrequencyLimit {
		return outcomeRateLimited
	}
	return outcomeStop
}

// runStreamFailover drives streaming attempts across eligible accounts. Once
// the first chunk reached the client (emitted) or a non-rate-limit failure
// occurs, the loop stops immediately. A rate limit observed BEFORE any chunk is
// emitted re-attempts the SAME account up to sameAccountRetryLimit() extra times
// (sleeping sameAccountRetryDelayValue() between attempts) before quarantining
// it and moving on. Set retry_per_account = 0 for the immediate-switch behavior.
// A first-token watchdog timeout quarantines the account+model until the next
// Asia/Shanghai midnight without a same-account retry; when EVERY attempted
// account hit that watchdog, the result carries outcomeModelCooldown so the
// pump can surface the dedicated message.
func runStreamFailover(ctx context.Context, cands []*failoverCandidate, requestedModel string, attempt func(cand *failoverCandidate) (emitted bool, status int, err error)) failoverResult {
	res := failoverResult{Outcome: outcomeStop}
	extra := sameAccountRetryLimit()
	delay := sameAccountRetryDelayValue()
	busy := false
	attemptedCount := 0
	modelLimitedCount := 0
	for _, cand := range cands {
		if cand == nil || cand.Auth == nil || cand.AuthID == "" {
			continue
		}
		var (
			observed     bool
			attempted    bool
			modelLimited bool
			lastOutcome  attemptOutcome
			lastStatus   int
			lastBody     string
			lastErr      error
			cancelled    bool
		)
		for attemptIdx := 0; ; attemptIdx++ {
			if attemptIdx > 0 && !sleepWithContext(ctx, delay) {
				cancelled = true
				break
			}
			release, admissionErr := admitCandidateAttempt(ctx, cand)
			if errors.Is(admissionErr, errAccountBusy) {
				busy = true
				break
			}
			if admissionErr != nil {
				return admissionFailureResult(res, admissionErr)
			}
			if release == nil {
				break
			}
			attempted = true
			res.Tried = append(res.Tried, cand.AuthID)
			emitted, status, err := func() (bool, int, error) {
				defer release()
				return attempt(cand)
			}()
			if err == nil {
				resetCandidateCooldown(cand)
				// A success proves the account+model pair is still serving, so any
				// accumulated empty-stream strikes are stale.
				resetEmptyStreamStrikesForModel(cand, requestedModel)
				return failoverResult{Outcome: outcomeSuccess, AuthID: cand.AuthID, Tried: res.Tried}
			}
			// Chunks already reached the client — the stream must end here.
			if emitted {
				return failoverResult{Outcome: outcomeStop, AuthID: cand.AuthID, Tried: res.Tried,
					Emitted: true, Status: status, Body: err.Error(), Err: err}
			}
			if isEmptyStreamError(err) {
				// Upstream answered 200 then closed before the first chunk. That is how
				// it reports "this account's quota for this model is spent today", but a
				// single occurrence is indistinguishable from a network reset, so the
				// verdict only lands after emptyStreamStrikeThreshold consecutive
				// strikes.
				//
				// Either way we move to the next account: another one may still have
				// quota. `break` rather than `continue` because the inner loop is an
				// unbounded same-account retry whose exit is gated on the rate-limit
				// path; re-entering it here would spin on one exhausted account.
				lastStatus, lastBody, lastErr = status, err.Error(), err
				// Record the real error on res so the loop tail does not rewrite it.
				// unavailableFailoverResult replaces the result with a 429 whenever
				// res.Err is nil, which would turn a definite failure into "all
				// accounts temporarily unavailable" — a misleading message, and one
				// that invites a retry of an already-failed request.
				//
				// res is written directly instead of going through `observed`
				// because that flag also drives markCandidateUnavailable, and an
				// account must NOT be quarantined before the strike threshold is met.
				res.Status, res.Body, res.Err = lastStatus, lastBody, lastErr
				res.AuthID = cand.AuthID
				if recordEmptyStreamStrike(cand.AuthID, requestedModel) {
					if cooldownErr := coolCandidateModel(cand, requestedModel, nextChinaMidnight(time.Now()), cooldownReasonEmptyStreamQuota, err.Error()); cooldownErr != nil {
						hostLogCooldownFailure(cand.AuthID, requestedModel, cooldownErr)
					}
					modelLimited = true
				}
				break
			}
			if isFirstTokenTimeout(err) {
				timeoutErr, _ := err.(*firstTokenTimeoutError)
				if cooldownErr := coolCandidateModel(cand, timeoutErr.model, timeoutErr.resetAt, cooldownReasonFirstTokenTimeout, err.Error()); cooldownErr != nil {
					hostLogCooldownFailure(cand.AuthID, timeoutErr.model, cooldownErr)
				}
				modelLimited = true
				lastStatus, lastBody, lastErr = status, err.Error(), err
				break // a daily-quota verdict is not retried on the same account
			}
			if isAccountExhaustedResponse(status, err.Error()) || upstreamCodeOf(err) == codeCreditsExhausted {
				observed = true
				lastOutcome = outcomeAccountExhausted
				lastStatus, lastBody, lastErr = status, err.Error(), err
				break
			}
			if isRateLimitResponse(status, err.Error()) || upstreamCodeOf(err) == codeFrequencyLimit {
				observed = true
				lastOutcome = outcomeRateLimited
				lastStatus, lastBody, lastErr = status, err.Error(), err
				if attemptIdx >= extra {
					break
				}
				continue
			}
			return failoverResult{Outcome: outcomeStop, AuthID: cand.AuthID, Tried: res.Tried,
				Status: status, Body: err.Error(), Err: err}
		}
		if attempted {
			attemptedCount++
		}
		if observed {
			markCandidateUnavailable(cand, lastOutcome, lastBody)
			res.Outcome, res.AuthID = lastOutcome, cand.AuthID
			res.Status, res.Body, res.Err = lastStatus, lastBody, lastErr
		}
		if cancelled {
			return unavailableFailoverResult(res)
		}
		if modelLimited {
			modelLimitedCount++
			res.Outcome = outcomeModelCooldown
			res.Status, res.Body, res.Err = lastStatus, lastBody, lastErr
			continue
		}
	}
	if attemptedCount > 0 && modelLimitedCount == attemptedCount && !busy {
		return res // every attempt hit the first-token watchdog
	}
	if res.Outcome == outcomeModelCooldown {
		// The model-cooldown verdict is not unanimous (busy/rate-limited peers
		// exist) — let the caller emit the generic upstream error instead.
		res.Outcome = outcomeStop
	}
	return unavailableFailoverResult(res, busy)
}

func admissionFailureResult(res failoverResult, err error) failoverResult {
	status, body := upstreamStatusOf(err)
	outcome := outcomeStop
	if status == 429 {
		outcome = outcomeRateLimited
	} // local overload: no lifecycle/account mutation
	if body == "" {
		body = err.Error()
	}
	return failoverResult{Outcome: outcome, AuthID: res.AuthID, Tried: res.Tried, Status: status, Body: body, Err: err}
}

func unavailableFailoverResult(res failoverResult, busy ...bool) failoverResult {
	if res.Err == nil {
		res.Outcome = outcomeRateLimited
		res.Status, res.Body = 429, "all eligible workbuddy accounts are temporarily unavailable"
		if len(busy) > 0 && busy[0] {
			res.Body = "all eligible workbuddy accounts are busy; retry later"
		}
		res.Err = &upstreamStatusError{status: res.Status, body: res.Body}
	}
	return res
}

// markCandidateUnavailable quarantines the candidate after a soft rate limit
// (model-scoped: the account keeps serving other models) or an explicit credit
// rejection (account-wide, with the long default backoff). Soft limits are
// capped at maxRateLimitCooldown so one burst never freezes a model for the
// full default window. Never disables/deletes an auth file; a replenished
// account is simply reachable again once the entry expires.
func markCandidateUnavailable(c *failoverCandidate, outcome attemptOutcome, body string) {
	if c == nil {
		return
	}
	reason := "429"
	if outcome == outcomeAccountExhausted {
		reason = "credits_exhausted"
	}
	resetAt := parseRateLimitResetAt(body)
	if resetAt.IsZero() || !resetAt.After(time.Now()) {
		resetAt = time.Now().Add(defaultCooldownDuration)
	}
	if reason == "429" {
		if capAt := time.Now().Add(maxRateLimitCooldown); resetAt.After(capAt) {
			resetAt = capAt
		}
	}
	for _, key := range candidateCooldownIDs(c) {
		if reason == "429" && c.RateModel != "" {
			markModelCooldown(key, c.RateModel, resetAt, reason)
			continue
		}
		markAuthCooldownReason(key, resetAt, reason)
	}
	fmt.Fprintf(os.Stderr, "[workbuddy] failover: skip auth=%q model=%q reason=%s until=%s\n", redactSecrets(c.AuthID), redactSecrets(c.RateModel), reason, resetAt.UTC().Format(time.RFC3339))
}

// failureIdentity maps terminal accounting and reconciliation to the last
// attempted candidate, not the account originally chosen by the host.
func failureIdentity(cands []*failoverCandidate, fin failoverResult, routedID, routedUID string) (string, string) {
	id := firstNonEmpty(fin.AuthID, routedID)
	return id, candUID(findCandidate(cands, id), routedUID)
}

// resetCandidateCooldown clears the quarantine a successful attempt proves
// stale. When the candidate carries a RateModel only that model's key is
// cleared: a success on model B must not clear model A's quarantine or a
// concurrently observed account-wide credit rejection.
func resetCandidateCooldown(c *failoverCandidate) {
	for _, key := range candidateCooldownIDs(c) {
		if c.RateModel == "" {
			resetAuthCooldown(key) // legacy/test callers
			continue
		}
		resetModelCooldown(key, c.RateModel)
	}
}

func candidateCooldownIDs(c *failoverCandidate) []string {
	if c == nil {
		return nil
	}
	keys := make([]string, 0, 3+len(c.Aliases))
	identities := []string{c.AuthID, c.RecordID}
	if c.Auth != nil {
		identities = append(identities, c.Auth.Account.UID)
	}
	identities = append(identities, c.Aliases...)
	seen := make(map[string]struct{}, len(identities))
	for _, key := range identities {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

func candidateOnCooldown(c *failoverCandidate) bool {
	return !candidateCooldownState(c).NextRetryAt.IsZero()
}

// candUID returns the account's UID for usage records; falls back to the
// routed account's UID when the candidate carries no parsed auth.
func candUID(c *failoverCandidate, fallback string) string {
	if c != nil && c.Auth != nil && strings.TrimSpace(c.Auth.Account.UID) != "" {
		return c.Auth.Account.UID
	}
	return fallback
}

// findCandidate returns the pool candidate matching authID.
func findCandidate(cands []*failoverCandidate, authID string) *failoverCandidate {
	for _, c := range cands {
		if c != nil && c.AuthID == authID {
			return c
		}
	}
	return nil
}
