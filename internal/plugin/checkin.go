// checkin.go implements daily check-in for CN accounts: the manual
// handleManualCheckin endpoint, the 09:00 / 21:00 auto scheduler, and the
// per-account mutex that prevents duplicate check-ins from racing browser
// tabs. Global accounts are excluded — they use one-shot trial claims instead.
package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/Evsdrg/cepvor-cpa-plinth/logging"
	"github.com/Evsdrg/cepvor-cpa-plinth/schedule"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

var (
	schedulerStop chan struct{}
	schedulerMu   sync.Mutex
)

// schedulerState 记录三类任务的每日结算与失败重试状态。
//
// 原调度器把槽位当成「只触发一次的事件」：进程在槽位之后启动（部署、重启）
// 只会排到明天，当天那个账号静默漏签；某个账号耗时越过后续账号的槽位时，
// 重算也会把后面的人推到明天。这里改成「每账号每任务每天至少成功一次」。
type schedulerState struct {
	mu       sync.Mutex
	dayKey   string
	settled  map[string]string    // key = task|account -> 本地日期
	attempts map[string]int       // key = task|account -> 当天失败次数
	retryAt  map[string]time.Time // key = task|account -> 下次重试时刻
}

var scheduler = &schedulerState{}

// taskAccountKey 是结算状态的键。任务与账号一起命名，因为同一个账号的
// 签到、保活、奖励是三个独立目标：签到成功不代表保活也成功。
func taskAccountKey(task scheduledTask, accountName string) string {
	return fmt.Sprintf("%d|%s", task, accountName)
}

// retryDelays 是三类任务共用的失败退避：5 / 15 / 45 分钟，最多三次。
var retryDelays = []time.Duration{5 * time.Minute, 15 * time.Minute, 45 * time.Minute}

// resetDayIfNeeded 在跨天时清空结算与重试状态。
func (s *schedulerState) resetDayIfNeeded(now time.Time) {
	day := now.Format("2006-01-02")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dayKey == day {
		return
	}
	s.dayKey = day
	s.settled = map[string]string{}
	s.attempts = map[string]int{}
	s.retryAt = map[string]time.Time{}
}

// settledToday 报告该任务账号今天是否已结算。
func (s *schedulerState) settledToday(task scheduledTask, accountName string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settled[taskAccountKey(task, accountName)] == now.Format("2006-01-02")
}

// markSettled 记录该任务账号今天已完成（成功或终态）。
func (s *schedulerState) markSettled(task scheduledTask, accountName string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.settled == nil {
		s.settled = map[string]string{}
	}
	key := taskAccountKey(task, accountName)
	s.settled[key] = now.Format("2006-01-02")
	delete(s.attempts, key)
	delete(s.retryAt, key)
}

// scheduleRetry 为失败的任务账号排下一次重试，返回 false 表示额度已耗尽。
func (s *schedulerState) scheduleRetry(task scheduledTask, accountName string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempts == nil {
		s.attempts = map[string]int{}
	}
	if s.retryAt == nil {
		s.retryAt = map[string]time.Time{}
	}
	key := taskAccountKey(task, accountName)
	attempt := s.attempts[key]
	if attempt >= len(retryDelays) {
		delete(s.retryAt, key)
		return false
	}
	s.attempts[key] = attempt + 1
	s.retryAt[key] = now.Add(retryDelays[attempt])
	return true
}

// retryDue 返回该任务账号的下次重试时刻；没有排重试时返回零值。
func (s *schedulerState) retryDue(task scheduledTask, accountName string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retryAt[taskAccountKey(task, accountName)]
}

// retryBudgetSpent 报告该任务账号今天的重试额度是否已用完。
func (s *schedulerState) retryBudgetSpent(task scheduledTask, accountName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts[taskAccountKey(task, accountName)] >= len(retryDelays)
}

// ensureSchedulerSource starts the periodic task loop. Tests replace it with a
// no-op: the real loop calls the host for the account list on its first tick,
// which in a test without a host blocks for the transport timeout.
var ensureSchedulerSource = ensureScheduler

func ensureScheduler() {
	schedulerMu.Lock()
	defer schedulerMu.Unlock()
	if schedulerStop != nil {
		return // already running
	}
	schedulerStop = make(chan struct{})
	go schedulerLoop(schedulerStop)
}

// Note: there is deliberately no stopCheckinScheduler. The plugin shutdown
// export is a no-op (see cliproxyPluginShutdown) because the host invokes it
// during its own runtime teardown, where touching Go sync primitives from the
// plugin's c-shared runtime caused SIGSEGV on every restart.

// scheduledTask 是一次唤醒要执行的任务种类。
type scheduledTask int

const (
	// taskCheckin 是每日签到。
	taskCheckin scheduledTask = iota
	// taskKeepalive 是 token 保活。
	taskKeepalive
	// taskDailyBonus 是 Global 每日奖励。
	taskDailyBonus
)

// scheduleHours 返回某类任务配置的小时表。
//
// 签到表必须走 configuredCheckinHours：configure() 在重配置时整份替换
// checkinHours，而本函数在 schedulerLoop 的 goroutine 里被调用。直接读裸变量
// 与那段写入构成数据竞争（go test -race 可复现），也会读到写了一半的切片头。
// 另外两张表是常量，生命周期内不会变。
func scheduleHours(task scheduledTask) []int {
	switch task {
	case taskKeepalive:
		return keepaliveHours
	case taskDailyBonus:
		return dailyBonusHours
	default:
		return configuredCheckinHours()
	}
}

// dueSlots 计算此刻已经到点、还没结算的（账号, 任务），按时刻排序。
//
// 每个任务用 schedule.DaySlots 取当天全部槽位（含已经过去的），这样进程在
// 槽位之后启动也能补做，而不是等到明天。失败过的账号按 5/15/45 分钟重试。
// 抖动按（账号名, 日期, 小时）派生，所以同一时刻不会有多账号一起打上游。
func dueSlots(now time.Time, accountNames []string) []slotTask {
	scheduler.resetDayIfNeeded(now)
	out := make([]slotTask, 0, len(accountNames)*3)
	for _, task := range []scheduledTask{taskCheckin, taskKeepalive, taskDailyBonus} {
		hours := scheduleHours(task)
		if len(hours) == 0 {
			continue
		}
		if ok := taskEnabled(task); !ok {
			continue
		}
		slots := schedule.DaySlots(now, accountNames, hours)
		// 每个 (task, account) 只取最早一个未结算槽位。当天多个时段都过去时
		// 不该把同一个账号连打多次——签到/保活/奖励本身幂等，但重复调用只是
		// 白耗上游配额。补做只需要一次。
		seen := map[string]bool{}
		for _, slot := range slots {
			key := taskAccountKey(task, slot.AccountName)
			if seen[key] {
				continue
			}
			if scheduler.settledToday(task, slot.AccountName, now) {
				continue
			}
			if retry := scheduler.retryDue(task, slot.AccountName); !retry.IsZero() {
				// 有排定的重试：只在重试时刻到点时执行，不要用常规槽位再打一次。
				if retry.After(now) {
					continue
				}
				seen[key] = true
				out = append(out, slotTask{Slot: slot, Task: task})
				continue
			}
			if scheduler.retryBudgetSpent(task, slot.AccountName) {
				continue
			}
			if slot.At.After(now) {
				continue
			}
			seen[key] = true
			out = append(out, slotTask{Slot: slot, Task: task})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Slot.At.Equal(out[j].Slot.At) {
			return out[i].Slot.AccountName < out[j].Slot.AccountName
		}
		return out[i].Slot.At.Before(out[j].Slot.At)
	})
	return out
}

// nextFutureSlot 返回下一个未来的（账号, 任务）唤醒点，用于睡眠。
func nextFutureSlot(now time.Time, accountNames []string) (slotTask, bool) {
	var best slotTask
	found := false
	for _, task := range []scheduledTask{taskCheckin, taskKeepalive, taskDailyBonus} {
		hours := scheduleHours(task)
		if len(hours) == 0 || !taskEnabled(task) {
			continue
		}
		// 有未到点的重试时，那个时刻才是最近的唤醒点。
		for _, name := range accountNames {
			if retry := scheduler.retryDue(task, name); !retry.IsZero() && retry.After(now) {
				candidate := slotTask{Slot: schedule.AccountSlot{AccountName: name, At: retry}, Task: task}
				if !found || candidate.Slot.At.Before(best.Slot.At) {
					best, found = candidate, true
				}
			}
		}
		// 先看今天的未来槽位，再看明天的（跨天结算状态会重置，明天的槽位
		// 一律视为可排）。两类都参与全局比较：不能一发现今天的槽位就停，
		// 否则更早的明天槽位（比如每日奖励 08:00 早于签到 09:00）会被漏掉。
		for _, slot := range schedule.DaySlots(now, accountNames, hours) {
			if scheduler.settledToday(task, slot.AccountName, now) {
				continue
			}
			if !scheduler.retryDue(task, slot.AccountName).IsZero() {
				continue
			}
			if scheduler.retryBudgetSpent(task, slot.AccountName) {
				continue
			}
			if slot.At.After(now) {
				if !found || slot.At.Before(best.Slot.At) {
					best, found = slotTask{Slot: slot, Task: task}, true
				}
			}
		}
		for _, slot := range schedule.DaySlots(now.Add(24*time.Hour), accountNames, hours) {
			if !found || slot.At.Before(best.Slot.At) {
				best, found = slotTask{Slot: slot, Task: task}, true
			}
		}
	}
	return best, found
}

// slotTask 是一次待执行的（账号, 任务）。
type slotTask struct {
	Slot schedule.AccountSlot
	Task scheduledTask
}

// taskEnabled 报告该任务是否在配置中启用。
func taskEnabled(task scheduledTask) bool {
	switch task {
	case taskKeepalive:
		return keepaliveEnabled()
	case taskDailyBonus:
		return true
	default:
		checkinAutoMu.RLock()
		defer checkinAutoMu.RUnlock()
		return checkinAuto
	}
}

func schedulerLoop(stop chan struct{}) {
	for {
		now := time.Now()
		names := accountNamesForSchedule()
		if len(names) == 0 {
			// 还没有账号：等一会儿再看，不要退出循环——账号可能在运行期被加入。
			//
			// 启动竞态也落在这里：插件可能在 plugin.register 之前就开始跑循环，
			// 宿主到注册之后才把 auth 文件变成可用的运行时凭据。等一会儿再取。
			timer := time.NewTimer(5 * time.Second)
			select {
			case <-stop:
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}

		// 先执行所有已到点、未结算的任务。名单在开始前固定：某个账号耗时
		// 再长，也不会把后面的账号挤出今天的名单。
		due := dueSlots(now, names)
		for _, item := range due {
			select {
			case <-stop:
				return
			default:
			}
			runScheduledTask(item.Task, item.Slot.AccountName)
		}
		// 一轮跑完重新取时间与名单（执行期间可能有账号加入或跨天）。
		now = time.Now()
		names = accountNamesForSchedule()
		if len(names) == 0 {
			continue
		}
		next, ok := nextFutureSlot(now, names)
		if !ok {
			// 三类任务今天都结算完了，或者都没有配置小时：睡到明天第一个槽位。
			timer := time.NewTimer(untilTomorrow(now, names))
			select {
			case <-stop:
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		timer := time.NewTimer(time.Until(next.Slot.At))
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// untilTomorrow 返回距离明天第一个槽位的时长。
func untilTomorrow(now time.Time, names []string) time.Duration {
	tomorrow := now.Add(24 * time.Hour)
	best := time.Time{}
	for _, task := range []scheduledTask{taskCheckin, taskKeepalive, taskDailyBonus} {
		hours := scheduleHours(task)
		if len(hours) == 0 || !taskEnabled(task) {
			continue
		}
		for _, slot := range schedule.DaySlots(tomorrow, names, hours) {
			if best.IsZero() || slot.At.Before(best) {
				best = slot.At
			}
		}
	}
	if best.IsZero() {
		return time.Hour
	}
	if d := time.Until(best); d > 0 {
		return d
	}
	return time.Hour
}

// runScheduledTask 执行一次唤醒对应的任务，并把结果回报给调度器。
//
// 只处理被唤醒的那个账号，而不是全量：全量会让所有账号在同一秒打上游，
// 正是抖动要消除的脚本特征。签到/保活/奖励三个操作本身按账号幂等，所以
// 单个账号失败不会影响其他账号的下一次唤醒。
//
// 返回值只有一个用途：决定这个（账号, 任务）今天是否已经结算、失败要不要
// 排重试。没有这个回报，一次 5xx 会被当成完成，账号当天就静默空窗。
func runScheduledTask(task scheduledTask, accountName string) {
	now := time.Now()
	ok := true
	reason := ""
	switch task {
	case taskKeepalive:
		ok, reason = runTokenKeepaliveForAccount(accountName)
	case taskDailyBonus:
		ok, reason = runDailyBonusForAccount(accountName)
	default:
		ok, reason = runAutoCheckinForAccount(accountName)
	}
	if ok {
		scheduler.markSettled(task, accountName, now)
		logging.Info(&abi.Host{}, "Workbuddy.scheduler：定时任务完成",
			logging.Auth(accountName), logging.Operation(taskName(task)), logging.State("ok"))
		return
	}
	if scheduler.scheduleRetry(task, accountName, now) {
		logging.Warn(&abi.Host{}, "Workbuddy.scheduler：定时任务失败，稍后重试 "+reason,
			logging.Auth(accountName), logging.Operation(taskName(task)), logging.State("retrying"))
		return
	}
	// 重试额度用完：记为今日已结算，避免继续空转；错误保留在日志里。
	scheduler.markSettled(task, accountName, now)
	logging.Error(&abi.Host{}, "Workbuddy.scheduler：定时任务失败，今日重试额度已用尽 "+reason,
		logging.Auth(accountName), logging.Operation(taskName(task)), logging.State("failed"))
}

// taskName 把任务枚举转成日志里的可读名字。
func taskName(task scheduledTask) string {
	switch task {
	case taskKeepalive:
		return "token-keepalive"
	case taskDailyBonus:
		return "daily-bonus"
	default:
		return "checkin"
	}
}

// runAutoCheckinForAccount 签到单个账号（按文件名匹配）。
func runAutoCheckinForAccount(accountName string) (bool, string) {
	files, err := hostAuthList()
	if err != nil {
		return false, "列出凭据失败: " + truncateRedacted(err.Error(), 120)
	}
	checkinAutoMu.RLock()
	doCheckin := checkinAuto
	checkinAutoMu.RUnlock()
	for _, f := range files {
		name := strings.TrimSpace(f.Name)
		if name == "" {
			name = strings.TrimSpace(f.ID)
		}
		if name != accountName {
			continue
		}
		return processAutoCheckinAccount(f, doCheckin)
	}
	return true, "" // 账号已不存在：视为终态，不重试
}

// runTokenKeepaliveForAccount 续期单个账号。
//
// 复用 refreshOneAuth 而不是 runTokenKeepalive：后者会遍历全部账号，正是抖动
// 要避免的「同一秒全量打上游」。汇总仍写进 lastKeepalive，保持面板可见。
func runTokenKeepaliveForAccount(accountName string) (bool, string) {
	if !keepaliveEnabled() {
		return true, ""
	}
	files, err := hostAuthList()
	if err != nil {
		return false, "列出凭据失败: " + truncateRedacted(err.Error(), 120)
	}
	for _, f := range files {
		name := strings.TrimSpace(f.Name)
		if name == "" {
			name = strings.TrimSpace(f.ID)
		}
		if name != accountName {
			continue
		}
		row := keepaliveRow{AuthIndex: f.AuthIndex}
		if sa, err := hostAuthGet(f.AuthIndex); err == nil {
			row.Nickname = sa.Account.Nickname
			row.Region = accountRegion(sa)
		}
		status, err := refreshOneAuth(f.AuthIndex, f.ID)
		row.Status = status
		if err != nil {
			row.Detail = truncateRedacted(err.Error(), 200)
		}
		recordKeepalive(&keepaliveSummary{When: time.Now(), Results: []keepaliveRow{row}})
		switch status {
		case "refreshed", "skipped":
			return true, ""
		case "session-dead":
			// 刷新链已死，重试也不会成功。
			return true, ""
		default:
			if err != nil {
				return false, truncateRedacted(err.Error(), 160)
			}
			return false, status
		}
	}
	return true, ""
}

// runDailyBonusForAccount 领取单个账号的每日奖励。
func runDailyBonusForAccount(accountName string) (bool, string) {
	files, err := hostAuthList()
	if err != nil {
		return false, "列出凭据失败: " + truncateRedacted(err.Error(), 120)
	}
	for _, f := range files {
		name := strings.TrimSpace(f.Name)
		if name == "" {
			name = strings.TrimSpace(f.ID)
		}
		if name != accountName {
			continue
		}
		row := runDailyBonusOne(f.AuthIndex, dailyBonusRow{AuthIndex: f.AuthIndex})
		recordDailyBonus(&dailyBonusSummary{When: time.Now(), Results: []dailyBonusRow{row}})
		switch row.Status {
		case "reported", "already-lit", "skipped":
			return true, ""
		default:
			return false, row.Detail
		}
	}
	return true, ""
}

// accountNamesForSchedule 返回参与调度的账号名。
// 用文件名而不是运行期索引：前者跨重启稳定，后者会随加载顺序变化。
//
// 返回空名单同时表示「还没有账号」和「宿主还没分配运行时索引」：插件可能在
// plugin.register 之前就开始跑循环，而宿主到注册之后才把 auth 文件变成带
// AuthIndex 的运行时凭据。两种情况都该等一会儿，而不是拿空索引去请求上游。
func accountNamesForSchedule() []string {
	files, err := hostAuthList()
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		// 已禁用账号不参与调度：它们是操作员显式关掉的，定时器还去叫醒只会
		// 制造失败日志，并在面板上把「已禁用」表现成「签到失败」。
		if f.Disabled {
			continue
		}
		if strings.TrimSpace(f.AuthIndex) == "" {
			// 启动竞态：宿主还没分配运行时索引。整个名单视为未就绪，
			// 让调度循环等下一轮，不要把这个状态记成任务失败。
			return nil
		}
		name := strings.TrimSpace(f.Name)
		if name == "" {
			name = strings.TrimSpace(f.ID)
		}
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	return names
}

// runAutoCheckin is the scheduled lifecycle tick (09:00 / 21:00).
// CN: optional daily check-in, then reconcile (disable exhausted / reenable after credits).
// Global: no auto trial (one-shot claim is manual only); reconcile may delete exhausted auths.
//
// v0.6.31: per-account work runs concurrently (sem=4) — was serial, so N accounts
// meant 3N serial HTTP round-trips on the billing API. Matches the pattern used
// by buildDashboardEx and handleManualCheckin.
func runAutoCheckin() {
	checkinAutoMu.RLock()
	doCheckin := checkinAuto
	checkinAutoMu.RUnlock()
	// Lifecycle may still run when check-in is off (credit gate).
	if !doCheckin && !lifecycleEnabled() {
		return
	}
	files, err := hostAuthList()
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for _, f := range files {
		f := f
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			processAutoCheckinAccount(f, doCheckin)
		}()
	}
	wg.Wait()
}

// processAutoCheckinAccount handles one account's scheduled tick. Extracted so
// runAutoCheckin can fan out per-account work without duplicating logic.
func processAutoCheckinAccount(f pluginapi.HostAuthFileEntry, doCheckin bool) (bool, string) {
	// A-24: only fetch sa when needed (checkin). For lifecycle-only paths,
	// let reconcileOneAccount do the single hostAuthGetBundle internally.
	if doCheckin {
		sa, err := hostAuthGet(f.AuthIndex)
		if err != nil {
			return false, "读取凭据失败: " + truncateRedacted(err.Error(), 120)
		}
		if isGlobalDomain(sa.Auth.Domain) {
			// Global: never check-in or auto-claim trial. Lifecycle only.
			// Invalidate cache (copy entry, set credits=nil, keep plan/checkin).
			if v, ok := accountCache.Load(f.ID); ok {
				if e, ok2 := v.(*accountCacheEntry); ok2 {
					fresh := *e
					fresh.credits = nil
					fresh.fetched = time.Now()
					accountCache.Store(f.ID, &fresh)
				}
			}
			if lifecycleEnabled() {
				_, _ = reconcileOneAccount(f.AuthIndex, f.ID, true)
			}
			return true, ""
		}
		// CN: daily check-in when enabled.
		ci, err := fetchCheckinStatus(sa)
		if err == nil && ci != nil && ci.Active && !ci.TodayCheckedIn {
			if _, callErr := performCheckinCall(sa); callErr == nil {
				// Refresh once after a successful checkin call so cache reflects
				// the post-call state. If the status call fails keep the pre-call
				// snapshot rather than dropping it (v0.6.31: avoid shadowing ci
				// with a second fetch that could race with concurrent readers).
				if ci2, _ := fetchCheckinStatus(sa); ci2 != nil {
					ci = ci2
				}
				// P1-5: checkin grants new credits — refresh the credits cache
				// immediately so the panel shows the updated balance without
				// waiting for the async reconcile pass.
				if cr2, crErr := fetchUserResource(sa); crErr == nil && cr2 != nil {
					if v, ok := accountCache.Load(f.ID); ok {
						if prev, ok2 := v.(*accountCacheEntry); ok2 {
							fresh := *prev
							fresh.credits = cr2
							fresh.fetched = time.Now()
							accountCache.Store(f.ID, &fresh)
						}
					}
				}
			}
		}
		checkinFailed := ""
		if err != nil {
			checkinFailed = truncateRedacted(err.Error(), 160)
		} else if ci != nil && ci.Active && !ci.TodayCheckedIn {
			checkinFailed = "签到未完成"
		}
		// Refresh cache with latest checkin status (merge, don't wipe credits/plan).
		if ci != nil {
			var prev *accountCacheEntry
			if v, ok := accountCache.Load(f.ID); ok {
				prev, _ = v.(*accountCacheEntry)
			}
			entry := &accountCacheEntry{checkin: ci, fetched: time.Now()}
			if prev != nil {
				entry.credits = prev.credits
				entry.plan = prev.plan
			}
			accountCache.Store(f.ID, entry)
		}
		if lifecycleEnabled() {
			_, _ = reconcileOneAccount(f.AuthIndex, f.ID, true)
		}
		if checkinFailed != "" {
			return false, checkinFailed
		}
		return true, ""
	}
	// Lifecycle-only (checkin off): reconcile handles its own get.
	if lifecycleEnabled() {
		_, _ = reconcileOneAccount(f.AuthIndex, f.ID, true)
	}
	return true, ""
}

// handleManualCheckin serves POST /checkin.
//
// Single-account mode (body.auth_index set): runs checkinOneAccount directly —
// one hostAuthGet + at most two upstream calls (status, then check-in). The
// old three-phase classify/execute/summarize pipeline ran the same framework
// for one account as for thirty (4 RPC + 5 HTTP + lock-held re-reads), which
// routinely blew past the 30s management API timeout: the check-in succeeded
// upstream but the response never reached the panel.
//
// Batch mode (empty auth_index): fans out checkinOneAccount per account
// (sem=4), preserving input order and the {results, summary} response shape
// the panel depends on.
func handleManualCheckin(req pluginapi.ManagementRequest) map[string]any {
	var body struct {
		AuthIndex string `json:"auth_index"`
	}
	_ = json.Unmarshal(req.Body, &body)
	authIndex := strings.TrimSpace(body.AuthIndex)
	// 宿主聚合签到用 ?all=1 驱动本路由，显式覆盖 body 里的 auth_index。
	if isAllAccountsQuery(req.Query) {
		authIndex = ""
	}
	single := authIndex != ""

	files, err := hostAuthList()
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	var targets []pluginapi.HostAuthFileEntry
	for _, f := range files {
		if !single || f.AuthIndex == authIndex {
			targets = append(targets, f)
		}
	}
	if len(targets) == 0 {
		return map[string]any{"error": "no matching account"}
	}

	results := make([]map[string]any, len(targets))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i, f := range targets {
		wg.Add(1)
		go func(i int, f pluginapi.HostAuthFileEntry) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = checkinOneAccount(f)
		}(i, f)
	}
	wg.Wait()

	// Summary counters (field names are a panel contract — panel.html
	// checkinAll reads success/already/fail/skipped_global/eligible).
	successN, failN, alreadyN, globalN, eligibleN := 0, 0, 0, 0, 0
	for _, out := range results {
		if out == nil {
			continue
		}
		if out["error"] != nil {
			failN++
			continue
		}
		reason, _ := out["reason"].(string)
		switch reason {
		case "already":
			alreadyN++
			continue
		case "global":
			globalN++
			continue
		}
		eligibleN++
		if out["success"] == true {
			successN++
		} else {
			failN++
		}
	}
	return map[string]any{
		"results": results,
		"summary": map[string]any{
			"total":          len(targets),
			"eligible":       eligibleN,
			"success":        successN,
			"already":        alreadyN,
			"skipped_global": globalN,
			"fail":           failN,
			"attempted":      eligibleN,
		},
	}
}

// checkinOneAccount performs the daily check-in for one account with the
// minimum number of round-trips:
//
//	hostAuthGet ×1 (RPC) → domain gate → fetchCheckinStatus ×1 (failure
//	tolerated, upstream is idempotent) → performCheckinCall ×1.
//
// No lock-held re-reads, no lifecycle reconcile (that is the scheduled
// runAutoCheckin path's job), no redundant status refetches. Cache updates
// merge into the previous entry so credits/plan survive.
func checkinOneAccount(f pluginapi.HostAuthFileEntry) map[string]any {
	out := map[string]any{"auth_index": f.AuthIndex}

	sa, err := hostAuthGet(f.AuthIndex)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	out["nickname"] = sa.Account.Nickname

	if isGlobalDomain(sa.Auth.Domain) {
		out["success"] = false
		out["skipped"] = true
		out["reason"] = "global"
		out["message"] = "国际版账号不支持签到，请使用领取专家加油包"
		return out
	}

	mu := checkinLockFor(f.AuthIndex)
	mu.Lock()
	defer mu.Unlock()

	// Status probe: a failure here is NOT fatal — the check-in call below is
	// idempotent upstream and its business message tells us "already" anyway.
	ci, ciErr := fetchCheckinStatus(sa)
	if ciErr == nil && ci != nil && ci.TodayCheckedIn {
		mergeCheckinCache(f.ID, ci)
		out["success"] = true
		out["skipped"] = true
		out["reason"] = "already"
		out["message"] = "already checked in today"
		return out
	}

	res, err := performCheckinCall(sa)
	if err != nil {
		out["error"] = err.Error()
		out["success"] = false
		return out
	}
	for k, v := range res {
		out[k] = v
	}
	// Business soft-fail ("already checked in" family) → done, not failure.
	if msg, _ := out["message"].(string); msg != "" && out["success"] == false {
		low := strings.ToLower(msg)
		if strings.Contains(low, "already") || strings.Contains(msg, "已签") || strings.Contains(msg, "今日") {
			out["success"] = true
			out["skipped"] = true
			out["reason"] = "already"
		}
	}
	if _, ok := out["success"]; !ok {
		out["success"] = true
	}

	// Post-call cache refresh: one extra status fetch at most; on failure
	// write a TodayCheckedIn placeholder rather than leaving the panel stale.
	if ci2, err2 := fetchCheckinStatus(sa); err2 == nil && ci2 != nil {
		mergeCheckinCache(f.ID, ci2)
	} else {
		mergeCheckinCache(f.ID, &checkinSummary{TodayCheckedIn: true})
	}
	return out
}

// mergeCheckinCache stores the latest check-in snapshot while preserving the
// credits/plan fields of any previous cache entry (merge, not replace).
func mergeCheckinCache(authID string, ci *checkinSummary) {
	var prev *accountCacheEntry
	if v, ok := accountCache.Load(authID); ok {
		prev, _ = v.(*accountCacheEntry)
	}
	entry := &accountCacheEntry{checkin: ci, fetched: time.Now()}
	if prev != nil {
		entry.credits = prev.credits
		entry.plan = prev.plan
	}
	accountCache.Store(authID, entry)
}

func checkinLockFor(authIndex string) *sync.Mutex {
	v, _ := checkinLocks.LoadOrStore(authIndex, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// pruneCheckinLocks removes lock entries for auth indices that no longer
// exist in hostAuthList. Call after dashboard prune.
// Lock keys are auth_index (used for host RPC), so live map needs auth_index too.
func pruneCheckinLocks() {
	files, err := hostAuthList()
	if err != nil {
		return
	}
	live := make(map[string]struct{}, len(files))
	for _, f := range files {
		live[f.ID] = struct{}{}
		live[f.AuthIndex] = struct{}{} // checkinLockFor uses auth_index as key
	}
	checkinLocks.Range(func(key, _ any) bool {
		idx, _ := key.(string)
		if _, ok := live[idx]; !ok {
			checkinLocks.Delete(key)
		}
		return true
	})
}

// isAllAccountsQuery reports whether the caller asked for every account with
// ?all=1 (or all=true). The host's aggregate check-in uses this form so it can
// drive any plugin's check-in route uniformly.
func isAllAccountsQuery(query url.Values) bool {
	if len(query) == 0 {
		return false
	}
	value := strings.ToLower(strings.TrimSpace(query.Get("all")))
	return value == "1" || value == "true"
}
