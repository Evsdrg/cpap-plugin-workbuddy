package main

import (
	"testing"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
)

// ensureTasksEnabledForTest 把三类定时任务都打开。
//
// 这批用例依赖「签到任务会被调度」，而 checkinAuto 是包级变量，会被其他
// 用例的 configure() 改写。不显式设置就会出现「单独跑通过、全量跑失败」。
func ensureTasksEnabledForTest(t *testing.T) {
	t.Helper()
	checkinAutoMu.Lock()
	previousCheckin := checkinAuto
	checkinAuto = true
	checkinAutoMu.Unlock()

	keepaliveAutoMu.Lock()
	previousKeepalive := keepaliveAuto
	keepaliveAuto = true
	keepaliveAutoMu.Unlock()

	t.Cleanup(func() {
		checkinAutoMu.Lock()
		checkinAuto = previousCheckin
		checkinAutoMu.Unlock()
		keepaliveAutoMu.Lock()
		keepaliveAuto = previousKeepalive
		keepaliveAutoMu.Unlock()
	})
}

// TestDueSlotsCatchesUpAfterStartupWorkbuddy 钉住「槽位过后启动仍补做」。
func TestDueSlotsCatchesUpAfterStartupWorkbuddy(t *testing.T) {
	ensureTasksEnabledForTest(t)
	scheduler.resetDayIfNeeded(time.Now())
	scheduler.mu.Lock()
	scheduler.settled = map[string]string{}
	scheduler.attempts = map[string]int{}
	scheduler.retryAt = map[string]time.Time{}
	scheduler.mu.Unlock()

	// 22:30，签到 9/21 都已过去。
	now := time.Date(2026, 7, 24, 22, 30, 0, 0, time.UTC)
	due := dueSlots(now, []string{"workbuddy-acct-1"})
	if len(due) == 0 {
		t.Fatal("no due slots: an account whose slots already passed must still be caught up")
	}
	foundCheckin := false
	for _, item := range due {
		if item.Task == taskCheckin {
			foundCheckin = true
			if item.Slot.At.After(now) {
				t.Errorf("checkin slot = %v, want a time no later than %v", item.Slot.At, now)
			}
		}
	}
	if !foundCheckin {
		t.Error("checkin task missing from the catch-up set")
	}
}

// TestDueSlotsSkipsSettledWorkbuddy 钉住已结算的任务账号不再排。
func TestDueSlotsSkipsSettledWorkbuddy(t *testing.T) {
	ensureTasksEnabledForTest(t)
	now := time.Date(2026, 7, 24, 22, 30, 0, 0, time.UTC)
	scheduler.resetDayIfNeeded(now)
	scheduler.markSettled(taskCheckin, "workbuddy-acct-1", now)

	due := dueSlots(now, []string{"workbuddy-acct-1"})
	for _, item := range due {
		if item.Task == taskCheckin {
			t.Fatal("settled checkin must not appear in the due set")
		}
	}
}

// TestSchedulerRetryBackoffWorkbuddy 钉住失败后的 5/15/45 分钟退避与额度上限。
func TestSchedulerRetryBackoffWorkbuddy(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	scheduler.resetDayIfNeeded(now)

	want := []time.Duration{5, 15, 45}
	for index, delay := range want {
		if !scheduler.scheduleRetry(taskCheckin, "acc", now) {
			t.Fatalf("retry %d must stay inside the budget", index+1)
		}
		if got := scheduler.retryDue(taskCheckin, "acc"); !got.Equal(now.Add(delay * time.Minute)) {
			t.Errorf("retry %d at %v, want %v", index+1, got, now.Add(delay*time.Minute))
		}
		// 清掉排期，下一轮 scheduleRetry 才能继续累加 attempts。
		scheduler.mu.Lock()
		delete(scheduler.retryAt, taskAccountKey(taskCheckin, "acc"))
		scheduler.mu.Unlock()
	}
	if scheduler.scheduleRetry(taskCheckin, "acc", now) {
		t.Fatal("fourth retry must exceed the budget")
	}
	if !scheduler.retryBudgetSpent(taskCheckin, "acc") {
		t.Error("retryBudgetSpent must report true once the budget is exhausted")
	}
}

// TestSchedulerDayRolloverWorkbuddy 钉住跨天重置。
func TestSchedulerDayRolloverWorkbuddy(t *testing.T) {
	day1 := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	scheduler.resetDayIfNeeded(day1)
	scheduler.markSettled(taskCheckin, "acc", day1)
	if !scheduler.settledToday(taskCheckin, "acc", day1) {
		t.Fatal("must report settled on the same day")
	}

	day2 := day1.Add(24 * time.Hour)
	scheduler.resetDayIfNeeded(day2)
	if scheduler.settledToday(taskCheckin, "acc", day2) {
		t.Fatal("settled state must reset on a new day")
	}
}

// TestDueSlotsDeduplicatesPerTaskAccountWorkbuddy 钉住同一账号同任务当天多个
// 已过时段只补一次。
func TestDueSlotsDeduplicatesPerTaskAccountWorkbuddy(t *testing.T) {
	ensureTasksEnabledForTest(t)
	now := time.Date(2026, 7, 24, 22, 30, 0, 0, time.UTC)
	scheduler.resetDayIfNeeded(now)

	due := dueSlots(now, []string{"workbuddy-acct-1"})
	counts := map[scheduledTask]int{}
	for _, item := range due {
		counts[item.Task]++
	}
	if counts[taskCheckin] > 1 {
		t.Errorf("checkin appeared %d times, want at most 1 per account per day", counts[taskCheckin])
	}
}

// TestAccountNamesWaitsForRuntimeIndexWorkbuddy 钉住启动竞态门控。
//
// 宿主在 plugin.register 之后才把 auth 文件变成带 AuthIndex 的运行时凭据。
// 在索引就绪前调度循环必须等待，而不是拿空索引去请求上游并把结果记成失败。
func TestAccountNamesWaitsForRuntimeIndexWorkbuddy(t *testing.T) {
	abi.SetHostCaller(func(method string, request []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostAuthList:
			return abi.OK(map[string]any{"files": []map[string]any{{
				"name":     "workbuddy-test.json",
				"provider": providerName,
				"type":     providerName,
				"status":   "active",
				// auth_index 故意留空：模拟 plugin.register 之前的启动阶段
			}}})
		}
		return abi.OK(map[string]any{})
	})
	defer abi.SetHostCaller(nil)

	if names := accountNamesForSchedule(); len(names) != 0 {
		t.Fatalf("accountNamesForSchedule = %v, want empty while AuthIndex is missing", names)
	}
}
