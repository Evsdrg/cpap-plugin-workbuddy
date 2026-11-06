package main

import (
	"testing"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/schedule"
)

// 这几个测试钉住 workbuddy 调度对抖动的依赖。抖动本身在 SDK 的 schedule 包
// 有完整测试；这里只验证调度循环确实用了它、且多账号不会同一秒打上游。

// 多账号在同一配置小时上必须落在不同时刻。
func TestWorkbuddyScheduleSpreadsAccounts(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	now := time.Date(2026, 10, 19, 0, 0, 0, 0, loc)
	names := []string{"wb-acct-1", "wb-acct-2", "wb-acct-3", "wb-acct-4", "wb-acct-5", "wb-acct-6"}
	slots := schedule.NextSlots(now, names, checkinHours)
	seen := map[time.Time]bool{}
	for _, slot := range slots {
		if seen[slot.At] {
			t.Fatalf("两个账号被安排在同一时刻 %v", slot.At)
		}
		seen[slot.At] = true
	}
}

// 唤醒时刻不能落在精确整点上——精确整点是脚本特征，也是把抖动删掉后最直观的
// 症状。这条断言比「落在 ±10 分钟内」更有区分力：零抖动实现会立刻失败。
func TestWorkbuddyScheduleNeverFiresOnTheExactHour(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	now := time.Date(2026, 10, 19, 0, 0, 0, 0, loc)
	names := []string{"wb-acct-1", "wb-acct-2", "wb-acct-3", "wb-acct-4"}
	exact := 0
	for _, slot := range schedule.NextSlots(now, names, []int{9, 21}) {
		if slot.At.Minute() == 0 && slot.At.Second() == 0 {
			exact++
		}
	}
	if exact > 0 {
		t.Fatalf("%d/%d 个槽位落在精确整点，抖动没有生效", exact, len(names)*2)
	}
}

// 同一账号同一天重复计算结果不变（进程重启不会重复签到）。
func TestWorkbuddyScheduleIsStablePerAccountPerDay(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	now := time.Date(2026, 10, 19, 8, 0, 0, 0, loc)
	first, ok := schedule.NextAt(now, []string{"wb-acct-1"}, checkinHours)
	if !ok {
		t.Fatal("没有算出槽位")
	}
	for i := 0; i < 20; i++ {
		again, _ := schedule.NextAt(now, []string{"wb-acct-1"}, checkinHours)
		if !again.At.Equal(first.At) {
			t.Fatalf("第 %d 次重算得到 %v，首次是 %v", i, again.At, first.At)
		}
	}
}
