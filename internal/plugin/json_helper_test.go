package main

import (
	"testing"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/schedule"
)

func TestJsonBool(t *testing.T) {
	m := map[string]any{"a": true, "b": float64(1), "c": "true", "d": "false", "e": "1", "f": float64(0)}
	if !jsonBool(m, "a") {
		t.Error("a")
	}
	if !jsonBool(m, "b") {
		t.Error("b")
	}
	if !jsonBool(m, "c") {
		t.Error("c")
	}
	if !jsonBool(m, "e") {
		t.Error("e")
	}
	if jsonBool(m, "d") {
		t.Error("d")
	}
	if jsonBool(m, "f") {
		t.Error("f")
	}
	if jsonBool(m, "missing") {
		t.Error("missing")
	}
	if !jsonBool(m, "missing", "a") {
		t.Error("fallback a")
	}
}
func TestJsonI64(t *testing.T) {
	m := map[string]any{"a": float64(42), "b": int64(7), "c": "99", "d": "abc"}
	if jsonI64(m, "a") != 42 {
		t.Error("a")
	}
	if jsonI64(m, "b") != 7 {
		t.Error("b")
	}
	if jsonI64(m, "c") != 99 {
		t.Error("c")
	}
	if jsonI64(m, "d") != 0 {
		t.Error("d")
	}
	if jsonI64(m, "missing") != 0 {
		t.Error("missing")
	}
}
func TestJsonStr(t *testing.T) {
	m := map[string]any{"a": "hello", "b": 42, "c": true}
	if jsonStr(m, "a") != "hello" {
		t.Error("a")
	}
	if jsonStr(m, "b") != "" {
		t.Error("b")
	}
	if jsonStr(m, "c") != "" {
		t.Error("c")
	}
	if jsonStr(m, "missing") != "" {
		t.Error("missing")
	}
	if jsonStr(m, "missing", "a") != "hello" {
		t.Error("fallback")
	}
}

// nextTaskSlot 是调度循环的唤醒计算，必须覆盖三类计划：签到（09:00/21:00）、
// token 保活（22:00）、Global 每日奖励（08:00/12:00/16:00/20:00）。
//
// 断言用「目标时刻 ± 抖动窗口」而不是精确小时：签到已改为按账号带 ±10 分钟
// 抖动，所以 09:00 的槽位可能落在 08:50 或 09:10，跨过小时边界是正常的。
func TestNextTaskSlot(t *testing.T) {
	names := []string{"workbuddy-acct-1"}
	window := schedule.JitterWindow

	assertNear := func(label string, got time.Time, want time.Time) {
		t.Helper()
		if d := got.Sub(want); d < -window || d > window {
			t.Fatalf("%s: got %v, want %v ± %v (off by %v)", label, got, want, window, d)
		}
	}

	// 08:00 → 下一个签到槽 09:00。
	morning := time.Date(2026, 7, 24, 8, 0, 0, 0, time.UTC)
	next, ok := nextFutureSlot(morning, names)
	if !ok {
		t.Fatal("no slot computed")
	}
	if next.Task != taskCheckin {
		t.Fatalf("08:00 → task %v, want checkin", next.Task)
	}
	assertNear("08:00", next.Slot.At, time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC))

	// 10:00 → 下一个是 12:00 的每日奖励槽。
	noon := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	next, ok = nextFutureSlot(noon, names)
	if !ok {
		t.Fatal("no slot computed")
	}
	if next.Task != taskDailyBonus {
		t.Fatalf("10:00 → task %v, want daily bonus", next.Task)
	}
	assertNear("10:00", next.Slot.At, time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC))

	// 22:30 → 所有槽位都已过去，次日最早是 08:00 的每日奖励。
	late := time.Date(2026, 7, 24, 22, 30, 0, 0, time.UTC)
	next, ok = nextFutureSlot(late, names)
	if !ok {
		t.Fatal("no slot computed")
	}
	if next.Task != taskDailyBonus {
		t.Fatalf("22:30 → task %v, want daily bonus", next.Task)
	}
	assertNear("22:30", next.Slot.At, time.Date(2026, 7, 25, 8, 0, 0, 0, time.UTC))
}
