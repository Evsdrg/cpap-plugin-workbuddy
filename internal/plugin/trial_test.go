package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestPerformTrialCall_Success simulates a successful trial claim.
func TestPerformTrialCall_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/billing/ide/trial" {
			t.Errorf("expected /billing/ide/trial, got %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"msg":"OK","data":{"trial":true,"credits":250}}`))
	}))
	defer srv.Close()

	restore := setBillingBaseGlobal(srv.URL)
	defer restore()

	sa := &storedAuth{Auth: storedTokens{Domain: "www.workbuddy.ai"}}
	res, err := performTrialCall(sa)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res["success"] != true {
		t.Errorf("expected success=true, got %v", res["success"])
	}
}

// TestPerformTrialCall_AlreadyClaimed simulates the code=14051 "has applied trial" response.
func TestPerformTrialCall_AlreadyClaimed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":14051,"msg":"has applied trial","data":null}`))
	}))
	defer srv.Close()

	restore := setBillingBaseGlobal(srv.URL)
	defer restore()

	sa := &storedAuth{Auth: storedTokens{Domain: "www.workbuddy.ai"}}
	res, err := performTrialCall(sa)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res["success"] != false {
		t.Errorf("expected success=false, got %v", res["success"])
	}
	if res["already_claimed"] != true {
		t.Errorf("expected already_claimed=true, got %v", res["already_claimed"])
	}
	if msg, ok := res["message"].(string); !ok || !strings.Contains(msg, "已领取") {
		t.Errorf("expected message to contain 已领取, got %v", res["message"])
	}
}

// TestHasTrialPack_Found tests detection of the trial pack by name.
func TestHasTrialPack_Found(t *testing.T) {
	cr := &creditsSummary{
		Packages: []packageSummary{
			{Name: "CodeBuddy One-time Free 2-Week Pro Plan Trial", Remain: 250, Used: 0},
		},
	}
	if !hasTrialPack(cr) {
		t.Error("expected hasTrialPack=true for trial package")
	}
}

// TestHasTrialPack_NotFound tests that non-trial packages return false.
func TestHasTrialPack_NotFound(t *testing.T) {
	cr := &creditsSummary{
		Packages: []packageSummary{
			{Name: "CodeBuddy Free Monthly Pack", Remain: 500, Used: 0},
			{Name: "裂变奖励包A", Remain: 99, Used: 1},
		},
	}
	if hasTrialPack(cr) {
		t.Error("expected hasTrialPack=false for non-trial packages")
	}
}

// TestHasTrialPack_CNPersonalExperienceNotTrial locks the live CN free-tier
// package name "CodeBuddy个人体验版" / bare "体验版". Matching bare "体验" would
// falsely mark every CN account as trial_claimed (A-18).
func TestHasTrialPack_CNPersonalExperienceNotTrial(t *testing.T) {
	cases := []string{
		"CodeBuddy个人体验版",
		"体验版",
		"CodeBuddy个人版国内运营裂变包",
	}
	for _, name := range cases {
		cr := &creditsSummary{Packages: []packageSummary{{Name: name, Remain: 500, Used: 0}}}
		if hasTrialPack(cr) {
			t.Errorf("hasTrialPack(%q)=true, want false (CN free tier / fission, not Global expert trial)", name)
		}
	}
}

// TestHasTrialPack_GlobalProPlanTrial is the live Global one-shot pack shape.
func TestHasTrialPack_GlobalProPlanTrial(t *testing.T) {
	cr := &creditsSummary{
		Packages: []packageSummary{
			{Name: "CodeBuddy One-time Free 2-Week Pro Plan Trial", Remain: 250, Used: 0},
		},
	}
	if !hasTrialPack(cr) {
		t.Error("expected hasTrialPack=true for Global Pro Plan Trial pack")
	}
}

// TestHasTrialPack_LiveBonusPackShape locks the live 2026-10-14 Global payload:
// the 250-credit / 14-day pack is named plain "Bonus Pack", identical to the
// daily 30-credit growth reward, so name matching alone never fired and both
// accounts showed 专家包待领取 while the pack was already granted.
func TestHasTrialPack_LiveBonusPackShape(t *testing.T) {
	cr := &creditsSummary{
		Packages: []packageSummary{
			{Name: "Bonus Pack", Size: 250, Remain: 250, Used: 0, CycleStart: "2026-10-11 23:41:29", CycleEnd: "2026-10-25 23:41:28"},
			{Name: "Free Plan Subscription", Size: 100, Remain: 100, Used: 0, CycleStart: "2026-10-01 00:00:00", CycleEnd: "2026-10-31 23:59:59"},
			{Name: "Bonus Pack", Size: 30, Remain: 30, Used: 0, CycleStart: "2026-10-14 02:41:16", CycleEnd: "2026-11-14 02:41:15"},
		},
	}
	if !hasTrialPack(cr) {
		t.Error("expected hasTrialPack=true for the live Bonus Pack (250 credits / 14 days)")
	}
}

// TestHasTrialPack_DailyBonusOnlyIsNotTrial guards the false-positive edge: an
// account with only the 30-credit daily reward must still show 待领取.
func TestHasTrialPack_DailyBonusOnlyIsNotTrial(t *testing.T) {
	cr := &creditsSummary{
		Packages: []packageSummary{
			{Name: "Bonus Pack", Size: 30, Remain: 30, Used: 0, CycleStart: "2026-10-14 02:41:16", CycleEnd: "2026-11-14 02:41:15"},
		},
	}
	if hasTrialPack(cr) {
		t.Error("expected hasTrialPack=false for a 30-credit daily Bonus Pack")
	}
}

// TestHasTrialPack_ExpiredWindowNotDetected documents the shape rule's blind
// spot: once the pack's cycle fields are missing or the window is not ~14 days,
// only name matching can still recognise it.
func TestHasTrialPack_ExpiredWindowNotDetected(t *testing.T) {
	cr := &creditsSummary{
		Packages: []packageSummary{
			{Name: "Bonus Pack", Size: 250, Remain: 0, Used: 250, CycleStart: "", CycleEnd: ""},
		},
	}
	if hasTrialPack(cr) {
		t.Error("expected hasTrialPack=false when the cycle fields are absent (shape undetectable)")
	}
}

// TestIsTrialPackShape_WindowBoundaries pins the 10–20 day acceptance window.
func TestIsTrialPackShape_WindowBoundaries(t *testing.T) {
	cases := []struct {
		name string
		pkg  packageSummary
		want bool
	}{
		{"14d", packageSummary{Size: 250, CycleStart: "2026-10-11 23:41:29", CycleEnd: "2026-10-25 23:41:28"}, true},
		{"monthly", packageSummary{Size: 250, CycleStart: "2026-10-01 00:00:00", CycleEnd: "2026-10-31 23:59:59"}, false},
		{"wrong size", packageSummary{Size: 30, CycleStart: "2026-10-11 23:41:29", CycleEnd: "2026-10-25 23:41:28"}, false},
		{"bad time", packageSummary{Size: 250, CycleStart: "not-a-time", CycleEnd: "2026-10-25 23:41:28"}, false},
	}
	for _, tc := range cases {
		if got := isTrialPackShape(tc.pkg); got != tc.want {
			t.Errorf("isTrialPackShape(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestHasTrialPack_Nil tests nil safety.
func TestHasTrialPack_Nil(t *testing.T) {
	if hasTrialPack(nil) {
		t.Error("expected hasTrialPack=false for nil credits")
	}
}

// TestHasTrialPack_Empty tests empty packages.
func TestHasTrialPack_Empty(t *testing.T) {
	cr := &creditsSummary{}
	if hasTrialPack(cr) {
		t.Error("expected hasTrialPack=false for empty packages")
	}
}
