package main

import (
	"encoding/json"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// resetModelSelection 清理全局状态，避免测试间互相污染。
func resetModelSelection(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { applyModelSelection(nil) })
	applyModelSelection(nil)
}

// TestModelSelectionAbsentRegistersEverything 钉住「未配置 = 全量注册」。
// 这是向后兼容的基石：升级后没有配置过 model_selection 的实例行为不能变。
func TestModelSelectionAbsentRegistersEverything(t *testing.T) {
	resetModelSelection(t)

	if selectionConfigured() {
		t.Fatal("fresh state must report not configured")
	}
	models := []pluginapi.ModelInfo{{ID: "glm-5.3"}, {ID: "kimi-k3"}}
	got := filterByModelSelection(models)
	if len(got) != 2 {
		t.Fatalf("unconfigured selection filtered %d models, want 2", len(got))
	}
	if name, ok := registeredNameFor("glm-5.3"); !ok || name != "glm-5.3" {
		t.Fatalf("registeredNameFor = %q/%v, want glm-5.3/true", name, ok)
	}
}

// TestModelSelectionEmptyRegistersNothing 钉住「配置为空 = 一个都不注册」。
// 与上一个测试成对：把 nil 与空切片合并会让运维清空勾选后整个 provider 静默消失。
func TestModelSelectionEmptyRegistersNothing(t *testing.T) {
	resetModelSelection(t)
	applyModelSelection([]modelSelectionEntry{})

	if !selectionConfigured() {
		t.Fatal("empty slice must still report configured")
	}
	models := []pluginapi.ModelInfo{{ID: "glm-5.3"}, {ID: "kimi-k3"}}
	got := filterByModelSelection(models)
	if len(got) != 0 {
		t.Fatalf("empty selection kept %d models, want 0", len(got))
	}
}

// TestModelSelectionKeepsOnlyTicked 验证白名单语义：不在列表里的模型被完全隐藏。
func TestModelSelectionKeepsOnlyTicked(t *testing.T) {
	resetModelSelection(t)
	applyModelSelection([]modelSelectionEntry{{Upstream: "glm-5.3"}})

	models := []pluginapi.ModelInfo{
		{ID: "glm-5.3", Name: "GLM-5.3"},
		{ID: "kimi-k3", Name: "Kimi-K3"},
		{ID: "deepseek-v4.1-flash", Name: "DeepSeek-V4.1-Flash"},
	}
	got := filterByModelSelection(models)
	if len(got) != 1 || got[0].ID != "glm-5.3" {
		t.Fatalf("filtered = %+v, want only glm-5.3", got)
	}
}

// TestModelSelectionCustomRegistrationName 验证自定义注册名双向映射。
// ID 是客户端请求名，Name 是上游原生名，两者必须同时正确设置。
func TestModelSelectionCustomRegistrationName(t *testing.T) {
	resetModelSelection(t)
	applyModelSelection([]modelSelectionEntry{
		{Upstream: "DeepSeek-V4.1-Flash", Name: "ds-flash"},
	})

	models := []pluginapi.ModelInfo{{ID: "DeepSeek-V4.1-Flash", Name: "DeepSeek-V4.1-Flash"}}
	got := filterByModelSelection(models)
	if len(got) != 1 {
		t.Fatalf("filtered %d models, want 1", len(got))
	}
	if got[0].ID != "ds-flash" {
		t.Fatalf("registered ID = %q, want ds-flash", got[0].ID)
	}
	if got[0].Name != "DeepSeek-V4.1-Flash" {
		t.Fatalf("upstream Name = %q, want DeepSeek-V4.1-Flash", got[0].Name)
	}

	// 反解：客户端请求 ds-flash → 上游 DeepSeek-V4.1-Flash。
	if upstream := upstreamForRegistered("ds-flash"); upstream != "DeepSeek-V4.1-Flash" {
		t.Fatalf("upstreamForRegistered(ds-flash) = %q, want DeepSeek-V4.1-Flash", upstream)
	}
}

// TestModelSelectionPreservesTickOrder 验证输出按勾选顺序，
// 让面板里的顺序与网关上的顺序一致。
func TestModelSelectionPreservesTickOrder(t *testing.T) {
	resetModelSelection(t)
	applyModelSelection([]modelSelectionEntry{
		{Upstream: "kimi-k3"},
		{Upstream: "glm-5.3"},
		{Upstream: "hy3"},
	})

	models := []pluginapi.ModelInfo{
		{ID: "glm-5.3"}, {ID: "hy3"}, {ID: "kimi-k3"},
	}
	got := filterByModelSelection(models)
	if len(got) != 3 {
		t.Fatalf("filtered %d models, want 3", len(got))
	}
	want := []string{"kimi-k3", "glm-5.3", "hy3"}
	for i, id := range want {
		if got[i].ID != id {
			t.Fatalf("position %d = %q, want %q (whole list %+v)", i, got[i].ID, id, got)
		}
	}
}

// TestCoerceModelSelectionDistinguishesAbsentFromEmpty 是本文件最重要的契约：
// 键不存在 → nil（全量注册）；键存在但为空 → 空切片（零注册）。
func TestCoerceModelSelectionDistinguishesAbsentFromEmpty(t *testing.T) {
	if got := coerceModelSelection(nil); got != nil {
		t.Fatalf("absent key = %#v, want nil", got)
	}
	if got := coerceModelSelection([]any{}); got == nil {
		t.Fatal("present-but-empty must not collapse to nil")
	} else if len(got) != 0 {
		t.Fatalf("present-but-empty = %#v, want zero length", got)
	}
}

// TestCoerceModelSelectionSkipsIncompleteRows 验证缺 upstream 的行被跳过，
// 而不是生成一个空 upstream 的幽灵条目。
func TestCoerceModelSelectionSkipsIncompleteRows(t *testing.T) {
	value := []any{
		map[string]any{"upstream": "glm-5.3"},
		map[string]any{"name": "no-upstream"},
		map[string]any{"upstream": "  "},
		map[string]any{"upstream": "kimi-k3", "name": "k3"},
	}
	got := coerceModelSelection(value)
	if len(got) != 2 {
		t.Fatalf("coerced %d entries, want 2: %#v", len(got), got)
	}
	if got[0].Upstream != "glm-5.3" || got[1].Upstream != "kimi-k3" || got[1].Name != "k3" {
		t.Fatalf("coerced entries = %#v", got)
	}
}

// TestBuildModelCandidatesPutsSelectedFirst 验证勾选器的排序：
// 已勾选的在前，其余按显示名排序。
func TestBuildModelCandidatesPutsSelectedFirst(t *testing.T) {
	resetModelSelection(t)
	applyModelSelection([]modelSelectionEntry{{Upstream: "kimi-k3"}})

	models := []pluginapi.ModelInfo{
		{ID: "glm-5.3", DisplayName: "GLM-5.3"},
		{ID: "kimi-k3", DisplayName: "Kimi-K3"},
		{ID: "hy3", DisplayName: "Hy3"},
	}
	got := buildModelCandidates(models)
	if len(got) != 3 {
		t.Fatalf("candidates = %d, want 3", len(got))
	}
	if got[0].Upstream != "kimi-k3" || !got[0].Selected {
		t.Fatalf("first candidate = %+v, want selected kimi-k3", got[0])
	}
	for _, row := range got[1:] {
		if row.Selected {
			t.Fatalf("unexpected selected row after the first: %+v", row)
		}
	}
}

// TestConfigureAppliesModelSelection 验证配置真的会流到全局状态。
func TestConfigureAppliesModelSelection(t *testing.T) {
	resetModelSelection(t)

	configYAML := "model_selection:\n  - upstream: glm-5.3\n    name: glm\n  - upstream: kimi-k3\n"
	payload, _ := json.Marshal(map[string][]byte{"config_yaml": []byte(configYAML)})
	configure(payload)

	entries := selectedModels()
	if len(entries) != 2 {
		t.Fatalf("configured %d entries, want 2: %#v", len(entries), entries)
	}
	if entries[0].Upstream != "glm-5.3" || entries[0].Name != "glm" {
		t.Fatalf("entry 0 = %#v", entries[0])
	}
	if entries[1].Upstream != "kimi-k3" || entries[1].Name != "" {
		t.Fatalf("entry 1 = %#v", entries[1])
	}
}

// TestConfigureOmittingModelSelectionMeansAll 验证一份不含 model_selection 的
// 配置不会把插件变成「零注册」。
func TestConfigureOmittingModelSelectionMeansAll(t *testing.T) {
	resetModelSelection(t)
	// 先配置一次，确认后续省略该键会回到「未配置」。
	payload, _ := json.Marshal(map[string][]byte{"config_yaml": []byte("model_selection: []\n")})
	configure(payload)
	if selectedModels() == nil {
		t.Fatal("setup: expected configured selection")
	}

	payload, _ = json.Marshal(map[string][]byte{"config_yaml": []byte("checkin_auto: true\n")})
	configure(payload)
	if selectedModels() != nil {
		t.Fatalf("omitted key left selection = %#v, want nil", selectedModels())
	}
}

// TestConfigureReadsCheckinHours 验证签到时刻可配。
func TestConfigureReadsCheckinHours(t *testing.T) {
	t.Cleanup(func() {
		checkinHoursMu.Lock()
		checkinHours = []int{9, 21}
		checkinHoursMu.Unlock()
	})

	payload, _ := json.Marshal(map[string][]byte{"config_yaml": []byte("checkin_hours: \"8, 20\"\n")})
	configure(payload)

	got := configuredCheckinHours()
	if len(got) != 2 || got[0] != 8 || got[1] != 20 {
		t.Fatalf("checkin hours = %v, want [8 20]", got)
	}
}

// TestConfigureIgnoresCommentAndQuotes 验证 YAML 解析比旧的逐行匹配更强：
// 行内注释与引号都能正确处理。
func TestConfigureIgnoresCommentAndQuotes(t *testing.T) {
	resetModelSelection(t)
	payload, _ := json.Marshal(map[string][]byte{
		"config_yaml": []byte("checkin_auto: \"false\"  # 暂停自动签到\n"),
	})
	configure(payload)

	if checkinAutoEnabled() {
		t.Fatal("checkin_auto quoted false with comment parsed as enabled")
	}
}

// checkinAutoEnabled 读取 checkinAuto 的当前值（带锁）。
func checkinAutoEnabled() bool {
	checkinAutoMu.RLock()
	defer checkinAutoMu.RUnlock()
	return checkinAuto
}
