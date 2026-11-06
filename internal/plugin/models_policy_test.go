package main

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// 内嵌清单必须能解析——解析失败时 applyModelPolicy 会静默跳过全部剔除，
// 那会让本应隐藏的旧模型重新出现在 /v1/models 里。
func TestModelExclusionsLoad(t *testing.T) {
	_, parsed, err := loadModelExclusions()
	if err != nil {
		t.Fatalf("models_excluded.json 解析失败: %v", err)
	}
	if len(parsed.Abstract.IDs) == 0 || len(parsed.Legacy.IDs) == 0 || len(parsed.MiniMax.IDs) == 0 {
		t.Fatalf("排除清单有空组: abstract=%d legacy=%d minimax=%d",
			len(parsed.Abstract.IDs), len(parsed.Legacy.IDs), len(parsed.MiniMax.IDs))
	}
}

// 规则一：抽象档位必须全部剔除。
func TestModelPolicyDropsAbstractTiers(t *testing.T) {
	for _, id := range []string{
		"auto", "default", "default-model", "fast-model",
		"balanced-model", "primary-model", "deep-model",
	} {
		if !excludedByModelPolicy(id) {
			t.Fatalf("%q 应作为抽象档位被剔除", id)
		}
	}
}

// 规则二（glm）：5.2 及以前剔除，5.3 及以后保留；视觉系列 glm-5v-turbo 也剔除。
func TestModelPolicyGLMExclusions(t *testing.T) {
	excluded := []string{
		"glm-4.6", "glm-4.6v", "glm-4.7",
		"glm-5", "glm-5.0", "glm-5.0-turbo", "glm-5.1", "glm-5.2",
		"glm-5v-turbo",
	}
	for _, id := range excluded {
		if !excludedByModelPolicy(id) {
			t.Fatalf("%q 应作为旧 glm 模型被剔除", id)
		}
	}
	kept := []string{"glm-5.3", "glm-5.3-flash", "glm-5.3-flashx", "glm-6.0"}
	for _, id := range kept {
		if excludedByModelPolicy(id) {
			t.Fatalf("%q 应保留", id)
		}
	}
}

// 规则二（kimi）：K2.6 及以前剔除，K2.7 及以后保留。
func TestModelPolicyKimiExclusions(t *testing.T) {
	excluded := []string{
		"kimi-k2", "kimi-k2-instruct", "kimi-k2-instruct-taiji",
		"kimi-k2-thinking", "kimi-k2.5", "kimi-k2.6",
	}
	for _, id := range excluded {
		if !excludedByModelPolicy(id) {
			t.Fatalf("%q 应作为旧 kimi 模型被剔除", id)
		}
	}
	kept := []string{"kimi-k2.7", "kimi-k2.8-preview", "kimi-k3", "kimi-k3-2"}
	for _, id := range kept {
		if excludedByModelPolicy(id) {
			t.Fatalf("%q 应保留", id)
		}
	}
	// kimi-k3-1 属于产品策略排除（与 kimi-k3.1-pro 重叠），不是 legacy。
	if !excludedByModelPolicy("kimi-k3-1") {
		t.Fatalf("kimi-k3-1 应按产品策略剔除")
	}
}

// 规则三：MiniMax 系列全部剔除。
func TestModelPolicyDropsMiniMax(t *testing.T) {
	for _, id := range []string{"minimax", "minimax-m3", "minimax-m2.7", "minimax-m2.5", "minimax-m3-pay"} {
		if !excludedByModelPolicy(id) {
			t.Fatalf("%q 应作为 MiniMax 模型被剔除", id)
		}
	}
}

// 清单外的模型一律保留——清单策略的前提就是「只删明确知道的」。
func TestModelPolicyKeepsEverythingElse(t *testing.T) {
	kept := []string{
		"deepseek-v4.1-flash", "deepseek-v4-pro", "hy3", "hy4-preview",
		"gpt-6-astra", "grok-4.7", "gemini-3.8-flash", "step-5-preview",
		"mimo-v2.6-pro", "qwen3.8-max",
		// 版本号更大的新模型不能因为不在清单里被误删。
		"glm-5.30", "glm-6.0", "kimi-k2.60", "kimi-k3",
	}
	for _, id := range kept {
		if excludedByModelPolicy(id) {
			t.Fatalf("%q 应保留", id)
		}
	}
}

// 大小写不敏感：服务端下发过 "GLM-5.3" 与 "glm-5.3" 两种写法。
func TestModelPolicyIsCaseInsensitive(t *testing.T) {
	if !excludedByModelPolicy("GLM-5.2") {
		t.Fatalf("GLM-5.2 应被剔除")
	}
	if excludedByModelPolicy("GLM-5.3") {
		t.Fatalf("GLM-5.3 应保留")
	}
	if !excludedByModelPolicy("MiniMax-M3") {
		t.Fatalf("MiniMax-M3 应被剔除")
	}
}

// applyModelPolicy 必须保持原有顺序。
func TestApplyModelPolicyPreservesOrder(t *testing.T) {
	in := []pluginapi.ModelInfo{
		{ID: "glm-5.3"},
		{ID: "fast-model"},
		{ID: "kimi-k2.6"},
		{ID: "minimax-m3"},
		{ID: "deepseek-v4.1-flash"},
		{ID: "glm-5.2"},
	}
	got := ids(applyModelPolicy(in))
	want := []string{"glm-5.3", "deepseek-v4.1-flash"}
	if !equalStrings(got, want) {
		t.Fatalf("models = %v, want %v", got, want)
	}
}

// 兜底目录也必须过同一套策略，且表里不再列被排除的模型。
func TestGlobalFallbackPassesPolicy(t *testing.T) {
	for _, m := range globalFallbackModels() {
		if excludedByModelPolicy(m.ID) {
			t.Fatalf("兜底目录包含被排除的模型 %q", m.ID)
		}
	}
}

// 清单里的每个 ID 都必须真的会被剔除，避免表里写了但匹配不上。
func TestEveryExcludedIDIsActuallyExcluded(t *testing.T) {
	all := excludedModelIDs()
	if len(all) == 0 {
		t.Fatal("排除清单为空")
	}
	for _, id := range all {
		if !excludedByModelPolicy(id) {
			t.Fatalf("清单里的 %q 没有被剔除逻辑命中", id)
		}
	}
}
