package main

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// TestBuildModelCandidatesCarriesRate 钉住勾选清单带倍率。
//
// 倍率必须来自 ModelInfo.Rate（目录构造时已填），不能读 modelRateFor——
// 那个 sync.Map 的生产代码从未写入，恒返回 0，面板上永远看不到倍率。
func TestBuildModelCandidatesCarriesRate(t *testing.T) {
	models := []pluginapi.ModelInfo{
		{ID: "glm-5.3", Name: "GLM-5.3", DisplayName: "GLM-5.3", Rate: 0.79},
		{ID: "hy3", Name: "Hy3", DisplayName: "Hy3", Rate: 0},
	}
	rows := buildModelCandidates(models)
	if len(rows) != 2 {
		t.Fatalf("候选 %d 行，want 2", len(rows))
	}
	byUpstream := map[string]float64{}
	for _, row := range rows {
		byUpstream[row.Upstream] = row.Rate
	}
	if byUpstream["glm-5.3"] != 0.79 {
		t.Fatalf("glm-5.3 倍率 = %v，want 0.79", byUpstream["glm-5.3"])
	}
	if byUpstream["hy3"] != 0 {
		t.Fatalf("hy3 倍率 = %v，want 0（上游未提供）", byUpstream["hy3"])
	}
}

// 候选清单必须与注册结果同口径：被排除策略剔除的模型不出现在候选里。
// 否则操作员会勾上一个永远不会被注册的模型，却得不到任何提示。
func TestModelCandidatesHidePolicyExcludedModels(t *testing.T) {
	models := []pluginapi.ModelInfo{
		{ID: "minimax-m3.1-flash", Name: "minimax-m3.1-flash"},
		{ID: "minimax-m3", Name: "minimax-m3"},
		{ID: "glm-5.2", Name: "glm-5.2"},
		{ID: "deepseek-v4.1-flash", Name: "deepseek-v4.1-flash"},
	}
	candidates := modelCandidateModels(models)
	for _, candidate := range candidates {
		if candidate.Upstream == "minimax-m3.1-flash" || candidate.Upstream == "minimax-m3" || candidate.Upstream == "glm-5.2" {
			t.Fatalf("policy-excluded model %q must not appear in the candidate list", candidate.Upstream)
		}
	}
	found := false
	for _, candidate := range candidates {
		if candidate.Upstream == "deepseek-v4.1-flash" {
			found = true
		}
	}
	if !found {
		t.Fatalf("deepseek-v4.1-flash must remain a candidate, got %+v", candidates)
	}
}
