package main

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// 复刻 selectTeamsModelsByCliAgent：白名单非空时只取白名单内的模型。
func TestBuildCNModelsUsesCLIAllowlist(t *testing.T) {
	models := []cnModelWire{
		{ID: "auto", Name: "Auto"},
		{ID: "glm-5.3", Name: "GLM-5.3", Credits: "x0.79 credits"},
		{ID: "old-model", Name: "Old"},
	}
	agents := []cnAgentWire{{Name: "cli", Models: []string{"glm-5.3"}}}

	got := ids(buildCNModels(models, agents, false))
	want := []string{"glm-5.3"}
	if !equalStrings(got, want) {
		t.Fatalf("models = %v, want %v", got, want)
	}
}

// 白名单为空时回退到全部模型——客户端 selectTeamsModelsByCliAgent 的
// `if (r.length === 0) return [...models]`。
func TestBuildCNModelsFallsBackToAllWhenAllowlistEmpty(t *testing.T) {
	models := []cnModelWire{
		{ID: "a", Name: "A"},
		{ID: "b", Name: "B"},
	}
	got := ids(buildCNModels(models, nil, false))
	want := []string{"a", "b"}
	if !equalStrings(got, want) {
		t.Fatalf("models = %v, want %v", got, want)
	}
}

// EnableAutoModelTiers 为 true 时，白名单里的 auto 展开成三个内置档位，
// 且三个档位带客户端写死的倍率。
func TestBuildCNModelsExpandsAutoIntoTiers(t *testing.T) {
	models := []cnModelWire{{ID: "auto", Name: "Auto"}}
	agents := []cnAgentWire{{Name: "cli", Models: []string{"auto", "glm-5.3"}}}
	models = append(models, cnModelWire{ID: "glm-5.3", Name: "GLM-5.3"})

	got := buildCNModels(models, agents, true)
	gotIDs := ids(got)
	want := []string{"fast-model", "balanced-model", "deep-model", "glm-5.3"}
	if !equalStrings(gotIDs, want) {
		t.Fatalf("models = %v, want %v", gotIDs, want)
	}
	// 倍率必须与客户端 AUTO_TIER_MODELS 常量一致。
	wantRates := map[string]float64{"fast-model": 0.21, "balanced-model": 0.65, "deep-model": 1.20}
	for _, m := range got {
		if want, ok := wantRates[m.ID]; ok && m.Rate != want {
			t.Fatalf("%s rate = %v, want %v", m.ID, m.Rate, want)
		}
	}
}

// 未开启分档时保留 auto 本身，不展开。
func TestBuildCNModelsKeepsAutoWhenTiersDisabled(t *testing.T) {
	models := []cnModelWire{{ID: "auto", Name: "Auto"}}
	agents := []cnAgentWire{{Name: "cli", Models: []string{"auto"}}}

	got := ids(buildCNModels(models, agents, false))
	want := []string{"auto"}
	if !equalStrings(got, want) {
		t.Fatalf("models = %v, want %v", got, want)
	}
}

// 服务端下发的 credits 必须透传成 Rate——客户端 toModelInfo 里
// `credits: e.credits` 是逐字段透传，倍率不是插件自己编的。
func TestCNModelInfoPassesThroughCredits(t *testing.T) {
	cases := []struct {
		credits string
		want    float64
	}{
		{"x0.79 credits", 0.79},
		{"x0.00 credits", 0},
		{"x0.11 credits", 0.11},
		{"x1.62 credits", 1.62},
		{"", 0},
	}
	for _, c := range cases {
		got := cnModelInfo(cnModelWire{ID: "m", Name: "M", Credits: c.credits}).Rate
		if got != c.want {
			t.Fatalf("credits %q → rate %v, want %v", c.credits, got, c.want)
		}
	}
}

// 白名单里不存在于 models 的 id 必须跳过，不能凭空造模型。
func TestBuildCNModelsSkipsUnknownIDs(t *testing.T) {
	models := []cnModelWire{{ID: "known", Name: "Known"}}
	agents := []cnAgentWire{{Name: "cli", Models: []string{"known", "ghost"}}}

	got := ids(buildCNModels(models, agents, false))
	want := []string{"known"}
	if !equalStrings(got, want) {
		t.Fatalf("models = %v, want %v", got, want)
	}
}

// 重复 id 只保留一次。
func TestBuildCNModelsDeduplicates(t *testing.T) {
	models := []cnModelWire{{ID: "a", Name: "A"}}
	agents := []cnAgentWire{{Name: "cli", Models: []string{"a", "a"}}}

	got := ids(buildCNModels(models, agents, false))
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("models = %v, want [a]", got)
	}
}

var _ = pluginapi.ModelInfo{}
