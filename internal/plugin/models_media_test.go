package main

import (
	"encoding/json"
	"testing"
)

// 主路径必须与兜底路径一致：纯图像/视频生成模型不注册。
func TestBuildGlobalModelsDropsMediaGeneration(t *testing.T) {
	cfg := &globalProductConfig{
		Models: []globalModelWire{
			{ID: "glm-5.3", Name: "GLM-5.3"},
			{ID: "hy-image-3.5", Name: "HY-Image-3.5", Tags: []string{"text-to-image", "image-to-image"}},
			{ID: "kling-video-v3", Name: "Kling-Video-V3", Tags: []string{"text-to-video"}, VideoGeneration: json.RawMessage(`{"maxSeconds":15}`)},
			{ID: "some-video-no-tags", Name: "Video", VideoGeneration: json.RawMessage(`{"maxSeconds":5}`)},
		},
	}
	got := buildGlobalModels(cfg, "standard")
	ids := make(map[string]bool, len(got))
	for _, m := range got {
		ids[m.ID] = true
	}
	if !ids["glm-5.3"] {
		t.Fatalf("glm-5.3 应保留")
	}
	for _, id := range []string{"hy-image-3.5", "kling-video-v3", "some-video-no-tags"} {
		if ids[id] {
			t.Fatalf("%q 是图像/视频生成模型，不应注册", id)
		}
	}
}

// 账号等级来自 PackageCode，而不是已下线的 get-user-resource-summary。
func TestAccountPackageTierMapping(t *testing.T) {
	cases := map[string]string{
		"TCACA_code_001_PqouKr6QWV": "free",
		"TCACA_code_035_ArVxJcGDsm": "free",
		"TCACA_code_008_cfWoLwvjU4": "free",
		"TCACA_code_002_AkiJS3ZHF5": "standard",
		"TCACA_code_003_FAnt7lcmRT": "standard",
		"TCACA_code_005_maRGyrHhw1": "standard",
		"TCACA_code_039_KRcQj7wUat": "standard",
		"TCACA_code_040_mi9rCYg46x": "standard",
		"TCACA_code_023_4xbGhMrE6q": "youth",
		"TCACA_code_026_BaESVICNoi": "advanced",
		"TCACA_code_027_0FCGVA6vSa": "flagship",
	}
	for code, want := range cases {
		got, ok := accountPackageTier[code]
		if !ok {
			t.Fatalf("%s 未在映射表里", code)
		}
		if got != want {
			t.Fatalf("%s -> %q, want %q", code, got, want)
		}
	}
	if _, ok := accountPackageTier["TCACA_code_007_nzdH5h4Nl0"]; ok {
		t.Fatalf("活动/裂变包不应表达账号等级")
	}
}

// 取最高等级：多包账号不能被一个低等级包拉低。
func TestHighestPackageTierWins(t *testing.T) {
	tiers := []string{
		accountPackageTier["TCACA_code_008_cfWoLwvjU4"], // free
		accountPackageTier["TCACA_code_027_0FCGVA6vSa"], // flagship
		accountPackageTier["TCACA_code_002_AkiJS3ZHF5"], // standard
	}
	best, bestRank := "", 0
	for _, tier := range tiers {
		if r := userTypeRank[tier]; r > bestRank {
			best, bestRank = tier, r
		}
	}
	if best != "flagship" {
		t.Fatalf("最高等级 = %q, want flagship", best)
	}
}
