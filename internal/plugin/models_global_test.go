package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// 固定住国际版的过滤规则：抽象名必须剔除，Pro Only 按账号等级剔除。
func TestBuildGlobalModelsDropsAbstractAndProOnlyForFree(t *testing.T) {
	cfg := &globalProductConfig{
		Models: []globalModelWire{
			{ID: "default-model", Name: "Auto"},
			{ID: "fast-model", Name: "Fast"},
			{ID: "deepseek-v4.1-flash", Name: "Deepseek-V4.1-Flash", Credits: "x0.00"},
			{ID: "glm-5.3", Name: "GLM-5.3", Credits: "x0.79"},
			{ID: "gpt-6-astra", Name: "GPT-6-Astra", Credits: "x6.67"},
			{ID: "grok-4.7", Name: "Grok-4.7", Credits: "x1.90"},
		},
		ModelTiers: []modelTier{{
			ID:               "gpt-pro-only",
			Tier:             "standard",
			RequiredUserType: "standard",
			ModelIDs:         []string{"gpt-6-astra", "grok-4.7"},
		}},
	}

	// buildGlobalModels 只负责复刻客户端的 Pro Only 分层过滤；抽象档位的
	// 剔除是插件自己的注册策略，在 applyModelPolicy 里，由 fetchGlobalModels
	// 在返回前统一执行。这里分别断言两层。
	free := buildGlobalModels(cfg, "free")
	got := ids(free)
	want := []string{"default-model", "fast-model", "deepseek-v4.1-flash", "glm-5.3"}
	if !equalStrings(got, want) {
		t.Fatalf("free models = %v, want %v", got, want)
	}

	paid := buildGlobalModels(cfg, "standard")
	got = ids(paid)
	want = []string{"default-model", "fast-model", "deepseek-v4.1-flash", "glm-5.3", "gpt-6-astra", "grok-4.7"}
	if !equalStrings(got, want) {
		t.Fatalf("standard models = %v, want %v", got, want)
	}

	// 再过一遍注册策略：抽象档位应被剔除。
	if got := ids(applyModelPolicy(free)); !equalStrings(got, []string{"deepseek-v4.1-flash", "glm-5.3"}) {
		t.Fatalf("free models after policy = %v", got)
	}
}

// 未声明 requiredUserType 时按 standard 处理，与客户端 resolveModelTierAccess
// 的默认值一致——不能因为字段缺失就把 Pro Only 模型白送给免费账号。
func TestBuildGlobalModelsUnknownTierDefaultsToStandard(t *testing.T) {
	cfg := &globalProductConfig{
		Models: []globalModelWire{
			{ID: "mystery-model", Name: "Mystery"},
		},
		ModelTiers: []modelTier{{
			ID:       "mystery-tier",
			ModelIDs: []string{"mystery-model"},
		}},
	}
	if got := ids(buildGlobalModels(cfg, "free")); len(got) != 0 {
		t.Fatalf("free models = %v, want none (unknown tier defaults to standard)", got)
	}
	if got := ids(buildGlobalModels(cfg, "standard")); len(got) != 1 {
		t.Fatalf("standard models = %v, want the model", got)
	}
}

// 兜底目录不能含抽象名，也不能含媒体模型——插件只服务 chat 补全。
func TestGlobalFallbackExcludesAbstractAndMedia(t *testing.T) {
	banned := map[string]struct{}{
		"default-model": {}, "fast-model": {}, "balanced-model": {},
		"primary-model": {}, "deep-model": {},
		"gpt-image-2.5-sunburst": {}, "seedance-2.5": {},
		"gemini-3.0-pro-image": {}, "gemini-3.1-flash-image": {},
		"gemini-2.5-flash-image": {}, "hunyuan-image-v3.0": {},
		"hunyuan-image-v2.0-general-edit": {}, "hunyuan-video-art": {},
	}
	for _, m := range globalFallbackModels() {
		if _, bad := banned[m.ID]; bad {
			t.Fatalf("fallback contains %q, which is abstract or a media model", m.ID)
		}
		if len(m.SupportedGenerationMethods) == 0 || m.SupportedGenerationMethods[0] != "chat" {
			t.Fatalf("fallback %q must declare chat generation", m.ID)
		}
	}
}

// 模型缓存的键必须区分 realm 与账号等级，否则后查询的账号会复用前一个
// 账号的模型集。
func TestDynamicModelsCacheKeyedByRealmAndUserType(t *testing.T) {
	resetDynamicModelsCache()
	storeDynamicModels("global:free", []pluginapi.ModelInfo{{ID: "free-only"}})
	storeDynamicModels("global:standard", []pluginapi.ModelInfo{{ID: "paid-only"}})

	if got, ok := cachedDynamicModels("global:free"); !ok || len(got) != 1 || got[0].ID != "free-only" {
		t.Fatalf("global:free = %v ok=%v", got, ok)
	}
	if got, ok := cachedDynamicModels("global:standard"); !ok || len(got) != 1 || got[0].ID != "paid-only" {
		t.Fatalf("global:standard = %v ok=%v", got, ok)
	}
	if _, ok := cachedDynamicModels("cn:free"); ok {
		t.Fatalf("cn:free should be a miss")
	}
}

func resetDynamicModelsCache() {
	dynamicModelsCache.Lock()
	dynamicModelsCache.entries = nil
	dynamicModelsCache.Unlock()
}

func ids(models []pluginapi.ModelInfo) []string {
	out := make([]string, 0, len(models))
	for _, m := range models {
		out = append(out, m.ID)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// /v3/config 必须用 WorkBuddy 桌面客户端的 UA 请求。
//
// 服务端按 UA 分流：CLI UA 只下发 20 个模型且完全不带 modelTiers，于是
// Pro Only 过滤失效、5 个付费模型会被白送给免费账号。这个测试直接观察
// 生产代码构造的出站 wire，而不是断言常量本身。
func TestFetchGlobalProductConfigUsesWorkBuddyClientUA(t *testing.T) {
	restore := installFakeHostAPI(t)
	defer restore()

	var gotUA string
	abi.SetHostCaller(func(method string, request []byte) ([]byte, error) {
		var body map[string]any
		_ = json.Unmarshal(request, &body)
		// 线上形态是 {"headers":{"User-Agent":["..."]},...}——header 值是
		// 字符串数组（http.Header 的自然 JSON 形态），不是单个字符串。
		readHeaders := func(m map[string]any) {
			headers, ok := m["headers"].(map[string]any)
			if !ok {
				return
			}
			for k, v := range headers {
				if !strings.EqualFold(k, "User-Agent") {
					continue
				}
				switch typed := v.(type) {
				case string:
					gotUA = typed
				case []any:
					if len(typed) > 0 {
						gotUA, _ = typed[0].(string)
					}
				}
			}
		}
		readHeaders(body)
		if inner, ok := body["request"].(map[string]any); ok {
			readHeaders(inner)
		}
		payload := `{"code":0,"msg":"OK","data":{"models":[],"modelTiers":[]}}`
		envelope, _ := json.Marshal(map[string]any{
			"ok": true,
			// pluginapi.HTTPResponse 没有 json tag，反序列化按字段名匹配，
			// 所以这里必须写 StatusCode 而不是 status_code。
			"result": map[string]any{
				"StatusCode": 200,
				"Body":       base64.StdEncoding.EncodeToString([]byte(payload)),
			},
		})
		return envelope, nil
	})
	t.Cleanup(func() { abi.ClearHostCaller() })

	sa := &storedAuth{Auth: storedTokens{AccessToken: "token", Domain: "www.codebuddy.ai"}}
	if _, err := fetchGlobalProductConfig(context.Background(), sa); err != nil {
		t.Fatalf("fetchGlobalProductConfig: %v", err)
	}
	if gotUA != workBuddyClientUA {
		t.Fatalf("User-Agent = %q, want %q (CLI UA makes the server withhold modelTiers)", gotUA, workBuddyClientUA)
	}
}
