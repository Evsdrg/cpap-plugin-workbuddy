// models.go implements the ModelProvider capability: static and per-auth
// model lists, dynamic model discovery via the upstream models API, alias
// reverse resolution (client-facing alias → upstream model id), and the
// host-config oauth-excluded-models filter.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"strconv"
	"sync"
)

func wbModels() []pluginapi.ModelInfo {
	return []pluginapi.ModelInfo{
		{ID: "glm-5.2", Name: "GLM-5.2", ContextLength: 1000000, MaxCompletionTokens: 8192, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "glm-5.1", Name: "GLM-5.1", ContextLength: 131072, MaxCompletionTokens: 8192, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "glm-5v-turbo", Name: "GLM-5V Turbo", ContextLength: 131072, MaxCompletionTokens: 8192, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "kimi-k2.7", Name: "Kimi K2.7", ContextLength: 262144, MaxCompletionTokens: 8192, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "kimi-k3", Name: "Kimi K3", ContextLength: 262144, MaxCompletionTokens: 8192, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "minimax-m3", Name: "MiniMax M3", ContextLength: 204800, MaxCompletionTokens: 8192, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "hy3", Name: "Hy3", ContextLength: 262144, MaxCompletionTokens: 8192, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "hy4-preview", Name: "Hy4 preview", ContextLength: 1000000, MaxCompletionTokens: 64000, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "hy3-preview", Name: "Hy3 Preview", ContextLength: 262144, MaxCompletionTokens: 8192, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "hy3-preview-agent", Name: "Hy3 Preview Agent", ContextLength: 262144, MaxCompletionTokens: 8192, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "deepseek-v4-pro", Name: "DeepSeek V4 Pro", ContextLength: 1000000, MaxCompletionTokens: 8192, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "deepseek-v4-flash", Name: "DeepSeek V4 Flash", ContextLength: 1000000, MaxCompletionTokens: 8192, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		// Hidden models that the CodeBuddy upstream accepts when called directly.
		{ID: "gpt-5.6-luna", Name: "GPT-5.6 Luna", ContextLength: 200000, MaxCompletionTokens: 16384, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "claude-opus-5", Name: "Claude Opus 5", ContextLength: 200000, MaxCompletionTokens: 16384, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "gpt-5.6-sol", Name: "GPT-5.6 Sol", ContextLength: 200000, MaxCompletionTokens: 16384, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "glm-5.3", Name: "GLM-5.3", ContextLength: 200000, MaxCompletionTokens: 16384, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "gpt-5.6-terra", Name: "GPT-5.6 Terra", ContextLength: 200000, MaxCompletionTokens: 16384, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "deepseek-v4.1-flash", Name: "DeepSeek V4.1 Flash", ContextLength: 200000, MaxCompletionTokens: 16384, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		{ID: "glm-5.3-flash", Name: "GLM-5.3 Flash", ContextLength: 200000, MaxCompletionTokens: 16384, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
		// Live-verified 2026-09-29: 200 on a Global account, served as
		// gpt-6-astra, credit 0.04 for 18 prompt + 5 completion tokens. Not in
		// any other CPA plugin's list, so it is easy to miss.
		{ID: "gpt-6-astra", Name: "GPT-6 Astra", ContextLength: 200000, MaxCompletionTokens: 16384, OwnedBy: providerName, SupportedGenerationMethods: []string{"chat"}},
	}
}

func cachedDynamicModels(key string) ([]pluginapi.ModelInfo, bool) {
	dynamicModelsCache.RLock()
	defer dynamicModelsCache.RUnlock()
	e, ok := dynamicModelsCache.entries[key]
	if !ok || len(e.models) == 0 || time.Since(e.fetched) >= dynamicModelsCacheTTL {
		return nil, false
	}
	return e.models, true
}

func storeDynamicModels(key string, models []pluginapi.ModelInfo) {
	dynamicModelsCache.Lock()
	if dynamicModelsCache.entries == nil {
		dynamicModelsCache.entries = make(map[string]dynamicModelsEntry)
	}
	dynamicModelsCache.entries[key] = dynamicModelsEntry{models: models, fetched: time.Now()}
	dynamicModelsCache.Unlock()
}

// fetchDynamicModelsFromStorage 按 realm 分派到各自的权威端点。
//
//   - 国际版：GET /v3/config（models 27 个，含 modelTiers 的 Pro Only 分层）
//   - 国内版：GET /console/enterprises/personal/models（models 33 个，
//     agents[cli] 18 个白名单）
//
// 两个 realm 的端点不能互换，详见 models_global.go 的文件头注释。
//
// 两个 realm 都只走自己的实时端点，不保留 /v2 那一支：2026-10-19 实测
// Global 的 /v2/enterprises/personal/models 只有 18 个陈旧模型（没有
// deepseek-v4.1-flash、grok-4.7、gpt-6-astra），拿它当兜底会把错误目录
// 当成正确结果。真正失败时回落到客户端 product.json 提取的静态目录。
func fetchDynamicModelsFromStorage(storageJSON []byte) []pluginapi.ModelInfo {
	sa, err := parseStored(storageJSON)
	if err != nil {
		return wbModels()
	}
	realm := accountRegion(sa)
	userType := "free"
	if realm == "global" {
		userType = accountUserType(sa)
	}
	key := realm + ":" + userType
	if models, ok := cachedDynamicModels(key); ok {
		return models
	}
	if realm == "global" {
		if dyn, err := fetchGlobalModels(context.Background(), sa); err == nil && len(dyn) > 0 {
			storeDynamicModels(key, dyn)
			return dyn
		}
		return globalFallbackModels()
	}
	if dyn, err := fetchCNModels(context.Background(), sa); err == nil && len(dyn) > 0 {
		storeDynamicModels(key, dyn)
		return dyn
	}
	return wbModels()
}

// fetchDynamicModels calls the CodeBuddy API to get the latest model list.
// Falls back to the hardcoded list on any error.
// extractAccessToken handles both flat (CPA UI) and nested (plugin OAuth) auth file shapes.
func extractAccessToken(raw []byte) (string, bool) {
	// flat shape from CPA-Manager-Plus UI
	var flat struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(raw, &flat); err == nil && strings.TrimSpace(flat.AccessToken) != "" {
		return flat.AccessToken, true
	}
	// nested shape from plugin OAuth
	var nested storedAuth
	if err := json.Unmarshal(raw, &nested); err == nil && strings.TrimSpace(nested.Auth.AccessToken) != "" {
		return nested.Auth.AccessToken, true
	}
	return "", false
}

// realmFromToken decodes the JWT iss claim to determine the account realm.
// Global tokens have iss=...workbuddy.ai...; CN tokens have iss=...codebuddy.cn...
// Returns true if the token is Global.
func isGlobalToken(accessToken string) bool {
	parts := strings.Split(accessToken, ".")
	if len(parts) < 2 {
		return false
	}
	payload := parts[1]
	// base64url padding
	if pad := len(payload) % 4; pad != 0 {
		payload += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return false
	}
	var claims struct {
		ISS string `json:"iss"`
	}
	if json.Unmarshal(raw, &claims) != nil {
		return false
	}
	return strings.Contains(strings.ToLower(claims.ISS), "workbuddy.ai") || strings.Contains(strings.ToLower(claims.ISS), "codebuddy.ai")
}

// callModelsAPI gets the upstream model catalog for one account.
//
// Endpoint selection is per-realm and the Global path moved on 2026-10-14: the
// legacy /console/enterprises/personal/models started answering plain
// openresty/APISIX HTTP 500 while /v2/enterprises/personal/models answers 200
// with the same shape. Discovery therefore tries the /v2 path first for Global
// and falls back to /console, so an upstream rollback keeps working.

// modelRates carries the billing multiplier for each discovered model.
//
// It lives beside the model list rather than inside it because this plugin
// builds against CLIProxyAPI v7, whose pluginapi.ModelInfo has no Rate field. The
// host decodes the model list into its own ModelInfo, which *does* have one, so
// the value is attached at serialization time (see modelResponseWithRates).
var modelRates sync.Map

// storeModelRate records one model's multiplier. Zero (unparseable or absent)
// clears any previous entry so a stale rate never outlives the upstream data.
func storeModelRate(id string, rate float64) {
	if strings.TrimSpace(id) == "" {
		return
	}
	if rate == 0 {
		modelRates.Delete(id)
		return
	}
	modelRates.Store(id, rate)
}

// modelRateFor reports a model's multiplier, or 0 when none was reported.
func modelRateFor(id string) float64 {
	if v, ok := modelRates.Load(id); ok {
		if rate, isFloat := v.(float64); isFloat {
			return rate
		}
	}
	return 0
}

// parseCreditsRate extracts the multiplier from the upstream `credits` field.
//
// Observed shapes: "x0.79 credits", "x0.00", "x3.47". Anything else yields 0,
// which the host reads as "the provider did not report a rate".
func parseCreditsRate(raw string) float64 {
	value := strings.TrimSpace(raw)
	value = strings.TrimSuffix(value, "credits")
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "x")
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	rate, err := strconv.ParseFloat(value, 64)
	if err != nil || rate < 0 {
		return 0
	}
	return rate
}

// modelInfoWithRate is pluginapi.ModelInfo plus the multiplier.
//
// The embedded struct flattens into the same JSON object, so the host sees one
// model object whose `Rate` key it can decode — the plugin's vendored ModelInfo
// simply cannot name that field itself.
type modelInfoWithRate struct {
	pluginapi.ModelInfo
	Rate float64 `json:"Rate,omitempty"`
}

// modelResponseWithRates is the wire form of pluginapi.ModelResponse that
// carries per-model multipliers.
type modelResponseWithRates struct {
	Provider   string
	Models     []modelInfoWithRate
	AuthUpdate pluginapi.AuthData
}

// withRates attaches the recorded multiplies to a model list.
func withRates(models []pluginapi.ModelInfo) []modelInfoWithRate {
	out := make([]modelInfoWithRate, 0, len(models))
	for _, m := range models {
		out = append(out, modelInfoWithRate{ModelInfo: m, Rate: modelRateFor(m.ID)})
	}
	return out
}

func cacheModelAliases(host pluginapi.HostConfigSummary) {
	entries := host.OAuthModelAlias[providerName]
	if len(entries) == 0 {
		// Host may key the channel case-insensitively; fall back to a scan.
		for channel, list := range host.OAuthModelAlias {
			if strings.EqualFold(strings.TrimSpace(channel), providerName) {
				entries = list
				break
			}
		}
	}
	byAlias := make(map[string]string, len(entries))
	for _, e := range entries {
		name := strings.TrimSpace(e.Name)
		alias := strings.TrimSpace(e.Alias)
		if name == "" || alias == "" || strings.EqualFold(name, alias) {
			continue
		}
		byAlias[strings.ToLower(alias)] = name
	}
	modelAliasCache.Lock()
	modelAliasCache.byAlias = byAlias
	modelAliasCache.Unlock()
}

// resolveUpstreamModel maps an aliased requested model back to the real
// upstream model ID. Returns the input unchanged when nothing matches.
func resolveUpstreamModel(model string, attributes map[string]string) string {
	m := strings.TrimSpace(model)
	if m == "" {
		return model
	}
	key := strings.ToLower(m)
	if name, ok := parseModelAliasAttribute(attributes)[key]; ok {
		return name
	}
	modelAliasCache.RLock()
	name, ok := modelAliasCache.byAlias[key]
	modelAliasCache.RUnlock()
	if ok {
		return name
	}
	return m
}

// parseModelAliasAttribute decodes a per-auth alias override from auth
// attributes. Accepts JSON ([{"name":...,"alias":...}] or {alias:name}) or
// comma-separated "alias=name" pairs.
func parseModelAliasAttribute(attributes map[string]string) map[string]string {
	if len(attributes) == 0 {
		return nil
	}
	raw := ""
	for _, k := range []string{"model_alias", "model-alias", "oauth-model-alias"} {
		if v := strings.TrimSpace(attributes[k]); v != "" {
			raw = v
			break
		}
	}
	if raw == "" {
		return nil
	}
	out := make(map[string]string)
	add := func(name, alias string) {
		name, alias = strings.TrimSpace(name), strings.TrimSpace(alias)
		if name != "" && alias != "" && !strings.EqualFold(name, alias) {
			out[strings.ToLower(alias)] = name
		}
	}
	if strings.HasPrefix(raw, "[") {
		var list []struct {
			Name  string `json:"name"`
			Alias string `json:"alias"`
		}
		if json.Unmarshal([]byte(raw), &list) == nil {
			for _, e := range list {
				add(e.Name, e.Alias)
			}
			return out
		}
	}
	if strings.HasPrefix(raw, "{") {
		var m map[string]string
		if json.Unmarshal([]byte(raw), &m) == nil {
			for alias, name := range m {
				add(name, alias)
			}
			return out
		}
	}
	for _, pair := range strings.Split(raw, ",") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			add(kv[1], kv[0])
		}
	}
	return out
}

// filterExcludedModels removes models listed in oauth-excluded-models for
// the workbuddy provider. The host passes this config via HostConfigSummary.
func filterExcludedModels(models []pluginapi.ModelInfo, host pluginapi.HostConfigSummary) []pluginapi.ModelInfo {
	if len(host.ExcludedModels) == 0 {
		return models
	}
	// Try exact provider match, then case-insensitive scan.
	excluded := host.ExcludedModels[providerName]
	if len(excluded) == 0 {
		for channel, list := range host.ExcludedModels {
			if strings.EqualFold(strings.TrimSpace(channel), providerName) {
				excluded = list
				break
			}
		}
	}
	if len(excluded) == 0 {
		return models
	}
	excludeSet := make(map[string]struct{}, len(excluded))
	for _, m := range excluded {
		excludeSet[strings.ToLower(strings.TrimSpace(m))] = struct{}{}
	}
	// Use a fresh slice — models[:0] would alias the input's backing array,
	// which may be the dynamicModelsCache's own slice. Mutating it in place
	// would corrupt the cache for subsequent callers (P0 bug: after one
	// filterExcludedModels call, cache returns the filtered list as the
	// "full" list on the next fetch).
	out := make([]pluginapi.ModelInfo, 0, len(models))
	for _, m := range models {
		if _, skip := excludeSet[strings.ToLower(m.ID)]; skip {
			continue
		}
		out = append(out, m)
	}
	return out
}

// publishUsage reports one upstream attempt into CPAMP request monitoring.
// requestedModel is client-facing (may be alias); upstreamModel is resolved.

func handleModelStatic(raw []byte) ([]byte, error) {
	var req pluginapi.StaticModelRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	cacheModelAliases(req.Host)
	models := wbModels()
	models = filterExcludedModels(models, req.Host)
	return okEnvelope(modelResponseWithRates{Provider: providerName, Models: withRates(models)})
}

func handleModelForAuth(raw []byte) ([]byte, error) {
	var req pluginapi.AuthModelRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	// Always return the plugin's canonical provider key. The host skips any
	// response whose Provider doesn't match the auth's provider, so echoing
	// req.AuthProvider back would silently drop the model list whenever the
	// auth file carries a non-canonical provider string.
	cacheModelAliases(req.Host)
	models := fetchDynamicModelsFromStorage(req.StorageJSON)
	models = filterExcludedModels(models, req.Host)
	// model_selection 是最终决定：在宿主的 oauth-excluded-models 过滤之后再
	// 按勾选裁一次。放在最后是因为它优先级最高——运维点过的才算数。
	models = filterByModelSelection(models)
	return okEnvelope(modelResponseWithRates{Provider: providerName, Models: withRates(models)})
}
