package main

// 国内版（CN）的模型发现。
//
// 权威依据是客户端反编译，不是探测：
//
//	main/server.js 的 ModelsService.getTeamsModels()：
//	  let e = this.accountProvider?.()?.enterpriseId || `personal`,
//	      n = await this.http.get(`/console/enterprises/${e}/models`),
//	      r = n.data ?? n;
//	  return selectTeamsModelsByCliAgent(r.models ?? [], r.agents ?? []).map(toModelInfo)
//
//	main/server.js 的 selectTeamsModelsByCliAgent()：
//	  let r = agents.find(a => a.name === 'cli')?.models ?? [];
//	  if (r.length === 0) return [...models];   // 白名单为空 → 返回全部
//	  ...                                        // 否则按白名单筛
//
//	main/server.js 的 toModelInfo()：逐字段透传 credits / tags / maxInputTokens 等。
//
// 所以国内版必须：
//  1. 请求 /console/enterprises/{enterpriseId}/models（个人账号用字面量 personal）
//  2. 取 agents[cli].models 作为白名单；白名单为空才回退到全部 models
//  3. 直接透传服务端的 credits 与 tags，不自己编倍率
//
// 注意：国际版走 /v3/config，两个端点不能互换——CN 的 /v3/config 只下发 37 个
// 陈旧模型（最新到 glm-5.1，没有 glm-5.3 / kimi-k3-1 / step-5-preview），
// 而 CN 的 /console/enterprises/personal/models 才是客户端实际使用的数据源。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// autoTierModel 是客户端内置的 AUTO_TIER_MODELS 常量。
//
// 反编译自 main/dev-env-override.js：
//
//	AUTO_TIER_MODELS = [
//	  {credits:`x0.21`, id:`fast-model`,     name:`快速`, ...},
//	  {credits:`x0.65`, id:`balanced-model`, name:`均衡`, ...},
//	  {credits:`x1.20`, id:`deep-model`,     name:`极致`, ...},
//	]
//
// 客户端在 productFeatures.EnableAutoModelTiers 为 true 时，用
// expandAutoModelIds() 把白名单里的 `auto` 展开成这三个档位：
//
//	function expandAutoModelIds(t) {
//	  if (!Array.isArray(t) || !t.includes(`auto`)) return t;
//	  let n = new Set(AUTO_TIER_MODEL_IDS), i = [];
//	  for (let o of t) {
//	    let t = o === `auto` ? AUTO_TIER_MODEL_IDS : n.has(o) ? [] : [o];
//	    for (let n of t) i.includes(n) || i.push(n);
//	  }
//	  return i;
//	}
//
// 这三个模型不来自服务端，服务端只下发 `auto` 这个名字。它们的倍率是客户端
// 写死的常量，所以插件必须自己带。
var autoTierModels = []pluginapi.ModelInfo{
	{
		ID: "fast-model", Name: "快速", DisplayName: "快速",
		Description:   "优先响应速度，适合简单任务与快速问答",
		ContextLength: 200000, InputTokenLimit: 200000, MaxCompletionTokens: 48000,
		Rate: 0.21, OwnedBy: providerName,
		SupportedGenerationMethods: []string{"chat"},
		SupportedInputModalities:   []string{"text", "image"},
	},
	{
		ID: "balanced-model", Name: "均衡", DisplayName: "均衡",
		Description:   "兼顾速度与质量，适合大多数日常工作",
		ContextLength: 200000, InputTokenLimit: 200000, MaxCompletionTokens: 48000,
		Rate: 0.65, OwnedBy: providerName,
		SupportedGenerationMethods: []string{"chat"},
		SupportedInputModalities:   []string{"text", "image"},
	},
	{
		ID: "deep-model", Name: "极致", DisplayName: "极致",
		Description:   "优先深度与准确性，适合复杂分析和高要求任务",
		ContextLength: 200000, InputTokenLimit: 200000, MaxCompletionTokens: 48000,
		Rate: 1.20, OwnedBy: providerName,
		SupportedGenerationMethods: []string{"chat"},
		SupportedInputModalities:   []string{"text", "image"},
	},
}

// cnModelsResponse 是 /console/enterprises/{id}/models 的线上形态。
//
// 客户端里 `r = n.data ?? n`，说明这个端点可能把内容包在 data 里，也可能
// 直接平铺——两种都要能解。
type cnModelsResponse struct {
	Code int `json:"code"`
	Data *struct {
		Models     []cnModelWire `json:"models"`
		Agents     []cnAgentWire `json:"agents"`
		Endpoint   string        `json:"endpoint"`
		Properties struct {
			EnableAutoModelTiers bool `json:"EnableAutoModelTiers"`
		} `json:"productFeatures"`
	} `json:"data"`
	// 平铺形态
	Models     []cnModelWire `json:"models"`
	Agents     []cnAgentWire `json:"agents"`
	Endpoint   string        `json:"endpoint"`
	Properties struct {
		EnableAutoModelTiers bool `json:"EnableAutoModelTiers"`
	} `json:"productFeatures"`
}

// cnModelWire 是单个模型的线上形态，字段与客户端 toModelInfo 对齐。
type cnModelWire struct {
	ID                string   `json:"id"`
	Name              string   `json:"name"`
	DescriptionZh     string   `json:"descriptionZh"`
	DescriptionEn     string   `json:"descriptionEn"`
	Credits           string   `json:"credits"`
	Tags              []string `json:"tags"`
	MaxInputTokens    int64    `json:"maxInputTokens"`
	MaxOutputTokens   int64    `json:"maxOutputTokens"`
	MaxAllowedSize    int64    `json:"maxAllowedSize"`
	SupportsImages    bool     `json:"supportsImages"`
	SupportsReasoning bool     `json:"supportsReasoning"`
	OnlyReasoning     bool     `json:"onlyReasoning"`
	IsDefault         bool     `json:"isDefault"`
	Disabled          bool     `json:"disabled"`
}

type cnAgentWire struct {
	Name   string   `json:"name"`
	Models []string `json:"models"`
}

// fetchCNModels 拉取国内版模型目录，按客户端逻辑筛选与展开。
func fetchCNModels(ctx context.Context, sa *storedAuth) ([]pluginapi.ModelInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// 客户端用 account.enterpriseId，个人账号取字面量 personal。
	enterpriseID := strings.TrimSpace(sa.Account.EnterpriseID)
	if enterpriseID == "" {
		enterpriseID = "personal"
	}
	url := billingBaseFor(sa) + "/console/enterprises/" + enterpriseID + "/models"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	// 模型目录走桌面端接口（/console/enterprises/{id}/models），用桌面端身份。
	desktopTransportHeaders(req)
	req.Header.Set("Authorization", "Bearer "+sa.Auth.AccessToken)
	req.Header.Set("Accept", "application/json")
	resp, err := hostHTTPDo(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cn models status %d", resp.StatusCode)
	}
	var parsed cnModelsResponse
	if err := json.Unmarshal(resp.Body, &parsed); err != nil {
		return nil, fmt.Errorf("cn models parse: %w", err)
	}
	models, agents, enableTiers := parsed.Models, parsed.Agents, parsed.Properties.EnableAutoModelTiers
	if parsed.Data != nil {
		models, agents, enableTiers = parsed.Data.Models, parsed.Data.Agents, parsed.Data.Properties.EnableAutoModelTiers
	}
	// 抽象档位、过旧模型、MiniMax 三条注册规则统一在 models_policy.go 里执行。
	// 放在这里而不是 buildCNModels 里，是为了让 buildCNModels 保持纯粹的
	// 「复刻客户端筛选逻辑」职责，策略变更不影响它的测试。
	return applyModelPolicy(buildCNModels(models, agents, enableTiers)), nil
}

// buildCNModels 复刻 selectTeamsModelsByCliAgent + expandAutoModelIds。
func buildCNModels(models []cnModelWire, agents []cnAgentWire, enableAutoTiers bool) []pluginapi.ModelInfo {
	byID := make(map[string]cnModelWire, len(models))
	for _, m := range models {
		byID[m.ID] = m
	}

	// selectTeamsModelsByCliAgent：取 cli 白名单；为空则回退全部。
	var allow []string
	for _, a := range agents {
		if a.Name == "cli" {
			allow = a.Models
			break
		}
	}
	if len(allow) == 0 {
		allow = make([]string, 0, len(models))
		for _, m := range models {
			allow = append(allow, m.ID)
		}
	}

	out := make([]pluginapi.ModelInfo, 0, len(allow)+len(autoTierModels))
	seen := make(map[string]struct{}, len(allow)+len(autoTierModels))
	for _, id := range allow {
		// expandAutoModelIds：auto 展开成三个档位，档位名本身不再单列。
		if id == "auto" {
			if enableAutoTiers {
				for _, tier := range autoTierModels {
					if _, dup := seen[tier.ID]; dup {
						continue
					}
					seen[tier.ID] = struct{}{}
					out = append(out, tier)
				}
				continue
			}
			// 未开启分档时保留 auto 本身。
			if m, ok := byID[id]; ok {
				if _, dup := seen[id]; !dup {
					seen[id] = struct{}{}
					out = append(out, cnModelInfo(m))
				}
			}
			continue
		}
		m, ok := byID[id]
		if !ok || m.Disabled {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, cnModelInfo(m))
	}
	return out
}

// cnModelInfo 把服务端模型转成 pluginapi.ModelInfo，透传 credits 与 tags。
func cnModelInfo(m cnModelWire) pluginapi.ModelInfo {
	desc := strings.TrimSpace(m.DescriptionZh)
	if desc == "" {
		desc = strings.TrimSpace(m.DescriptionEn)
	}
	info := pluginapi.ModelInfo{
		ID:                         m.ID,
		Name:                       m.Name,
		DisplayName:                m.Name,
		Description:                desc,
		OwnedBy:                    providerName,
		InputTokenLimit:            m.MaxInputTokens,
		MaxCompletionTokens:        m.MaxOutputTokens,
		ContextLength:              m.MaxAllowedSize,
		SupportedGenerationMethods: []string{"chat"},
		Rate:                       parseCreditsRate(m.Credits),
	}
	if m.SupportsImages {
		info.SupportedInputModalities = []string{"text", "image"}
	} else {
		info.SupportedInputModalities = []string{"text"}
	}
	if m.SupportsReasoning || m.OnlyReasoning {
		info.Thinking = &pluginapi.ThinkingSupport{}
	}
	return info
}
