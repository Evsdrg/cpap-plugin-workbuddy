package main

// 国际版（Global）的模型发现。
//
// 两个 realm 的权威端点不同，这是 2026-10-19 的实测结论：
//
//	Global  GET https://www.workbuddy.ai/v3/config
//	        → models 27 个、modelTiers 1 组（gpt-pro-only，12 个模型）、
//	          agents[cli] 27 个（等于全部模型）
//	CN      GET https://www.workbuddy.cn/console/enterprises/personal/models
//	        → models 33 个、agents[cli] 18 个、无 modelTiers
//
// 两个端点不能互串：Global 的 /console/enterprises/personal/models 恒返回
// APISIX 500；CN 的 /v3/config 只下发 37 个陈旧模型（最新到 glm-5.1，没有
// glm-5.3 / deepseek-v4.1-flash / step-5-preview）。
//
// 官方客户端的 ModelsService.getTeamsModels() 只调 /console/enterprises/{id}/models
// 这一支，而 /v3/config 是 CloudProductProvider 的 product 配置加载路径——
// 国际版客户端的模型选择器最终读的是 product 配置里的 models，两者在
// 客户端里并存，服务端只在国际集群上开了 /v3/config 这一路。

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// abstractModelIDs 的定义已移到 models_policy.go，与另外两条注册规则集中在一处。

// userTypeRank 来自官方客户端 renderer 的账号等级表：
//
//	$At = {trial:1, standard:2, advanced:3, flagship:4}
//	ejt = {free:1, youth:1, standard:2, advanced:3, flagship:4}
//
// 客户端用 resolveModelTierAccess 比较「账号等级」与「模型 requiredUserType」，
// 账号等级不足时给模型打上 Pro Only 徽章。
// workBuddyClientUA 是 WorkBuddy 桌面客户端在 /v3/config 上使用的 UA。
// 服务端用它区分「桌面客户端」与「CLI」两种下发形态，见 fetchGlobalProductConfig。
const workBuddyClientUA = desktopUA

var userTypeRank = map[string]int{
	"free":     1,
	"youth":    1,
	"standard": 2,
	"advanced": 3,
	"flagship": 4,
}

// globalModelWire 是 /v3/config 里单个模型的线上形态。
type globalModelWire struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	DescriptionZh string   `json:"descriptionZh"`
	DescriptionEn string   `json:"descriptionEn"`
	Credits       string   `json:"credits"`
	Tags          []string `json:"tags"`
	// VideoGeneration 非空表示这是一个视频生成模型（上游只在视频模型上
	// 下发这一块）。它没有 maxInputTokens/maxOutputTokens，也无法按 chat
	// 补全协议调用，注册出去只会让客户端选中后拿到 400。
	VideoGeneration   json.RawMessage `json:"videoGeneration"`
	MaxInputTokens    int64           `json:"maxInputTokens"`
	MaxOutputTokens   int64           `json:"maxOutputTokens"`
	MaxAllowedSize    int64           `json:"maxAllowedSize"`
	SupportsImages    bool            `json:"supportsImages"`
	SupportsReasoning bool            `json:"supportsReasoning"`
	OnlyReasoning     bool            `json:"onlyReasoning"`
	IsDefault         bool            `json:"isDefault"`
	Disabled          bool            `json:"disabled"`
}

// mediaModelTags 是目录里专用于图像/视频生成的标签。
//
// 插件只服务 chat 补全，不代理图像与视频生成，因此带这些标签的条目一律
// 不注册。注意不能用「是否支持图像输入」判断——supportsImages 表示能读
// 图片，恰恰是对话模型的能力，与图像生成无关。
var mediaModelTags = map[string]struct{}{
	"text-to-image":  {},
	"image-to-image": {},
	"text-to-video":  {},
	"image-to-video": {},
}

// isMediaGenerationModel 报告一个模型条目是否属于纯图像/视频生成能力。
func isMediaGenerationModel(tags []string, videoGeneration json.RawMessage) bool {
	for _, tag := range tags {
		if _, ok := mediaModelTags[strings.ToLower(strings.TrimSpace(tag))]; ok {
			return true
		}
	}
	if len(bytes.TrimSpace(videoGeneration)) > 0 && string(bytes.TrimSpace(videoGeneration)) != "null" {
		return true
	}
	return false
}

// globalProductConfig 是 /v3/config 里本插件用到的部分。
type globalProductConfig struct {
	Models     []globalModelWire `json:"models"`
	ModelTiers []modelTier       `json:"modelTiers"`
}

// modelTier 是 /v3/config 下发的模型分层定义。服务端对所有账号下发同一份
// 分层表，由客户端按账号等级本地判定——所以插件也必须自己拿到账号等级。
type modelTier struct {
	ID               string   `json:"id"`
	Tier             string   `json:"tier"`
	RequiredUserType string   `json:"requiredUserType"`
	AccessMode       string   `json:"accessMode"`
	ModelIDs         []string `json:"modelIds"`
}

// fetchGlobalModels 走 /v3/config 拉国际版模型目录，并按账号等级过滤。
func fetchGlobalModels(ctx context.Context, sa *storedAuth) ([]pluginapi.ModelInfo, error) {
	cfg, err := fetchGlobalProductConfig(ctx, sa)
	if err != nil {
		return nil, err
	}
	userType := accountUserType(sa)
	// 抽象档位、过旧模型、MiniMax 三条注册规则统一在 models_policy.go 里执行。
	return applyModelPolicy(buildGlobalModels(cfg, userType)), nil
}

// fetchGlobalProductConfig 拉取并解析 /v3/config。
func fetchGlobalProductConfig(ctx context.Context, sa *storedAuth) (*globalProductConfig, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, billingBaseFor(sa)+"/v3/config", nil)
	if err != nil {
		return nil, err
	}
	// /v3/config 是桌面端产品配置接口，用桌面端身份。
	desktopTransportHeaders(req)
	req.Header.Set("Authorization", "Bearer "+sa.Auth.AccessToken)
	req.Header.Set("Accept", "application/json")
	// 必须用 WorkBuddy 桌面客户端的 UA，不能用 CLI 身份。
	// 服务端按 UA 分流：CLI UA 只下发 20 个模型且完全不带 modelTiers，
	// 于是 Pro Only 过滤会失效、5 个付费模型被白送给免费账号；WorkBuddy UA
	// 才下发完整的 27 个模型与 modelTiers 分层表。实测于 2026-10-19。
	req.Header.Set("User-Agent", workBuddyClientUA)
	resp, err := hostHTTPDo(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("/v3/config status %d", resp.StatusCode)
	}
	var env struct {
		Code int                  `json:"code"`
		Msg  string               `json:"msg"`
		Data *globalProductConfig `json:"data"`
	}
	if err := json.Unmarshal(resp.Body, &env); err != nil {
		return nil, fmt.Errorf("/v3/config parse: %w", err)
	}
	if env.Code != 0 || env.Data == nil {
		return nil, fmt.Errorf("/v3/config code=%d msg=%s", env.Code, env.Msg)
	}
	return env.Data, nil
}

// accountPackageTier 把官方客户端的套餐代码映射到账号等级。
//
// 代码表来自客户端 main/account.js 的 $ 枚举：
//
//	free      TCACA_code_001_PqouKr6QWV
//	proMon    TCACA_code_002_AkiJS3ZHF5   -> standard
//	proYear   TCACA_code_003_FAnt7lcmRT   -> standard
//	proMonPlus TCACA_code_005_maRGyrHhw1  -> standard
//	freeMon   TCACA_code_008_cfWoLwvjU4   -> free
//	youth     TCACA_code_023_4xbGhMrE6q
//	advanced  TCACA_code_026_BaESVICNoi
//	flagship  TCACA_code_027_0FCGVA6vSa
//	freeMonIntl TCACA_code_035_ArVxJcGDsm -> free
//	proTrialMon / proTrialYear           -> standard
//
// 赠送包与活动包（gift / activity / extra / bonus*）不表达订阅等级，
// 因此不在此表内；只持有它们的账号按最低等级处理。
var accountPackageTier = map[string]string{
	"TCACA_code_001_PqouKr6QWV": "free",
	"TCACA_code_002_AkiJS3ZHF5": "standard",
	"TCACA_code_003_FAnt7lcmRT": "standard",
	"TCACA_code_005_maRGyrHhw1": "standard",
	"TCACA_code_008_cfWoLwvjU4": "free",
	"TCACA_code_023_4xbGhMrE6q": "youth",
	"TCACA_code_026_BaESVICNoi": "advanced",
	"TCACA_code_027_0FCGVA6vSa": "flagship",
	"TCACA_code_035_ArVxJcGDsm": "free",
	"TCACA_code_039_KRcQj7wUat": "standard",
	"TCACA_code_040_mi9rCYg46x": "standard",
}

// accountUserType 返回账号等级，取值与客户端 ejt 表一致。
//
// 等级来源是 billing 资源包里的 PackageCode，而不是已下线的
// /v2/billing/meter/get-user-resource-summary（实测 5 个账号全部 404）。
// /v2/billing/meter/get-payment-type 只有 free/paid 两档，表达不了客户端
// 那套 free/youth/standard/advanced/flagship 五档，因此也不用它。
//
// 账号可能同时持有多个包（体验版 + 赠送包 + 订阅包），取其中最高的等级。
// 查询失败时按 free 处理：宁可少给模型，也不要把 Pro Only 模型暴露给
// 免费账号——那会让调用在上游被拒，错误更难解释。
func accountUserType(sa *storedAuth) string {
	codes, err := fetchAccountPackageCodes(sa)
	if err != nil {
		return "free"
	}
	best := ""
	bestRank := 0
	for _, code := range codes {
		tier, ok := accountPackageTier[strings.TrimSpace(code)]
		if !ok {
			continue
		}
		if rank := userTypeRank[tier]; rank > bestRank {
			best = tier
			bestRank = rank
		}
	}
	if best == "" {
		return "free"
	}
	return best
}

// fetchAccountPackageCodes 返回账号当前有效资源包的 PackageCode 列表。
//
// 复用 credits 模块同一个端点与同一套分页/状态参数，避免两套口径。
func fetchAccountPackageCodes(sa *storedAuth) ([]string, error) {
	now := time.Now()
	body := map[string]any{
		"PageNumber":               1,
		"PageSize":                 100,
		"ProductCode":              "p_tcaca",
		"Status":                   []int{0, 3},
		"PackageEndTimeRangeBegin": now.Format("2006-01-02 15:04:05"),
		"PackageEndTimeRangeEnd":   now.Add(365 * 101 * 24 * time.Hour).Format("2006-01-02 15:04:05"),
	}
	data, err := billingCall(sa, "/v2/billing/meter/get-user-resource", body)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Response struct {
			Data struct {
				Accounts []struct {
					PackageCode string `json:"PackageCode"`
				} `json:"Accounts"`
			} `json:"Data"`
		} `json:"Response"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	codes := make([]string, 0, len(resp.Response.Data.Accounts))
	for _, item := range resp.Response.Data.Accounts {
		if code := strings.TrimSpace(item.PackageCode); code != "" {
			codes = append(codes, code)
		}
	}
	return codes, nil
}

// buildGlobalModels 把 /v3/config 的模型表转成 pluginapi.ModelInfo，
// 剔除抽象名与当前账号等级不满足 requiredUserType 的模型。
func buildGlobalModels(cfg *globalProductConfig, userType string) []pluginapi.ModelInfo {
	rank, ok := userTypeRank[userType]
	if !ok {
		rank = userTypeRank["free"]
	}
	// 账号等级不足的模型集合。
	blocked := make(map[string]struct{})
	for _, t := range cfg.ModelTiers {
		need, known := userTypeRank[t.RequiredUserType]
		if !known {
			// 未声明或拼写未知时按 standard 处理，与客户端
			// resolveModelTierAccess 的默认值一致。
			need = userTypeRank["standard"]
		}
		if rank >= need {
			continue
		}
		for _, id := range t.ModelIDs {
			blocked[id] = struct{}{}
		}
	}

	out := make([]pluginapi.ModelInfo, 0, len(cfg.Models))
	for _, m := range cfg.Models {
		if m.ID == "" || m.Disabled {
			continue
		}
		// 纯图像/视频生成模型不注册：本插件只服务 chat 补全。
		// 兜底目录 globalFallbackModels 早已做过同样的剔除，主路径
		// 此前漏了这一步，会把 hy-image-3.5 / kling-video-v3 注册出去。
		if isMediaGenerationModel(m.Tags, m.VideoGeneration) {
			continue
		}
		if _, deny := blocked[m.ID]; deny {
			continue
		}
		out = append(out, globalModelInfo(m))
	}
	return out
}

// globalModelInfo 做 /v3/config 模型到 pluginapi.ModelInfo 的字段映射。
func globalModelInfo(m globalModelWire) pluginapi.ModelInfo {
	desc := strings.TrimSpace(m.DescriptionEn)
	if desc == "" {
		desc = strings.TrimSpace(m.DescriptionZh)
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

// globalFallbackModels 是 /v3/config 不可用时的兜底目录。
//
// 来源：国际版 WorkBuddy 客户端 app.asar 内的 cli/product.json（30 个模型）。
// 已剔除 5 个抽象名（见 abstractModelIDs）与 8 个纯媒体模型
// （gpt-image-2.5-sunburst / seedance-2.5 / gemini-3.0-pro-image /
// gemini-3.1-flash-image / gemini-2.5-flash-image / hunyuan-image-v3.0 /
// hunyuan-image-v2.0-general-edit / hunyuan-video-art），
// 因为本插件只服务 chat 补全，不代理图像与视频生成。
//
// 兜底路径拿不到账号等级，故不做 Pro Only 过滤——宁可让上游去拒绝，
// 也不要在一个本就已经降级的路径上再悄悄少给模型。
//
// 这份表本身也不再列过旧模型（glm-5.2、kimi-k2.6），规则与实时路径一致。
var globalFallbackIDs = []string{
	"deepseek-v4.1-flash",
	"deepseek-v4.1-flash-sg",
	"gpt-6-astra",
	"hy4-preview",
	"hy3",
	"kimi-k2.8-preview",
	"gpt-5.6-sol",
	"gpt-5.6-terra",
	"gpt-5.6-luna",
	"gpt-5.5",
	"gpt-5.4",
	"gemini-3.5-flash",
	"glm-5.3-flash",
	"glm-5.3",
	"kimi-k3",
}

// globalFallbackModels 把兜底 ID 表展开成 ModelInfo。
func globalFallbackModels() []pluginapi.ModelInfo {
	out := make([]pluginapi.ModelInfo, 0, len(globalFallbackIDs))
	for _, id := range globalFallbackIDs {
		out = append(out, pluginapi.ModelInfo{
			ID:                         id,
			Name:                       id,
			DisplayName:                id,
			OwnedBy:                    providerName,
			SupportedGenerationMethods: []string{"chat"},
			SupportedInputModalities:   []string{"text"},
		})
	}
	return applyModelPolicy(out)
}
