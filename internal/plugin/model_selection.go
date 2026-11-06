package main

// model_selection：面板勾选「最终决定」注册哪些模型。
//
// 与 trae 的同名机制对齐，但语义更强：workbuddy 的动态目录会随账号等级与
// realm 变化（Global 免费 11 / 付费 15、CN 20），而这里的选择是**最终决定**——
// 只要配置了就不再加回目录里的其它模型。未配置（nil）时才回落到目录全量。
//
// 为什么不是「排除列表」：排除列表在上游新增模型时会把新模型自动放进来，
// 而运维真正想要的是「我点过的才上线」，所以白名单语义更贴合。

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// modelSelectionEntry 是面板里的一行：上游模型 id + 可选的自定义注册名。
type modelSelectionEntry struct {
	// Upstream 是上游目录里的模型 id（大小写敏感，必须逐字节保留）。
	Upstream string `json:"upstream"`
	// Name 是注册到网关、客户端请求时使用的名字；留空则回落到 Upstream。
	Name string `json:"name,omitempty"`
}

var (
	modelSelectionMu sync.RWMutex
	// modelSelection 为 nil 表示「未配置」——此时注册目录全量。
	// 非 nil 的零长度切片表示「配置了但一个都没勾」——此时一个都不注册。
	// 这两种情况必须区分：合并它们会让运维清空勾选后误以为插件坏了。
	modelSelection []modelSelectionEntry
)

// applyModelSelection 由 configure() 在每次 reconfigure 时调用。
func applyModelSelection(entries []modelSelectionEntry) {
	modelSelectionMu.Lock()
	modelSelection = entries
	modelSelectionMu.Unlock()
}

// selectedModels 返回当前配置；nil 表示未配置。
func selectedModels() []modelSelectionEntry {
	modelSelectionMu.RLock()
	defer modelSelectionMu.RUnlock()
	return modelSelection
}

// selectionConfigured 报告运维是否给出过显式列表。
func selectionConfigured() bool {
	return selectedModels() != nil
}

// registeredNameFor 返回某个上游模型在网关上的注册名。
//
// 第二个返回值表示该模型是否被选中。未配置时所有模型都算选中，注册名沿用
// 上游 id（workbuddy 的上游 id 本身就是小写的，不需要像 trae 那样派生）。
func registeredNameFor(upstream string) (string, bool) {
	entries := selectedModels()
	if entries == nil {
		return upstream, true
	}
	for _, entry := range entries {
		if entry.Upstream != upstream {
			continue
		}
		if name := strings.TrimSpace(entry.Name); name != "" {
			return name, true
		}
		return upstream, true
	}
	return "", false
}

// upstreamForRegistered 把客户端请求的注册名反解回上游 id。
//
// 这一步是承重的：客户端发来的是注册名，上游只认原始 id。找不到时原样返回，
// 让上游用自己的错误语义拒绝，而不是在插件侧编造一个。
func upstreamForRegistered(registered string) string {
	entries := selectedModels()
	if entries == nil {
		return registered
	}
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			name = entry.Upstream
		}
		if name == registered {
			return entry.Upstream
		}
	}
	return registered
}

// filterByModelSelection 把目录过滤到已勾选的模型，并把注册名写进 ModelInfo。
//
// ModelInfo 有 ID 与 Name 两个字段：ID 是客户端请求用的名字，Name 是上游原生名。
// 自定义注册名时两者会不同，这里必须同时设置，否则宿主按 upstream 名字注册、
// 客户端用注册名请求就对不上。
func filterByModelSelection(models []pluginapi.ModelInfo) []pluginapi.ModelInfo {
	entries := selectedModels()
	if entries == nil {
		return models
	}
	byUpstream := make(map[string]string, len(entries))
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			name = entry.Upstream
		}
		byUpstream[entry.Upstream] = name
	}
	out := make([]pluginapi.ModelInfo, 0, len(models))
	for _, model := range models {
		name, ok := byUpstream[model.ID]
		if !ok {
			continue
		}
		model.Name = model.ID
		model.ID = name
		out = append(out, model)
	}
	// 按勾选顺序输出，让面板里的顺序与网关上的顺序一致；未在目录里出现的
	// 勾选项（上游下架了）不会凭空造出来。
	order := make(map[string]int, len(entries))
	for i, entry := range entries {
		order[entry.Upstream] = i
	}
	sort.SliceStable(out, func(i, j int) bool {
		return order[out[i].Name] < order[out[j].Name]
	})
	return out
}

// modelCandidate 是面板勾选器的一行。
type modelCandidate struct {
	Upstream string  `json:"upstream"`
	Name     string  `json:"name"`
	Display  string  `json:"display,omitempty"`
	Selected bool    `json:"selected"`
	Rate     float64 `json:"rate,omitempty"`
	Context  int64   `json:"context,omitempty"`
}

// modelCandidatesResponse 是候选端点的响应。
type modelCandidatesResponse struct {
	Provider   string           `json:"provider"`
	Models     []modelCandidate `json:"models"`
	Discovered bool             `json:"discovered"`
	Error      string           `json:"error,omitempty"`
}

// buildModelCandidates 依据当前配置与一份目录渲染勾选清单。
//
// 已勾选的排在前面：运维打开对话框时最关心的是「我现在注册了什么」。
func buildModelCandidates(models []pluginapi.ModelInfo) []modelCandidate {
	out := make([]modelCandidate, 0, len(models))
	for _, model := range models {
		name, selected := registeredNameFor(model.ID)
		if name == "" {
			name = model.ID
		}
		out = append(out, modelCandidate{
			Upstream: model.ID,
			Name:     name,
			Display:  firstNonEmpty(model.DisplayName, model.Name, model.ID),
			Selected: selected,
			// 倍率必须读 model.Rate（目录构造时已填）。不要读 modelRateFor：
			// 那个 sync.Map 的生产代码从未写入，恒返回 0，勾选器上永远看不到倍率。
			Rate:    model.Rate,
			Context: model.ContextLength,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Selected != out[j].Selected {
			return out[i].Selected
		}
		left, right := strings.ToLower(out[i].Display), strings.ToLower(out[j].Display)
		if left != right {
			return left < right
		}
		return out[i].Upstream < out[j].Upstream
	})
	return out
}

// coerceModelSelection 解析 config_yaml 里的 model_selection 数组。
//
// 返回 nil 表示「键不存在」；返回空切片表示「键存在但为空」。两者的语义差别
// 见 modelSelection 的注释，不能合并。
func coerceModelSelection(value any) []modelSelectionEntry {
	if value == nil {
		return nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]modelSelectionEntry, 0, len(items))
	for _, item := range items {
		mapping, okMap := item.(map[string]any)
		if !okMap {
			continue
		}
		upstream := strings.TrimSpace(coerceStringValue(mapping["upstream"]))
		if upstream == "" {
			continue
		}
		out = append(out, modelSelectionEntry{
			Upstream: upstream,
			Name:     strings.TrimSpace(coerceStringValue(mapping["name"])),
		})
	}
	return out
}

// coerceStringValue 把 YAML 标量折成字符串；非标量返回空串。
func coerceStringValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

// marshalModelSelection 把当前选择序列化回 config_yaml 形态，供管理 API 回显。
func marshalModelSelection(entries []modelSelectionEntry) []map[string]string {
	if entries == nil {
		return nil
	}
	out := make([]map[string]string, 0, len(entries))
	for _, entry := range entries {
		row := map[string]string{"upstream": entry.Upstream}
		if entry.Name != "" {
			row["name"] = entry.Name
		}
		out = append(out, row)
	}
	return out
}
