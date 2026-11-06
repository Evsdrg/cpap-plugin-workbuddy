package main

// 模型注册策略：从内嵌的 models_excluded.json 读取排除清单。
//
// 两个 realm 的目录端点都会下发一批「能列出来但不该注册」的模型。排除清单
// 写成数据文件而不是代码里的正则推导——要剔除的是**已知的具体模型**，不是
// 某个语法模式。用正则等于在猜上游以后会起什么名字，一旦上游出现
// glm-5.30 或 kimi-k2.60 这类新写法就会误判；清单的行为完全可预测，改动也
// 只需要往 JSON 表里加一行，不必重新理解代码。
//
// 清单分三组：
//
//	abstract —— 抽象档位（fast-model / balanced-model / deep-model 等）。
//	            这些名字是服务端按账号等级与实时负载动态选模型的路由别名，
//	            客户端与服务端都不暴露它本次落到哪个具体模型
//	            （AUTO_TIER_MODELS 只有价格档位，vendor 字段在本地
//	            product.json 与服务端 /v3/config 之间还不一致），注册它们会
//	            让插件侧的限流、路由与用量统计落在一个无法解释的名字上。
//	legacy   —— 过旧模型。上游仍下发是为了旧客户端兼容，但新会话没有理由再用。
//	minimax  —— MiniMax 系列。
//
// 匹配口径：注册名转小写后精确匹配，不做前缀、通配或版本号推导。清单外的
// 模型一律保留：宁可多给一个，也不要把新模型误判成旧模型删掉。

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

//go:embed assets/models_excluded.json
var modelsExcludedJSON []byte

// modelExclusionGroup 是清单里的一组排除项。
type modelExclusionGroup struct {
	Comment []string `json:"_comment"`
	IDs     []string `json:"ids"`
}

// modelExclusionFile 是 models_excluded.json 的整体结构。
type modelExclusionFile struct {
	Comment  []string            `json:"_comment"`
	Abstract modelExclusionGroup `json:"abstract"`
	Legacy   modelExclusionGroup `json:"legacy"`
	MiniMax  modelExclusionGroup `json:"minimax"`
	Product  modelExclusionGroup `json:"product"`
}

// exclusionGroups 在首次使用时解析内嵌清单，之后复用。
var (
	exclusionOnce   sync.Once
	exclusionSets   []map[string]struct{}
	exclusionErr    error
	exclusionParsed modelExclusionFile
)

// loadModelExclusions 解析内嵌清单并编译成查表集合。
func loadModelExclusions() ([]map[string]struct{}, modelExclusionFile, error) {
	exclusionOnce.Do(func() {
		if err := json.Unmarshal(modelsExcludedJSON, &exclusionParsed); err != nil {
			exclusionErr = fmt.Errorf("models_excluded.json: %w", err)
			return
		}
		groups := [][]string{
			exclusionParsed.Abstract.IDs,
			exclusionParsed.Legacy.IDs,
			exclusionParsed.MiniMax.IDs,
			exclusionParsed.Product.IDs,
		}
		sets := make([]map[string]struct{}, 0, len(groups))
		for _, ids := range groups {
			set := make(map[string]struct{}, len(ids))
			for _, id := range ids {
				set[normalizeModelID(id)] = struct{}{}
			}
			sets = append(sets, set)
		}
		exclusionSets = sets
	})
	return exclusionSets, exclusionParsed, exclusionErr
}

// normalizeModelID 统一注册名的比较口径。
//
// 上游目录里同一模型出现过大小写两种写法（GLM-5.2 与 glm-5.3 分属不同端点），
// 所以比较前统一转小写并去掉首尾空白。
func normalizeModelID(id string) string {
	return strings.ToLower(strings.TrimSpace(id))
}

// applyModelPolicy 按排除清单过滤模型列表，保持原有顺序。
func applyModelPolicy(models []pluginapi.ModelInfo) []pluginapi.ModelInfo {
	sets, _, err := loadModelExclusions()
	if err != nil {
		// 清单解析失败时不做任何剔除：注册策略是「少给」的优化，不能因为
		// 数据文件坏了就把整个 provider 的模型全部清空。
		return models
	}
	out := make([]pluginapi.ModelInfo, 0, len(models))
	for _, m := range models {
		if excludedBySets(sets, m.ID) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// excludedBySets 判断某个注册名是否落在任一排除组里。
func excludedBySets(sets []map[string]struct{}, id string) bool {
	key := normalizeModelID(id)
	if key == "" {
		return true
	}
	for _, set := range sets {
		if _, deny := set[key]; deny {
			return true
		}
	}
	return false
}

// excludedByModelPolicy 供测试与面板查询单个模型。
func excludedByModelPolicy(id string) bool {
	sets, _, err := loadModelExclusions()
	if err != nil {
		return false
	}
	return excludedBySets(sets, id)
}

// excludedModelIDs 返回全部被排除的注册名，供测试与面板展示使用。
func excludedModelIDs() []string {
	_, parsed, err := loadModelExclusions()
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(parsed.Abstract.IDs)+len(parsed.Legacy.IDs)+len(parsed.MiniMax.IDs))
	out = append(out, parsed.Abstract.IDs...)
	out = append(out, parsed.Legacy.IDs...)
	out = append(out, parsed.MiniMax.IDs...)
	return out
}
