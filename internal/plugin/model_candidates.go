package main

// model-candidates：`model_selection` 配置字段背后的只读端点。
//
// 面板自己算不出候选清单：它是上游目录，需要凭据与一次网络往返。把它放在
// 插件里，既把这份知识留在插件内，也让管理端渲染勾选清单，而不是要求运维
// 凭记忆手打上游模型 id。
//
// 这个端点**从不刷新**目录：`/refresh` 是一次写操作（替换缓存并让宿主重新
// 注册），打开配置对话框不该产生副作用。缓存冷时返回空清单并带 `discovered:
// false`，对话框据此提示先点「刷新目录」。

import (
	"context"
	"errors"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// modelCandidateModels 把上游目录转成勾选清单的候选项。
//
// 候选必须与注册结果同口径：被排除策略剔除的模型即使出现在上游目录里，也不会
// 被注册到网关。若不剔除，操作员会勾上一个永远不生效的模型却得不到任何提示。
func modelCandidateModels(models []pluginapi.ModelInfo) []modelCandidate {
	return buildModelCandidates(applyModelPolicy(models))
}

// modelCandidatesJSON 响应 GET /v0/management/plugins/workbuddy/model-candidates。
//
// 候选来自「当前账号的目录」：workbuddy 的模型列表按 realm 与账号等级不同，
// 所以这里按 auth_index 选一个账号取目录；未指定时用当前选中的账号。
func modelCandidatesJSON(req pluginapi.ManagementRequest) modelCandidatesResponse {
	authIndex := req.Query.Get("auth_index")
	models, errText := catalogueForCandidates(authIndex)
	// 候选清单必须与注册结果同口径：被排除策略剔除的模型即使出现在上游目录里，
	// 也不会被注册到网关。若不剔除，操作员勾了一个永远不生效的模型却得不到
	// 任何提示。
	return modelCandidatesResponse{
		Provider:   providerName,
		Models:     modelCandidateModels(models),
		Discovered: errText == "",
		Error:      errText,
	}
}

// catalogueForCandidates 取某个账号的目录。
//
// 缓存命中就直接用；未命中时**拉一次只读 GET**，只写插件内部缓存，
// 不调用 catalogueRefresh（后者会发布给宿主并触发重新注册，有副作用）。
// 两者必须区分：打开配置对话框拉一份目录是只读的，而「刷新目录」按钮是写操作。
func catalogueForCandidates(authIndex string) ([]pluginapi.ModelInfo, string) {
	storage, errStorage := storageJSONForAuth(authIndex)
	if errStorage != nil {
		return nil, errStorage.Error()
	}
	sa, errParse := parseStored(storage)
	if errParse != nil {
		return nil, "无法解析凭据：" + errParse.Error()
	}
	realm := accountRegion(sa)
	userType := "free"
	if realm == "global" {
		userType = accountUserType(sa)
	}
	key := realm + ":" + userType
	if cached, ok := cachedDynamicModels(key); ok {
		return cached, ""
	}
	if realm == "global" {
		if fetched, errFetch := fetchGlobalModels(context.Background(), sa); errFetch == nil && len(fetched) > 0 {
			storeDynamicModels(key, fetched)
			return fetched, ""
		}
	} else {
		if fetched, errFetch := fetchCNModels(context.Background(), sa); errFetch == nil && len(fetched) > 0 {
			storeDynamicModels(key, fetched)
			return fetched, ""
		}
	}
	return nil, "拉取模型目录失败，请先在面板点「刷新目录」或检查账号凭据"
}

// storageJSONForAuth 返回指定账号的 storage JSON。
//
// authIndex 可能是两种东西：宿主给的不透明 auth_index，或面板选中时存下的
// 文件名。两者都要认——getActiveAuthID() 保存的是面板传过来的标识，历史上
// 既存过 name 也存过 auth_index。所以这里先在列表里按 AuthIndex 或 Name 匹配，
// 匹配不到再当作 auth_index 直接问宿主。
func storageJSONForAuth(authIndex string) ([]byte, error) {
	if authIndex == "" {
		authIndex = getActiveAuthID()
	}
	entries, errList := hostAuthList()
	if errList == nil && len(entries) > 0 {
		resolved := ""
		for _, entry := range entries {
			if entry.Disabled {
				continue
			}
			if authIndex != "" && (entry.AuthIndex == authIndex || entry.Name == authIndex) {
				resolved = entry.AuthIndex
				break
			}
		}
		// 没有指定或指定值匹不上：用第一个未禁用账号，让对话框总能渲染。
		if resolved == "" {
			for _, entry := range entries {
				if entry.Disabled {
					continue
				}
				if index := strings.TrimSpace(entry.AuthIndex); index != "" {
					resolved = index
					break
				}
			}
		}
		if resolved != "" {
			authIndex = resolved
		}
	}
	if authIndex == "" {
		return nil, errors.New("还没有可用的账号，请先登录")
	}
	phys, err := hostAuthGetPhysical(authIndex)
	if err != nil {
		return nil, err
	}
	return phys.JSON, nil
}
