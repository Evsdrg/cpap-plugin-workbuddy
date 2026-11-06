package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// handleCooldownsList 实现统一插件冷却查询契约的 WorkBuddy 适配。
// 只返回传入 auth_index 对应账号仍有效的冷却，避免把账号标识暴露给无关调用方。
func handleCooldownsList(req pluginapi.ManagementRequest) pluginapi.ManagementResponse {
	authIndex := strings.TrimSpace(req.Query.Get("auth_index"))
	if authIndex == "" {
		return mgmtJSONResponse(http.StatusBadRequest, map[string]any{"error": "auth_index is required"})
	}
	files, err := hostAuthList()
	if err != nil {
		return mgmtJSONResponse(http.StatusBadGateway, map[string]any{"error": "列出账号失败"})
	}
	var file *pluginapi.HostAuthFileEntry
	for i := range files {
		if files[i].AuthIndex == authIndex {
			file = &files[i]
			break
		}
	}
	if file == nil {
		return mgmtJSONResponse(http.StatusNotFound, map[string]any{"error": "auth not found"})
	}
	identities := cooldownIdentities(*file)
	if stored, err := hostAuthGet(authIndex); err == nil && stored != nil {
		identities = appendCooldownIdentity(identities, stored.Account.UID)
	}
	entries := snapshotWorkbuddyCooldowns(authIndex, identities, time.Now())
	return mgmtJSONResponse(http.StatusOK, pluginapi.PluginCooldownListResponse{Cooldowns: entries})
}

// handleCooldownReset clears one model cooldown, or all cooldowns for the account
// when model is empty. The host and WorkBuddy maintain separate routing state, so
// both layers are reset before the handler reports success.
func handleCooldownReset(req pluginapi.ManagementRequest) pluginapi.ManagementResponse {
	var body pluginapi.PluginCooldownResetRequest
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return mgmtJSONResponse(http.StatusBadRequest, map[string]any{"error": "invalid request body"})
	}
	body.AuthIndex = strings.TrimSpace(body.AuthIndex)
	body.Model = strings.TrimSpace(body.Model)
	if body.AuthIndex == "" {
		return mgmtJSONResponse(http.StatusBadRequest, map[string]any{"error": "auth_index is required"})
	}
	files, err := hostAuthList()
	if err != nil {
		return mgmtJSONResponse(http.StatusBadGateway, map[string]any{"error": "列出账号失败"})
	}
	var file *pluginapi.HostAuthFileEntry
	for i := range files {
		if files[i].AuthIndex == body.AuthIndex {
			file = &files[i]
			break
		}
	}
	if file == nil {
		return mgmtJSONResponse(http.StatusNotFound, map[string]any{"error": "auth not found"})
	}
	identities := cooldownIdentities(*file)
	if stored, err := hostAuthGet(body.AuthIndex); err == nil && stored != nil {
		identities = appendCooldownIdentity(identities, stored.Account.UID)
	}

	if body.Model == "" {
		var hostResp pluginapi.HostRoutingResetCooldownResponse
		if err := abi.HostCallInto(pluginabi.MethodHostRoutingResetCooldown, pluginapi.HostRoutingResetCooldownRequest{AuthIndex: body.AuthIndex}, &hostResp); err != nil {
			return mgmtJSONResponse(http.StatusBadGateway, map[string]any{"error": "清除宿主账号冷却失败"})
		}
		pluginCleared := resetCooldownsForIdentities(identities) > 0
		return mgmtJSONResponse(http.StatusOK, pluginapi.PluginCooldownResetResponse{
			AuthIndex: body.AuthIndex,
			Cleared:   len(hostResp.Models) > 0 || pluginCleared,
		})
	}

	var hostResp pluginapi.HostRoutingResetModelCooldownResponse
	if err := abi.HostCallInto(pluginabi.MethodHostRoutingResetModelCooldown, pluginapi.HostRoutingResetModelCooldownRequest{
		AuthIndex: body.AuthIndex,
		Model:     body.Model,
	}, &hostResp); err != nil {
		return mgmtJSONResponse(http.StatusBadGateway, map[string]any{"error": "清除宿主模型冷却失败"})
	}
	pluginCleared := false
	for _, identity := range identities {
		if resetModelCooldown(identity, body.Model) {
			pluginCleared = true
		}
		resetEmptyStreamStrikes(identity, body.Model)
	}
	return mgmtJSONResponse(http.StatusOK, pluginapi.PluginCooldownResetResponse{
		AuthIndex: body.AuthIndex,
		Model:     strings.ToLower(body.Model),
		Cleared:   hostResp.Cleared || pluginCleared,
	})
}

func cooldownIdentities(file pluginapi.HostAuthFileEntry) []string {
	var identities []string
	identities = appendCooldownIdentity(identities, file.AuthIndex)
	identities = appendCooldownIdentity(identities, file.ID)
	// 旧版单文件名会被所有账号共用，不能作为冷却身份；否则查询/清除一个账号
	// 会误读或删除另一个账号的内部冷却。
	if !isLegacyWorkbuddyAuthName(file.Name) {
		identities = appendCooldownIdentity(identities, file.Name)
	}
	return identities
}

func appendCooldownIdentity(identities []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return identities
	}
	for _, existing := range identities {
		if existing == value {
			return identities
		}
	}
	return append(identities, value)
}

func snapshotWorkbuddyCooldowns(authIndex string, identities []string, now time.Time) []pluginapi.PluginCooldown {
	type modelKey struct {
		scope string
		model string
	}
	type cooldownSnapshot struct {
		entry pluginapi.PluginCooldown
		rank  int
	}
	byKey := make(map[modelKey]cooldownSnapshot)
	cooldownMu.Lock()
	defer cooldownMu.Unlock()
	for key, state := range cooldowns {
		var scope, model string
		for _, identity := range identities {
			if key == identity {
				scope = "account"
				break
			}
			prefix := "model:" + identity + ":"
			if strings.HasPrefix(key, prefix) {
				scope = "model"
				model = strings.TrimPrefix(key, prefix)
				break
			}
		}
		if scope == "" || !state.NextRetryAt.After(now) {
			continue
		}
		entry := pluginapi.PluginCooldown{
			AuthIndex: authIndex,
			Scope:     scope,
			Model:     model,
			Reason:    workbuddyCooldownReason(state.Reason),
			RetryAt:   state.NextRetryAt.UTC(),
			Source:    "workbuddy",
		}
		if !state.ProbeAt.IsZero() {
			probeAt := state.ProbeAt.UTC()
			entry.ProbeAt = &probeAt
		}
		remaining := state.NextRetryAt.Sub(now)
		entry.RemainingSeconds = int64(remaining / time.Second)
		if remaining%time.Second != 0 {
			entry.RemainingSeconds++
		}
		key := modelKey{scope: scope, model: model}
		old, ok := byKey[key]
		if !ok || entry.RetryAt.After(old.entry.RetryAt) || (entry.RetryAt.Equal(old.entry.RetryAt) && cooldownReasonRank(state.Reason) > old.rank) {
			byKey[key] = cooldownSnapshot{entry: entry, rank: cooldownReasonRank(state.Reason)}
		}
	}
	out := make([]pluginapi.PluginCooldown, 0, len(byKey))
	for _, snapshot := range byKey {
		out = append(out, snapshot.entry)
	}
	return out
}

func workbuddyCooldownReason(reason string) string {
	switch reason {
	case "429":
		return "rate_limited"
	case "credits_exhausted":
		return "quota_exhausted"
	case cooldownReasonFirstTokenTimeout:
		return "first_token_timeout"
	case cooldownReasonEmptyStreamQuota:
		return "empty_stream_quota"
	default:
		return reason
	}
}
