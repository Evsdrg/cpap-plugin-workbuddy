package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	plinthauth "github.com/Evsdrg/cepvor-cpa-plinth/auth"
	"github.com/Evsdrg/cepvor-cpa-plinth/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// refreshLead 是提前续期窗口。取值与其它三个插件一致：access token 有效期通常
// 以小时计，提前 10 分钟足够覆盖一次失败重试，又不会让每个请求都触发续期。
const refreshLead = 10 * time.Minute

// credentialRefresher 是本 provider 的凭据新鲜度状态。
//
// 为什么要和 keepalive 并存：keepalive 是「每天 22:00 主动保活」的定时任务，
// 而这里是「用之前确认还能用」的同步检查。进程重启后宿主定时器重新计时，
// 若凭据在这段空窗里过期，旧实现会让每个请求都拿过期 token 打上游、拿到 401，
// 再靠 failover 换号——那条路径会把一个本可自动续期的账号判成不可用。
var credentialRefresher = plinthauth.New(plinthauth.Policy{
	Provider: providerName,
	Lead:     refreshLead,
	Expiry:   workbuddyCredentialExpiry,
	Refreshable: func(storage json.RawMessage) bool {
		var sa storedAuth
		return json.Unmarshal(storage, &sa) == nil && strings.TrimSpace(sa.Auth.RefreshToken) != ""
	},
	Refresh: plinthauth.Handler(func(h *abi.Host, raw json.RawMessage) (any, error) {
		var request pluginapi.AuthRefreshRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		return refreshWorkbuddyCredential(h, request)
	}),
})

// workbuddyCredentialExpiry 读凭据自己的 expiresAt。
//
// 它是 Unix 秒（camelCase，和上游 JSON 一致）。同时接受两种外层形状：
// 嵌套的 {"auth":{"expiresAt":...}} 与扁平的 {"expiresAt":...}——parseStored
// 两种都认，读到期时间也必须一致，否则扁平凭据会被当成「无到期时间」而永不续期。
func workbuddyCredentialExpiry(storage json.RawMessage) (time.Time, bool) {
	var probe struct {
		Auth struct {
			ExpiresAt int64 `json:"expiresAt"`
		} `json:"auth"`
		ExpiresAt int64 `json:"expiresAt"`
	}
	if err := json.Unmarshal(storage, &probe); err != nil {
		return time.Time{}, false
	}
	seconds := probe.Auth.ExpiresAt
	if seconds <= 0 {
		seconds = probe.ExpiresAt
	}
	if seconds <= 0 {
		return time.Time{}, false
	}
	return time.Unix(seconds, 0), true
}

// nextRefreshAfter 把到期时间换算成宿主需要的「下一次续期时刻」。
// 没有可用到期时间时返回零值，表示「没有可安排的续期」而不是「立刻续期」。
func nextRefreshAfter(expiresAt int64) time.Time {
	if expiresAt <= 0 {
		return time.Time{}
	}
	return time.Unix(expiresAt, 0).Add(-refreshLead)
}

// refreshWorkbuddyCredential 续期一个凭据。
//
// 它只返回新凭据，不自己写文件：SDK 的 Refresher 在 Handler 返回后统一按
// 「宿主已知的文件名」写回，插件再写一次会造成双重写入。写回后的列表缓存
// 失效由 ensureWorkbuddyCredential 补上——SDK 的 SaveAuth 不认识本插件的
// 30 秒列表缓存，漏了这一步 failover 池会继续提供过期账号。
func refreshWorkbuddyCredential(h *abi.Host, request pluginapi.AuthRefreshRequest) (pluginapi.AuthRefreshResponse, error) {
	sa, err := parseStored(request.StorageJSON)
	if err != nil {
		return pluginapi.AuthRefreshResponse{}, err
	}
	if strings.TrimSpace(sa.Auth.RefreshToken) == "" {
		return pluginapi.AuthRefreshResponse{}, abi.Errorf("refresh_unavailable", "该凭据没有 refreshToken，无法自动续期")
	}

	data, raw, status, errRefresh := refreshCallSource(sa)
	if errRefresh != nil {
		message := string(raw)
		if status == http.StatusUnauthorized && isSessionDeadError(message) {
			// 离线会话被上游注销：refresh token 已死，继续重试只会得到同样的 401。
			// 标成终态让 refresher 停止尝试，由操作员重新登录。
			return pluginapi.AuthRefreshResponse{}, abi.HTTPError(
				"session_dead", http.StatusUnauthorized,
				"会话已失效（12153），需要重新登录：%s", truncateRedacted(message, 120))
		}
		if status == http.StatusUnauthorized || status == http.StatusForbidden {
			return pluginapi.AuthRefreshResponse{}, abi.HTTPError(
				"refresh_rejected", status,
				"续期被拒绝（HTTP %d）：%s", status, truncateRedacted(message, 120))
		}
		return pluginapi.AuthRefreshResponse{}, abi.Errorf(
			"refresh_failed", "续期失败（HTTP %d）：%s", status, truncateRedacted(errRefresh.Error(), 120))
	}

	var token tokenData
	if err := json.Unmarshal(data, &token); err != nil || token.AccessToken == "" {
		return pluginapi.AuthRefreshResponse{}, abi.Errorf("refresh_failed", "续期响应里没有 accessToken")
	}
	sa.Auth.AccessToken = token.AccessToken
	if token.RefreshToken != "" {
		sa.Auth.RefreshToken = token.RefreshToken
	}
	if token.Domain != "" {
		sa.Auth.Domain = token.Domain
	}
	// 续期响应省略 expiresIn 时不能用「现在 + 0 秒」充当新到期时间：
	// 那是个非零值，preserveExpiry 会直接采纳它，把凭据标成已过期，
	// 此后每个请求都会再触发一次续期。传 0 让 preserveExpiry 沿用它自己的旧值。
	newExpiry := int64(0)
	if token.ExpiresIn > 0 {
		newExpiry = time.Now().Add(time.Duration(token.ExpiresIn) * time.Second).Unix()
	}
	sa.Auth.ExpiresAt = preserveExpiry(newExpiry, sa.Auth.ExpiresAt)

	storage, errMarshal := json.Marshal(sa)
	if errMarshal != nil {
		return pluginapi.AuthRefreshResponse{}, errMarshal
	}
	next := nextRefreshAfter(sa.Auth.ExpiresAt)
	logging.Info(h, "Workbuddy.auth：凭据已自动续期",
		logging.Auth(sa.Account.UID),
		logging.Reason(fmt.Sprintf("到期时间 %s", next.Add(refreshLead).Format(time.RFC3339))))

	// FileName 留空：SDK 会用宿主已知的名字写回。自己派生一个名字会在宿主
	// 与插件之间产生两个文件名，等于给同一个账号多长出一份凭据。
	return pluginapi.AuthRefreshResponse{
		Auth: pluginapi.AuthData{
			Provider:    providerName,
			StorageJSON: storage,
		},
		NextRefreshAfter: next,
	}, nil
}

// ensureWorkbuddyCredential 在使用凭据前确认它仍然可用。
//
// 返回的凭据是「本次应当使用的」：续期成功时是新凭据，未到期时是原凭据。
// 已经过期且续期失败时返回错误——此时继续打上游只会得到一条注定失败的 401，
// 把真实原因（续期失败）掩盖掉。
func ensureWorkbuddyCredential(h *abi.Host, authID string, storage []byte, attributes map[string]string) (*storedAuth, error) {
	result, errEnsure := credentialRefresher.EnsureUsable(h, plinthauth.Request{
		Name:        authID,
		StorageJSON: storage,
		Attributes:  attributes,
	})
	if errEnsure != nil {
		return nil, errEnsure
	}
	if result.Refreshed {
		// SDK 已把新凭据写回磁盘，但它的 SaveAuth 不认识本插件的列表缓存。
		// 不失效的话 failover 池最多 30 秒内仍会拿到旧 token。
		invalidateAuthListCache()
	}
	sa, errParse := parseStored(result.Storage)
	if errParse != nil {
		return nil, errParse
	}
	return sa, nil
}
