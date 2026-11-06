// Package main implements the workbuddy CLIProxyAPI dynamic plugin.
//
// workbuddy wraps Tencent CodeBuddy (copilot.tencent.com) as a cliproxy
// provider: it performs the CodeBuddy web login flow, refreshes access
// tokens, and forwards OpenAI-compatible chat completion requests to the
// upstream /v2/chat/completions endpoint.
//
// This file is a clean-room reimplementation reconstructed from the public
// workbuddy.so binary (symbol table, string constants and RPC shape) published
// by Sliverkiss. Original credit for the workbuddy plugin goes to Sliverkiss;
// see https://github.com/Sliverkiss/cpa-plugin. Built with -buildmode=c-shared
// and exports the cliproxy C ABI entry points.
package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

// Wrappers so Go can invoke the host function-pointer table via cgo. The host
// API captured at init is used to push streaming chunks back asynchronously.
static int wb_call_host(cliproxy_host_api* api, const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	return api->call(api->host_ctx, method, request, request_len, response);
}
static void wb_free_host_buffer(cliproxy_host_api* api, void* ptr, size_t len) {
	api->free_buffer(ptr, len);
}

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);
*/
import "C"

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

const (
	providerName  = "workbuddy"
	authFileName  = "workbuddy.json"
	pluginLogoURL = "https://raw.githubusercontent.com/Evsdrg/cpap-plugin-workbuddy/main/assets/logo.png"
	// CN chat/auth gateway (iss = codebuddy.cn realm).
	upstreamBaseCN = "https://copilot.tencent.com"
	// Global chat/auth gateway (iss = workbuddy.ai realm). APISIX on
	// copilot.tencent.com rejects Global JWTs with 401; must use workbuddy.ai.
	upstreamBaseGlobal = "https://www.codebuddy.ai"
	// cliVersion 是 CodeBuddy CLI（@tencent-ai/codebuddy-code）的客户端版本。
	// CLI 的 X-IDE-Version 与 User-Agent 都用它：UA 形如
	// "CLI/<version> CodeBuddy/<version>"，由 UserAgentHttpInterceptor 拼装。
	cliVersion   = "2.172.1"
	cliUserAgent = "CLI/" + cliVersion + " CodeBuddy/" + cliVersion
	// desktopUA 是 WorkBuddy 桌面客户端在桌面端接口上的 UA。
	// 桌面端与 CLI 是两条独立协议线，不能混用 UA（见 models_global.go 注释）。
	desktopUA = "WorkBuddy/5.8.4"

	originReferer       = "https://www.codebuddy.cn"
	originRefererGlobal = "https://www.codebuddy.ai"

	// CN endpoint aliases (login / chat / models). upstreamBaseCN is the only
	// CN base; Global has its own upstreamBaseGlobal. No "upstreamBase" legacy
	// alias — removed in v0.6.31 dead-code sweep.
	endpointAuthState    = upstreamBaseCN + "/v2/plugin/auth/state?platform=CLI"
	endpointLoginAcct    = upstreamBaseCN + "/v2/plugin/login/account?state="
	endpointAuthToken    = upstreamBaseCN + "/v2/plugin/auth/token?state="
	endpointTokenRefresh = upstreamBaseCN + "/v2/plugin/auth/token/refresh"
	endpointChat         = upstreamBaseCN + "/v2/chat/completions"
	// Model-catalog paths live in models.go (modelsEnterprisePaths): the
	// Global path moved from /console to /v2 on 2026-10-14.

	loginTTL = 5 * time.Minute
)

// loginCtx holds the cookie-affined HTTP client for one in-flight login flow.
// CodeBuddy associates the browser login with the state issued at auth/state,
// so we must reuse the same cookie jar across the state request and the polls.
type loginCtx struct {
	client  *http.Client
	expires time.Time
}

var (
	hostAPI        *C.cliproxy_host_api // captured at init, used for async host calls
	loginStates    sync.Map             // state(string) -> *loginCtx
	httpClientOnce sync.Once
	sharedClient   *http.Client
)

// loginStatesPruneInterval bounds how often the janitor sweeps abandoned
// login states (user started a login but never finished).
const loginStatesPruneInterval = time.Minute

func init() {
	go func() {
		ticker := time.NewTicker(loginStatesPruneInterval)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			loginStates.Range(func(key, value any) bool {
				if lc, ok := value.(*loginCtx); ok && now.After(lc.expires) {
					loginStates.Delete(key)
				}
				return true
			})
		}
	}()
}

func main() {}

// -----------------------------------------------------------------------------
// C ABI exports
// -----------------------------------------------------------------------------

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	hostAPI = host
	// 把同一个 cgo 传输交给 SDK：SDK 的 abi.Host 方法与插件里手写的
	// hostCall 共用一条通道，两边都能用。后续迁移手写调用点时不需要再
	// 单独接线。
	abi.SetHostCaller(hostCall)
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required"))
		return 1
	}
	var requestBytes []byte
	if request != nil && requestLen > 0 {
		requestBytes = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	raw, errHandle := handleMethod(abi.NewHost(requestBytes), C.GoString(method), requestBytes)
	if errHandle != nil {
		writeResponse(response, errorEnvelope("plugin_error", errHandle.Error()))
		return 1
	}
	writeResponse(response, raw)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, len C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {
	// Intentionally a no-op. The host calls this on its own exit path (after
	// the host Go runtime has started tearing down) and dlclose()es this
	// library immediately afterwards. Touching any Go runtime state here —
	// mutexes, channel close, goroutine synchronization — risks a SIGSEGV in
	// cgo (observed on every docker restart: SIGSEGV in
	// _Cfunc_cliproxy_shutdown_plugin, PC near a freed runtime pointer).
	// The scheduler goroutine and janitor ticker hold no resources that
	// outlive the process; the OS reclaims them on exit.
}

// -----------------------------------------------------------------------------
// Host calls (async streaming + auth callbacks)
// -----------------------------------------------------------------------------

// hostCall invokes a host RPC method via the function-pointer table captured
// at init. Used to push stream chunks back asynchronously (host.stream.emit /
// host.stream.close) and to read the host's auth store (host.auth.list/get).
func hostCall(method string, request []byte) ([]byte, error) {
	if hostAPI == nil || hostAPI.call == nil {
		return nil, fmt.Errorf("host API unavailable")
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))
	var cReq unsafe.Pointer
	var reqLen C.size_t
	if len(request) > 0 {
		cReq = C.CBytes(request)
		defer C.free(cReq)
		reqLen = C.size_t(len(request))
	}
	var resp C.cliproxy_buffer
	rc := C.wb_call_host(hostAPI, cMethod, (*C.uint8_t)(cReq), reqLen, &resp)
	var out []byte
	if resp.ptr != nil && resp.len > 0 {
		out = C.GoBytes(resp.ptr, C.int(resp.len))
	}
	if resp.ptr != nil && hostAPI.free_buffer != nil {
		C.wb_free_host_buffer(hostAPI, resp.ptr, resp.len)
	}
	if rc != 0 {
		return out, fmt.Errorf("host call %s returned %d", method, int(rc))
	}
	return out, nil
}

// -----------------------------------------------------------------------------

func handleMethod(host *abi.Host, method string, request []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		configure(request)
		return okEnvelope(wbRegistration())
	case pluginabi.MethodModelStatic:
		return handleModelStatic(request)
	case pluginabi.MethodModelForAuth:
		return handleModelForAuth(request)
	case pluginabi.MethodAuthIdentifier:
		return okEnvelope(identifierResponse{Identifier: providerName})
	case pluginabi.MethodAuthParse:
		return handleParseAuth(request)
	case pluginabi.MethodAuthLoginStart:
		return handleStartLogin(request)
	case pluginabi.MethodAuthLoginPoll:
		return handlePollLogin(request)
	case pluginabi.MethodAuthRefresh:
		return handleRefreshAuth(request)
	case pluginabi.MethodExecutorIdentifier:
		return okEnvelope(identifierResponse{Identifier: providerName})
	case pluginabi.MethodExecutorExecute:
		return handleExecExecute(host, request)
	case pluginabi.MethodExecutorExecuteStream:
		return handleExecStream(host, request)
	case pluginabi.MethodExecutorCountTokens:
		// Upstream CodeBuddy has no dedicated count_tokens API. Return
		// unhandled-style zero estimate so clients fall back / skip.
		return okEnvelope(pluginapi.ExecutorResponse{Payload: []byte(`{"input_tokens":0}`)})
	case pluginabi.MethodManagementRegister:
		// Cache host-injected BasePath so handleManagement doesn't hardcode
		// /v0/management (v0.6.31: tolerate future host path changes).
		var regReq pluginapi.ManagementRegistrationRequest
		if err := json.Unmarshal(request, &regReq); err == nil {
			if regReq.BasePath != "" {
				setManagementBasePath(regReq.BasePath)
			}
		}
		return okEnvelope(managementRegistration())
	case pluginabi.MethodManagementHandle:
		return handleManagement(request)
	case pluginabi.MethodSchedulerPick:
		return handleSchedulerPick(request)
	case pluginabi.MethodUsageHandle:
		return handleUsage(request)
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

// -----------------------------------------------------------------------------
// Registration & models
// -----------------------------------------------------------------------------

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type identifierResponse struct {
	Identifier string `json:"identifier"`
}

type registration struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Metadata      pluginapi.Metadata     `json:"metadata"`
	Capabilities  registrationCapability `json:"capabilities"`
}

type streamResponse struct {
	Headers http.Header                     `json:"headers,omitempty"`
	Chunks  []pluginapi.ExecutorStreamChunk `json:"chunks,omitempty"`
}

type registrationCapability struct {
	ModelProvider         bool                         `json:"model_provider"`
	AuthProvider          bool                         `json:"auth_provider"`
	FrontendAuthProvider  bool                         `json:"frontend_auth_provider"`
	Executor              bool                         `json:"executor"`
	ExecutorModelScope    pluginapi.ExecutorModelScope `json:"executor_model_scope"`
	ExecutorInputFormats  []string                     `json:"executor_input_formats,omitempty"`
	ExecutorOutputFormats []string                     `json:"executor_output_formats,omitempty"`
	Scheduler             bool                         `json:"scheduler"`
	ManagementAPI         bool                         `json:"management_api"`
	UsagePlugin           bool                         `json:"usage_plugin"`
}

// version is injected at build time via -ldflags "-X main.version=...".
var version = "0.20.0"

func wbRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:             "CodeBuddy Intl CPA",
			Version:          version,
			Author:           "zidanefaqih (based on Sliverkiss/cpa-plugin, itself based on workbuddy by lovingfish)",
			GitHubRepository: "https://github.com/Evsdrg/cpap-plugin-workbuddy",
			Logo:             pluginLogoURL,
			ConfigFields: []pluginapi.ConfigField{
				{Name: "checkin_auto", Label: "每日自动签到", Type: pluginapi.ConfigFieldTypeBoolean, Default: true, Description: "每天在 checkin_hours 指定的时刻自动签到（国内账号）。签到在服务端幂等，错过某个时刻会在下一个时刻补上"},
				{Name: "checkin_hours", Label: "签到时刻", Type: pluginapi.ConfigFieldTypeString, Default: "9,21", Description: "自动签到的本地时刻，逗号分隔（默认 9,21，取 0-23 的整点）。多个时刻可自动补上错过的日子；越界的值会被丢弃"},
				{Name: "lifecycle_auto", Label: "额度生命周期自动化", Type: pluginapi.ConfigFieldTypeBoolean, Default: true, Description: "额度耗尽后自动停用国内账号 / 删除国际账号；国内账号签到达标恢复额度后自动重新启用（默认开启）"},
				{Name: "token_keepalive", Label: "令牌每日保活", Type: pluginapi.ConfigFieldTypeBoolean, Default: true, Description: "每天 22:00 本地时间刷新一次 access token，防止 Keycloak 离线会话过期（默认开启）"},
				{Name: "daily_bonus", Label: "国际版每日奖励", Type: pluginapi.ConfigFieldTypeBoolean, Default: true, Description: "每天 08/12/16/20 点上报一次成长行为，让每个国际版账号领到 30 积分每日奖励包。按账号按天幂等（默认开启）"},
				{
					Name:            "model_selection",
					Label:           "注册模型",
					Type:            pluginapi.ConfigFieldTypeModelSelection,
					OptionsEndpoint: "/v0/management/plugins/" + providerName + "/model-candidates",
					Default:         "全部模型（未配置时）",
					Description:     "勾选要注册到网关的模型，并可为每个模型指定注册名（客户端请求时使用的名字）。这是最终决定：未配置时注册目录中的全部模型；一旦配置，未勾选的模型会从网关完全隐藏",
				},
				{Name: "scheduler_mode", Label: "多账号选择策略", Type: pluginapi.ConfigFieldTypeEnum, EnumValues: []string{schedulerModeOff, schedulerModeCredits}, Default: schedulerModeOff, Description: "多账号选择：off（交给内置策略，默认）或 credits（优先剩余额度最高的账号）。注意：off + lifecycle_auto=false 时已耗尽账号仍可能被路由，建议开启 lifecycle_auto 或设为 credits"},
				{Name: "usage_report_url", Label: "用量上报地址", Type: pluginapi.ConfigFieldTypeString, Description: "覆盖 CPAMP 用量导入地址（默认 http://cpa-manager-plus:18317/v0/management/usage/import，也可用环境变量 USAGE_REPORT_URL）"},
				{Name: "usage_report_key", Label: "用量上报密钥", Type: pluginapi.ConfigFieldTypeString, Description: "覆盖 CPAMP 管理密钥。建议改用环境变量 CPAMP_ADMIN_KEY / USAGE_REPORT_KEY 或密钥文件 /run/secrets/cpamp_admin_key"},
				{Name: "management_key", Label: "管理接口密钥", Type: pluginapi.ConfigFieldTypeString, Description: "插件层管理接口的鉴权密钥。设置后所有写端点要求 Bearer 匹配并启用按 IP 限流；留空则只依赖宿主的管理中间件。也可用环境变量 WB_MANAGEMENT_KEY"},
				{Name: "retry_per_account", Label: "同账号重试次数", Type: pluginapi.ConfigFieldTypeInteger, Default: defaultRetryPerAccount, Description: "遇到限流后换账号前的同账号额外重试次数（默认 3；0 = 立即换账号）"},
				{Name: "retry_delay_seconds", Label: "重试间隔（秒）", Type: pluginapi.ConfigFieldTypeInteger, Default: defaultRetryDelaySeconds, Description: "同账号重试之间的等待秒数（默认 2）"},
				{Name: "model_refresh_ms", Label: "模型目录刷新间隔", Type: pluginapi.ConfigFieldTypeInteger, Default: 0, Description: "后台重拉模型目录的间隔（毫秒）。0 表示不刷新（默认）。一次刷新会按账号各走一次上游往返，因此需要显式开启；目录真的变化时插件会自动回写凭据触发宿主重新注册模型。"},
			},
		},
		Capabilities: registrationCapability{
			ModelProvider:         true,
			AuthProvider:          true,
			FrontendAuthProvider:  false,
			Executor:              true,
			ExecutorModelScope:    pluginapi.ExecutorModelScopeOAuth,
			ExecutorInputFormats:  []string{"chat-completions"},
			ExecutorOutputFormats: []string{"chat-completions"},
			ManagementAPI:         true,
			Scheduler:             true,
			UsagePlugin:           true,
		},
	}
}

// dynamicModelsCacheTTL bounds how long a fetched model list is reused.
// model.static / model.for_auth are re-invoked by CPA on every config reload
// and on each models query; without caching, every reload fans out to one
// upstream call per account.
const dynamicModelsCacheTTL = 5 * time.Minute

// dynamicModelsCache 按「realm + 账号等级」分桶。
//
// 不能只存一份：国际版的免费账号与付费账号看到的模型集不同（Pro Only 分层），
// 国内版与国际版的目录更是两套完全不同的端点。只缓存一份会让后查询的账号
// 直接复用前一个账号的结果。
var dynamicModelsCache struct {
	sync.RWMutex
	entries map[string]dynamicModelsEntry
}

type dynamicModelsEntry struct {
	models  []pluginapi.ModelInfo
	fetched time.Time
}

//
// CPA applies oauth-model-alias to the models this plugin registers, so the
// gateway may route a request whose model ID is an alias (e.g.
// "point/deepseek-v4-flash") to this executor. The upstream only knows the
// real model IDs, so the plugin must map the alias back before forwarding.
//
// ExecutorRequest carries no host config, so the alias table is cached from
// the AuthModelRequest.Host summary every time the host asks for models
// (model.static / model.for_auth are re-queried by CPA on config reload,
// keeping this cache in sync with oauth-model-alias changes). Auth-level
// attribute overrides ("model_alias"/"model-alias"/"oauth-model-alias")
// are parsed per request and take precedence over the global table.

var modelAliasCache struct {
	sync.RWMutex
	byAlias map[string]string
}

// ------------------------------------------------------------------------------
// Usage reporting (request monitoring)
// ------------------------------------------------------------------------------
//
// CPA built-in executors publish via host usage.DefaultManager → redisqueue.
// Plugin executors cannot: c-shared .so has its own Go runtime, so
// usage.PublishRecord would hit a separate empty DefaultManager (no sink).
//
// Only effective path: POST NDJSON to CPA-Manager-Plus
// /v0/management/usage/import. Key/URL resolved automatically from
// config → env → docker secret files (see resolveUsageReport).
// usage.Detail is still used as a pure token-counter struct.

// storedAuth is the on-disk shape of a workbuddy credential.
type storedAuth struct {
	Auth    storedTokens  `json:"auth"`
	Account storedAccount `json:"account"`
}

type storedTokens struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresAt    int64  `json:"expiresAt"`
	Domain       string `json:"domain"`
}

type storedAccount struct {
	UID          string `json:"uid"`
	EnterpriseID string `json:"enterpriseId"`
	Nickname     string `json:"nickname"`
}

// apiEnvelope is the generic {code,msg,data} wrapper used by every CodeBuddy API.
type apiEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type tokenData struct {
	AccessToken      string `json:"accessToken"`
	RefreshToken     string `json:"refreshToken"`
	ExpiresIn        int64  `json:"expiresIn"`
	RefreshExpiresIn int64  `json:"refreshExpiresIn"`
	Domain           string `json:"domain"`
}

type accountData struct {
	UID          string `json:"uid"`
	EnterpriseID string `json:"enterpriseId"`
	Nickname     string `json:"nickname"`
}

type authStateData struct {
	State   string `json:"state"`
	AuthURL string `json:"authUrl"`
}

func parseStored(raw []byte) (*storedAuth, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty auth storage")
	}
	// Accept both shapes seen in the wild:
	//   nested: {"auth":{"accessToken":...},"account":{"uid":...}} (plugin/oauth output)
	//   flat:   {"accessToken":...,"uid":...,"nickname":...} (CPA-Manager-Plus auths/workbuddy.json)
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("storage_parse_error: %w", err)
	}
	var sa storedAuth
	if _, nested := probe["auth"]; nested {
		if err := json.Unmarshal(raw, &sa); err != nil {
			return nil, fmt.Errorf("storage_parse_error: %w", err)
		}
	} else {
		var flat struct {
			AccessToken  string `json:"accessToken"`
			RefreshToken string `json:"refreshToken"`
			ExpiresAt    int64  `json:"expiresAt"`
			Domain       string `json:"domain"`
			UID          string `json:"uid"`
			EnterpriseID string `json:"enterpriseId"`
			Nickname     string `json:"nickname"`
		}
		if err := json.Unmarshal(raw, &flat); err != nil {
			return nil, fmt.Errorf("storage_parse_error: %w", err)
		}
		sa.Auth = storedTokens{AccessToken: flat.AccessToken, RefreshToken: flat.RefreshToken, ExpiresAt: flat.ExpiresAt, Domain: flat.Domain}
		sa.Account = storedAccount{UID: flat.UID, EnterpriseID: flat.EnterpriseID, Nickname: flat.Nickname}
	}
	if sa.Auth.AccessToken == "" {
		return nil, fmt.Errorf("parse_error: missing accessToken")
	}
	return &sa, nil
}

// -------------------------------------------------------------------------------
// HTTP plumbing
// -------------------------------------------------------------------------------

// canonicalSessionID reads the host's authoritative session identity out of the
// executor metadata.
//
// CPAP derives it once per request from a twelve-step priority list (explicit
// client session headers such as Session-Id / X-Session-Id / X-Zcode-Trace-Id,
// prompt_cache_key, conversation ids, then a content-derived fallback) and hands
// the value to the executor. Reusing it is strictly better than re-deriving a
// bucket from the body:
//
//   - it is what the host's own session-affinity bookkeeping is keyed on, so the
//     upstream cache bucket and the routing decision agree;
//   - it stays stable even when the first user message is not the conversation
//     opener (a resumed session whose prefix was compacted away).
//
// An empty return means the host could not identify a session; the caller then
// falls back to the content-prefix hash.
const canonicalSessionIDMetadataKey = "canonical_session_id"

func canonicalSessionID(metadata map[string]any) string {
	if len(metadata) == 0 {
		return ""
	}
	raw, ok := metadata[canonicalSessionIDMetadataKey]
	if !ok || raw == nil {
		return ""
	}
	text, ok := raw.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(text)
}

// stableConversationID derives a stable per-conversation ID from the request
// body prefix (system + first user message). CodeBuddy caches prompt KV-cache
// per X-Conversation-ID; random IDs force cache write on every request.
// Mirror of the SRouter/InferHub session scheme: hash ONLY the conversation
// prefix (system + first user text), NOT the whole body — the body grows on
// every turn, so hashing it would rotate the conversation id each turn and
// defeat the cache for multi-turn coding-agent sessions.
func stableConversationID(body []byte) string {
	var req struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	prefix := ""
	if err := json.Unmarshal(body, &req); err == nil {
		firstUserDone := false
		for _, m := range req.Messages {
			if m.Role == "system" {
				prefix += string(m.Content) + "\n"
			} else if m.Role == "user" && !firstUserDone {
				// content may be a plain string or an array of {type:text}
				var s string
				if json.Unmarshal(m.Content, &s) == nil {
					prefix += s
				} else {
					prefix += string(m.Content)
				}
				firstUserDone = true
			}
		}
	}
	if prefix == "" {
		prefix = string(body)
	}
	h := sha256.Sum256([]byte(prefix))
	return hex.EncodeToString(h[:16])
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// cliTransportHeaders 是 CodeBuddy CLI 的通用传输头。
//
// 端点归属：CLI 协议线（/v2/chat/completions 与 /v2/plugin/auth/*）。
// Origin / Referer / X-Requested-With 是 WorkBuddy 桌面端的浏览器式头，
// CLI 的模型请求路径不发这些，不能混到这条线上。
func cliTransportHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", cliUserAgent)
}

// cliIdentityHeaders 是 CLI 在模型请求上声明的客户端身份。
//
// 依据 dist/codebuddy.js 的 chat 发送点：
//
//	X-IDE-Type    = clientInfo.ideType       = product.json platform = "CLI"
//	X-IDE-Name    = clientInfo.platform      = 同上一并回退为 "CLI"
//	X-IDE-Version = clientInfo.platformVersion = productVersion ?? package.version
//
// X-Agent-Intent 默认 "craft"，X-Agent-Runtime 由 resolveSessionAgentRuntime
// 得出（无 meta / 无 CODEBUDDY_AGENT_RUNTIME 时默认 "other"）。
// X-Private-Data 只在会话带 codebuddy.ai/privateDataSticky 时发送，普通会话不发。
// X-Agent-Type 由 resolveModelCallAgentType 得出：无 teamContext / 无
// parentConversationId 的主会话恒为 "main"（见 resolveSessionAgentType）。
func cliIdentityHeaders(req *http.Request) {
	req.Header.Set("X-IDE-Type", "CLI")
	req.Header.Set("X-IDE-Name", "CLI")
	req.Header.Set("X-IDE-Version", cliVersion)
	req.Header.Set("X-Agent-Intent", "craft")
	req.Header.Set("X-Agent-Runtime", "other")
	req.Header.Set("X-Agent-Type", "main")
}

// cliSuppressIdentityHeaders 对应 CLI 的 X-No-* 约定，值为 "true"（不是 "1"）。
//
// 官方用法：auth/state 与 auth/token 四个头全置 true；login/account 只置
// 后三个（Authorization 由调用方提供）。
func cliSuppressIdentityHeaders(req *http.Request, suppressAuthorization bool) {
	if suppressAuthorization {
		req.Header.Set("X-No-Authorization", "true")
	}
	req.Header.Set("X-No-User-Id", "true")
	req.Header.Set("X-No-Enterprise-Id", "true")
	req.Header.Set("X-No-Department-Info", "true")
}

// desktopTransportHeaders 是 WorkBuddy 桌面端的通用头。
//
// 端点归属：桌面端功能（billing / 签到 / 模型目录），这些接口在 CLI 包里
// 完全不存在，必须用桌面端身份与浏览器式头。
func desktopTransportHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Origin", originReferer)
	req.Header.Set("Referer", originReferer+"/")
	req.Header.Set("User-Agent", desktopUA)
}

// originRefererFor returns the Origin/Referer base URL appropriate for the
// account's domain. Global accounts use https://www.workbuddy.ai; CN (and
// legacy auth files with empty domain) use the default https://www.codebuddy.cn.
func originRefererFor(sa *storedAuth) string {
	if sa != nil && isGlobalDomain(sa.Auth.Domain) {
		return originRefererGlobal
	}
	return originReferer
}

// upstreamBaseFor returns the chat/auth API host for the account realm.
// Global JWT iss is workbuddy.ai — those tokens only work on www.workbuddy.ai.
// CN tokens work on copilot.tencent.com. Mixing them yields APISIX 401.
func upstreamBaseFor(sa *storedAuth) string {
	if sa != nil && isGlobalDomain(sa.Auth.Domain) {
		return upstreamBaseGlobal
	}
	return upstreamBaseCN
}

func endpointChatFor(sa *storedAuth) string {
	return upstreamBaseFor(sa) + "/v2/chat/completions"
}

func endpointTokenRefreshFor(sa *storedAuth) string {
	return upstreamBaseFor(sa) + "/v2/plugin/auth/token/refresh"
}

// endpointModelsFor returns the first-choice model-catalog URL for an account.
// Global moved to /v2 on 2026-10-14; callModelsAPI owns the fallback chain.
func endpointModelsFor(sa *storedAuth) string {
	if accountRegion(sa) == "global" {
		return upstreamBaseGlobal + "/v2/enterprises/personal/models"
	}
	return upstreamBaseCN + "/console/enterprises/personal/models"
}

// backendHeaders 为 CLI 协议的 chat 请求应用头。
//
// 依据 dist/codebuddy.js 的 axiosToFetchAdapter：CLI 的模型请求携带
// Content-Type / Accept / Authorization / X-User-Id / X-Enterprise-Id /
// 会话四元组 / X-Agent-* / X-IDE-* / X-Product，另外由 runIdentityHeaders
// 补 X-User-Id 与 X-Enterprise-Id。它不发送 Origin / Referer /
// X-Requested-With / X-No-*（那些属于桌面端与认证端点）。
func backendHeaders(req *http.Request, sa *storedAuth, sessionIDOverride string) {
	cliTransportHeaders(req)
	cliIdentityHeaders(req)
	if sa.Auth.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+sa.Auth.AccessToken)
	}
	if sa.Account.UID != "" {
		req.Header.Set("X-User-Id", sa.Account.UID)
	}
	if sa.Account.EnterpriseID != "" {
		req.Header.Set("X-Enterprise-Id", sa.Account.EnterpriseID)
	}
	// CLI 的 X-Product 取 product.json 的 deploymentType，SaaS 发行版恒为 "SaaS"。
	req.Header.Set("X-Product", "SaaS")

	// 稳定 conversation ID 开启 CodeBuddy 的 prompt cache（KV cache 命中）。
	// 每次请求随机会导致每问都写缓存、读取恒为零。由请求体前缀派生；
	// 同一次请求内的子 ID 保持同值，与 CLI 的 messageId 用法一致。
	// Conversation identity: prefer the host's canonical session id, because that
	// is what CPAP's own session-affinity routing is keyed on. Only when the host
	// could not identify a session do we derive a bucket from the request body.
	cid := strings.TrimSpace(sessionIDOverride)
	if cid == "" && req.Body != nil {
		if b, err := io.ReadAll(req.Body); err == nil && len(b) > 0 {
			cid = stableConversationID(b)
			req.Body = io.NopCloser(bytes.NewReader(b))
		}
	}
	if cid == "" {
		cid = randomHex(16)
	}
	messageID := randomHex(16)
	req.Header.Set("X-Conversation-ID", cid)
	req.Header.Set("X-Conversation-Request-ID", randomHex(16))
	req.Header.Set("X-Conversation-Message-ID", messageID)
	req.Header.Set("X-Request-ID", messageID)
}

// -----------------------------------------------------------------------------
// Auth handlers
// -----------------------------------------------------------------------------

func handleParseAuth(raw []byte) ([]byte, error) {
	var req pluginapi.AuthParseRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	// Ownership check (CPA native contract): the host routes by the file's
	// top-level "type" field (synthesizer/file.go). Files without a type fall
	// back to polling every plugin — first Handled=true wins. Only claim files
	// whose declared type matches us — or, for type-less legacy files, when the
	// host already routed this to us or the filename carries our prefix.
	// Symmetric with the qoderwork plugin's guard (commit 7b776a9).
	var probeType struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(req.RawJSON, &probeType)
	declared := strings.ToLower(strings.TrimSpace(probeType.Type))
	if declared != "" && declared != providerName {
		// Explicitly another provider's file — never claim it.
		return okEnvelope(pluginapi.AuthParseResponse{Handled: false})
	}
	if declared == "" {
		routed := strings.EqualFold(strings.TrimSpace(req.Provider), providerName)
		prefixed := strings.HasPrefix(strings.ToLower(strings.TrimSpace(req.FileName)), providerName+"-")
		if !routed && !prefixed {
			return okEnvelope(pluginapi.AuthParseResponse{Handled: false})
		}
	}
	sa, err := parseStored(req.RawJSON)
	if err != nil {
		// Not a workbuddy credential; let the host try other providers.
		return okEnvelope(pluginapi.AuthParseResponse{Handled: false})
	}
	// CRITICAL: echo back the host-provided FileName AND leave ID empty.
	//
	// CPA uses ID for auth record identity (upsert key). If we set ID=uid
	// while the host's watcher initially registered with ID=filename,
	// upsertAuthRecord can't find the existing record → creates a NEW one
	// → duplicate auth entries (same file, different IDs).
	//
	// By leaving ID empty, CPA falls back to authIDForPath(path) which
	// derives ID from the file path → always matches the watcher's key.
	// FileName is also echoed back to avoid rename-based duplicates.
	ad := toAuthDataOpts(sa, nil, false)
	ad.ID = "" // let host compute from path (prevents ID mismatch dupes)
	if fn := strings.TrimSpace(req.FileName); fn != "" {
		ad.FileName = fn
	}
	return okEnvelope(pluginapi.AuthParseResponse{
		Handled: true,
		Auth:    ad,
	})
}

func toAuthData(sa *storedAuth) pluginapi.AuthData {
	return toAuthDataOpts(sa, nil, false)
}

// toAuthDataOpts builds AuthData with optional credits snapshot and disabled flag.
func toAuthDataOpts(sa *storedAuth, cr *creditsSummary, disabled bool) pluginapi.AuthData {
	storage, _ := json.Marshal(sa)
	id := providerName
	fileName := authFileName
	if sa != nil {
		if uid := sanitizeUIDForFileName(sa.Account.UID); uid != "" {
			id = uid
			fileName = "workbuddy-" + uid + ".json"
		}
	}
	label := labelForAuth(sa)
	meta := enrichAuthMetadata(sa, cr, disabled)
	return pluginapi.AuthData{
		Provider:    providerName,
		ID:          id,
		FileName:    fileName,
		Label:       label,
		Disabled:    disabled,
		StorageJSON: storage,
		// Standardized auth metadata. `type` is required by the host for
		// auth-file classification; `logo`/`note`/`disabled` surface on auth rows.
		Metadata: meta,
	}
}

// -----------------------------------------------------------------------------

func handleExecExecute(host *abi.Host, raw []byte) ([]byte, error) {
	var req pluginapi.ExecutorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	sa, err := ensureWorkbuddyCredential(host, req.AuthID, req.StorageJSON, req.AuthAttributes)
	if err != nil {
		return nil, err
	}
	// Resolve oauth-model-alias (e.g. "point/deepseek-v4-flash") back to the
	// real upstream model ID; the upstream rejects unknown alias IDs.
	upstreamModel := resolveUpstreamModel(req.Model, req.AuthAttributes)
	started := time.Now()
	authUID := ""
	if sa.Account.UID != "" {
		authUID = sa.Account.UID
	}
	// CodeBuddy rejects non-stream requests (code 11101), so always stream
	// upstream and fold the chunks into a single chat.completion object.
	// prepareUpstreamBody does forceStream + normalizeTools + rewriteSystem +
	// ensureSystemMessage + rewriteModel in ONE unmarshal/marshal pass.
	body := prepareUpstreamBody(req.Payload, req.OriginalRequest, sa, upstreamModel)

	// Failover (v0.8.8): when the routed account answers 429 / code 6004, the
	// same request is retried on the next eligible enabled account. One usage
	// record per inbound request (success or final failure); the CPAMP publish
	// follows the same single-line rule.
	pool := buildFailoverPool(req.AuthID, req.Model)
	cands := pool.ordered(req.StorageJSON)
	var completion []byte
	// 出站调用要带上本次调用的宿主句柄，宿主才能把上游路径记进用量。
	attemptCtx := withHost(context.Background(), host)
	fin := runFailover(attemptCtx, cands, func(cand *failoverCandidate) (attemptOutcome, int, string, error) {
		out, err := upstreamAttempt(attemptCtx, body, req.Model, cand, req.Metadata)
		completion = out // nil on error
		return classifyNonStream(out, err)
	})
	switch fin.Outcome {
	case outcomeSuccess:
		uid := candUID(findCandidate(cands, fin.AuthID), authUID)
		publishUsage(req.Model, upstreamModel, uid, started, usageDetailFromCompletion(completion), false, 0, "")
		recordUsageMap(req.Model, upstreamModel, req.AuthID, authUID, started, usageMapFromCompletion(completion), false)
		onFailoverSuccess(req.AuthID, fin.AuthID, authUID)
		return okEnvelope(pluginapi.ExecutorResponse{Payload: completion})
	default:
		// outcomeStop (transport/business) keeps the legacy reconcile path for
		// hard errors; pure rate-limit exhaustion records one failed line and
		// returns the last upstream error. Failures keep the routed account's
		// identity in usage (matches pre-failover behavior).
		publishUsage(req.Model, upstreamModel, authUID, started, usage.Detail{}, true, fin.Status, fin.Body)
		recordUsageMap(req.Model, upstreamModel, req.AuthID, authUID, started, nil, true)
		if fin.Status > 0 && !isRateLimitResponse(fin.Status, fin.Body) {
			reconcileAfterExecutorError(req.AuthID, fin.Status, fin.Body)
		}
		if fin.Err == nil {
			fin.Err = fmt.Errorf("workbuddy request failed")
		}
		return nil, fin.Err
	}
}

// executorStreamRequest wraps the host's executor.execute_stream RPC: the
// ExecutorRequest plus the async stream id the host uses to receive chunks.
type executorStreamRequest struct {
	pluginapi.ExecutorRequest
	StreamID       string `json:"stream_id,omitempty"`
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

func handleExecStream(host *abi.Host, raw []byte) ([]byte, error) {
	var req executorStreamRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	sa, err := ensureWorkbuddyCredential(host, req.AuthID, req.StorageJSON, req.AuthAttributes)
	if err != nil {
		return nil, err
	}
	upstreamModel := resolveUpstreamModel(req.Model, req.AuthAttributes)
	started := time.Now()
	authUID := ""
	if sa.Account.UID != "" {
		authUID = sa.Account.UID
	}
	body := req.Payload
	if len(body) == 0 {
		body = req.OriginalRequest
	}
	// Single-pass JSON rewrite (see handleExecExecute for the non-stream path).
	body = prepareUpstreamBody(body, nil, sa, upstreamModel)

	headers := streamHeaders()
	sseFramed := clientNeedsSSEFrame(req.Metadata)

	// No async stream id → fall back to synchronous chunk collection.
	if req.StreamID == "" {
		pool := buildFailoverPool(req.AuthID, req.Model)
		cands := pool.ordered(req.StorageJSON)
		collector := &sseUsageCollector{}
		var chunks []pluginapi.ExecutorStreamChunk
		partial := false
		attemptCtx := withHost(context.Background(), host)
		fin := runFailover(attemptCtx, cands, func(cand *failoverCandidate) (attemptOutcome, int, string, error) {
			var statusCode int
			var err error
			chunks, statusCode, err = collectStreamAttempt(attemptCtx, body, cand, sseFramed, collector, req.Metadata)
			if err != nil {
				if len(chunks) > 0 {
					// Partial data already collected — the sync path returns it
					// to the host, so do not replay on another account.
					partial = true
					return outcomeStop, statusCode, "", err
				}
				if statusCode == 0 && upstreamCodeOf(err) == 0 {
					return outcomeStop, 0, "", err
				}
				// 必须同时看业务错误码：上游把额度/限流错误放在 HTTP 200 的信封里，
				// 此时 statusCode 为 0/200，只有 code 能表达失败性质。否则这类错误
				// 会落到 outcomeStop，账号不切换、上游原文直接漏给客户端。
				switch classifyUpstreamFailure(statusCode, upstreamCodeOf(err), err.Error()) {
				case outcomeAccountExhausted:
					return outcomeAccountExhausted, statusCode, err.Error(), err
				case outcomeRateLimited:
					return outcomeRateLimited, statusCode, err.Error(), err
				}
				return outcomeStop, statusCode, err.Error(), err
			}
			return outcomeSuccess, 0, "", nil
		})
		switch fin.Outcome {
		case outcomeSuccess:
			publishUsage(req.Model, upstreamModel, candUID(findCandidate(cands, fin.AuthID), authUID), started, collector.detail(), false, 0, "")
			recordUsageMap(req.Model, upstreamModel, req.AuthID, authUID, started, collector.lastMap(), false)
			onFailoverSuccess(req.AuthID, fin.AuthID, authUID)
			return okEnvelope(streamResponse{Headers: headers, Chunks: chunks})
		default:
			if partial {
				// Partial chunks were already collected before the read error —
				// hand them to the host and let it close the stream.
				publishUsage(req.Model, upstreamModel, authUID, started, collector.detail(), true, fin.Status, fin.Body)
				recordUsageMap(req.Model, upstreamModel, req.AuthID, authUID, started, collector.lastMap(), true)
				return okEnvelope(streamResponse{Headers: headers, Chunks: chunks})
			}
			publishUsage(req.Model, upstreamModel, authUID, started, usage.Detail{}, true, fin.Status, fin.Body)
			recordUsageMap(req.Model, upstreamModel, req.AuthID, authUID, started, collector.lastMap(), true)
			if fin.Status > 0 && !isRateLimitResponse(fin.Status, fin.Body) {
				reconcileAfterExecutorError(req.AuthID, fin.Status, fin.Body)
			}
			if fin.Err == nil {
				fin.Err = fmt.Errorf("workbuddy stream request failed")
			}
			return nil, fin.Err
		}
	}

	// Async: return immediately with empty chunks. A goroutine pumps the upstream
	// and emits each chunk via host.stream.emit so the client sees true streaming.
	// Use context.Background() (not nil) so the request can be cancelled when the
	// client disconnects — otherwise the pump keeps reading a dead upstream until
	// sharedHTTPClient's 120s timeout, holding a pool slot the whole time.
	ctx, cancel := context.WithCancel(withHost(context.Background(), host))
	pool := buildFailoverPool(req.AuthID, req.Model)
	cands := pool.ordered(req.StorageJSON)
	go pumpWithFailover(ctx, cancel, req, body, upstreamModel, authUID, started, sseFramed, cands)
	return okEnvelope(streamResponse{Headers: headers})
}

// collectStreamAttempt runs one synchronous upstream stream attempt against
// cand, cleaning chunks through the collector. Mirrors the legacy
// collectUpstreamStream body, parameterized by candidate.
func collectStreamAttempt(ctx context.Context, body []byte, cand *failoverCandidate, sseFramed bool, collector *sseUsageCollector, metadata map[string]any) ([]pluginapi.ExecutorStreamChunk, int, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointChatFor(cand.Auth), bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	backendHeaders(httpReq, cand.Auth, canonicalSessionID(metadata))
	// Compliance: route via host.http.do_stream so request-log captures the call.
	stream, statusCode, _, err := hostHTTPDoStream(httpReq)
	if err != nil {
		return nil, 0, fmt.Errorf("http_error: %w", err)
	}
	defer stream.Close()
	reader := newHostStreamReader(stream)
	if statusCode >= 400 {
		errPayload, _ := io.ReadAll(reader)
		return nil, statusCode, fmt.Errorf("upstream %d: %s", statusCode, truncateRedacted(string(errPayload), 200))
	}
	chunks, errAgg := aggregateSSEWithCollector(reader, sseFramed, collector)
	if errAgg != nil {
		return chunks, statusCode, errAgg
	}
	if len(chunks) == 0 {
		// HTTP 200 but the upstream closed before any data chunk — an
		// upstream/request problem, never an account problem. Returning it as
		// an error here keeps the host from synthesizing a retryable
		// empty_stream and double-billing a second account.
		return nil, statusCode, errEmptyStream
	}
	return chunks, statusCode, nil
}

// onFailoverSuccess refreshes credits + panel selection after a successful
// chat. When the answering account differs from the routed one the active
// panel selection follows it so subsequent scheduler.pick calls stay on the
// healthy account.
func onFailoverSuccess(routedAuthID, attemptAuthID, authUID string) {
	if attemptAuthID != "" && attemptAuthID != routedAuthID {
		setActiveAuthID(attemptAuthID)
	}
	// The answering account's own UID is the correct credits key.
	fb, err := hostAuthGet(attemptAuthID)
	if err == nil && fb != nil && strings.TrimSpace(fb.Account.UID) != "" {
		authUID = fb.Account.UID
	}
	invalidateAccountCredits(attemptAuthID, authUID)
}

// -----------------------------------------------------------------------------

func okEnvelope(v any) ([]byte, error) {
	result, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{OK: true, Result: result})
}

func errorEnvelope(code, message string) []byte {
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message}})
	return raw
}

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}
