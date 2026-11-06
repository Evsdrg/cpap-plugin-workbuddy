// usage_config.go decodes plugin config from config_yaml on every
// register/reconfigure call and resolves the CPAMP usage report URL/key.
// All plugin-level config lives here so the rest of the plugin reads
// consistent, lock-protected snapshots.
package main

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/catalog"
	plinthconfig "github.com/Evsdrg/cepvor-cpa-plinth/config"
	"gopkg.in/yaml.v3"
)

// check-in schedule: 09:00 and 21:00 local time.
//
// 读取必须走 configuredCheckinHours：configure() 在重配置时整体替换这张表，
// 而签到循环与面板可能同时读它。
var (
	checkinHoursMu sync.RWMutex
	checkinHours   = []int{9, 21}
)

// configuredCheckinHours 返回当前签到时刻表的副本。
func configuredCheckinHours() []int {
	checkinHoursMu.RLock()
	defer checkinHoursMu.RUnlock()
	return append([]int(nil), checkinHours...)
}

// Defaults for the same-account retry policy (see failover.go). retry_per_account
// counts EXTRA attempts on the same account; retry_delay_seconds is the wait
// between them.
const (
	defaultRetryPerAccount   = 3
	defaultRetryDelaySeconds = 2
)

// plugin-level config decoded from plugin.register/reconfigure config_yaml.
var (
	checkinAuto   = true // enabled by default
	checkinAutoMu sync.RWMutex

	// usageReportURL / usageReportKey: POST NDJSON to CPA-Manager-Plus
	// /v0/management/usage/import (only path that reaches request monitoring;
	// c-shared plugins cannot use host usage.DefaultManager/redisqueue).
	//
	// Resolution order (community-style, like codex-auth-importer env injection):
	//  1) plugins.configs.workbuddy.usage_report_* in config.yaml
	//  2) env USAGE_REPORT_URL / USAGE_REPORT_KEY / CPAMP_ADMIN_KEY
	//  3) secret files (docker secrets / bind-mount), e.g. /run/secrets/cpamp_admin_key
	// Default URL targets the compose service name of CPA-Manager-Plus.
	usageReportURL = defaultUsageReportURL
	usageReportKey = ""
	usageReportMu  sync.RWMutex

	// managementAPIKey: plugin-layer auth for /v0/management/plugins/workbuddy/*
	// write endpoints. When empty, plugin relies on host-side auth (CPA's
	// management middleware) — that's the historical default and stays
	// backward-compatible. When set via config_yaml management_key: or env
	// WB_MANAGEMENT_KEY, handleManagement enforces constant-time Bearer match
	// plus per-IP token-bucket rate limiting on mutating endpoints.
	managementAPIKey   = ""
	managementAPIKeyMu sync.RWMutex
)

// Default URL tries localhost first (works for both bare-metal and Docker
// host-network), falls back to Docker compose service name. The probe runs
// once at configure() time; a reachable endpoint wins.
//
// For users who run CPA Manager Plus on a different host/port, set
// usage_report_url in plugin config or env USAGE_REPORT_URL.
const defaultUsageReportURL = "http://127.0.0.1:18317/v0/management/usage/import"

const fallbackUsageReportURL = "http://cpa-manager-plus:18317/v0/management/usage/import"

// configure decodes plugin config from the lifecycle request.
func configure(raw []byte) {
	// Parse config without holding any lock (fixes nested-lock hazard).
	nextCheckinAuto := true
	nextCheckinHours := []int{9, 21}
	nextLifecycleAuto := true
	nextSchedulerMode := schedulerModeOff // reset to default on reconfigure
	nextKeepaliveAuto := true
	nextDailyBonusAuto := true
	nextMgmtKey := ""
	// Same-account retry policy: reset to defaults on every reconfigure so a
	// removed setting falls back rather than sticking.
	nextRetryPerAccount := defaultRetryPerAccount
	nextRetryDelaySeconds := defaultRetryDelaySeconds
	// 目录刷新默认关闭：一次刷新要按账号各走一次上游往返，必须显式开启。
	nextModelRefresh := time.Duration(0)
	// 模型选择的最终决定权。nil 表示未配置（注册目录全量），
	// 空切片表示用户主动清空（注册零个）——两者语义不同，不能合并。
	var nextModelSelection []modelSelectionEntry

	cfgURL, cfgKey := "", ""
	if len(raw) > 0 {
		var req struct {
			ConfigYAML []byte `json:"config_yaml"`
		}
		if err := json.Unmarshal(raw, &req); err == nil {
			// 用 yaml.Unmarshal 而不是逐行 HasPrefix：逐行匹配会把
			// `checkin_auto: true   # 注释` 解析成 "true   # 注释"，比对失败后
			// 静默当成 false；它也认不了 flow style（`{checkin_auto: true}`）
			// 与嵌套结构，每加一个字段还得再写一个分支。
			var doc map[string]any
			if errYAML := yaml.Unmarshal(req.ConfigYAML, &doc); errYAML == nil && doc != nil {
				nextCheckinAuto = plinthconfig.Bool(doc["checkin_auto"], nextCheckinAuto)
				if hours := plinthconfig.Hours(doc["checkin_hours"], nextCheckinHours); len(hours) > 0 {
					nextCheckinHours = hours
				}
				// 键存在与否决定 nil 与空切片的差别，必须原样传递，
				// 不能在「解不出东西」时编一个空切片——那等于清空注册。
				if _, present := doc["model_selection"]; present {
					nextModelSelection = coerceModelSelection(doc["model_selection"])
				}
				nextLifecycleAuto = plinthconfig.Bool(doc["lifecycle_auto"], nextLifecycleAuto)
				nextKeepaliveAuto = plinthconfig.Bool(doc["token_keepalive"], nextKeepaliveAuto)
				nextDailyBonusAuto = plinthconfig.Bool(doc["daily_bonus"], nextDailyBonusAuto)
				if mode := plinthconfig.String(doc["scheduler_mode"], ""); mode == schedulerModeCredits {
					nextSchedulerMode = schedulerModeCredits
				}
				cfgURL = plinthconfig.String(doc["usage_report_url"], "")
				cfgKey = plinthconfig.String(doc["usage_report_key"], "")
				nextMgmtKey = plinthconfig.String(doc["management_key"], "")
				// 数值项保持「解析失败则沿用默认」的原语义：Int 的 fallback
				// 就是当前默认值。
				nextRetryPerAccount = plinthconfig.Int(doc["retry_per_account"], nextRetryPerAccount)
				nextRetryDelaySeconds = plinthconfig.Int(doc["retry_delay_seconds"], nextRetryDelaySeconds)
				nextModelRefresh = catalog.ParseInterval(doc["model_refresh_ms"])
			}
		}
	}

	// Apply each setting under its own lock — no nesting.
	checkinAutoMu.Lock()
	checkinAuto = nextCheckinAuto
	checkinAutoMu.Unlock()

	checkinHoursMu.Lock()
	checkinHours = nextCheckinHours
	checkinHoursMu.Unlock()

	applyModelSelection(nextModelSelection)

	lifecycleAutoMu.Lock()
	lifecycleAuto = nextLifecycleAuto
	lifecycleAutoMu.Unlock()

	schedulerModeMu.Lock()
	schedulerMode = nextSchedulerMode
	schedulerModeMu.Unlock()

	keepaliveAutoMu.Lock()
	keepaliveAuto = nextKeepaliveAuto
	keepaliveAutoMu.Unlock()

	dailyBonusAutoMu.Lock()
	dailyBonusAuto = nextDailyBonusAuto
	dailyBonusAutoMu.Unlock()

	setSameAccountRetry(nextRetryPerAccount, time.Duration(nextRetryDelaySeconds)*time.Second)

	// management key: config_yaml > env > keep existing. Empty stays empty
	// (plugin-layer auth disabled, host middleware still guards).
	if nextMgmtKey == "" {
		nextMgmtKey = strings.TrimSpace(os.Getenv("WB_MANAGEMENT_KEY"))
	}
	managementAPIKeyMu.Lock()
	managementAPIKey = nextMgmtKey
	managementAPIKeyMu.Unlock()

	resolveUsageReport(cfgURL, cfgKey)
	ensureSchedulerSource()
	configureCatalogueRefresh(nextModelRefresh)
}

// resolveUsageReport fills usageReportURL/key from config → env → secret files.
// Mirrors community plugins that inject management keys via env/build (e.g.
// codex-auth-importer CODEX_AUTH_IMPORTER_MANAGEMENT_KEY), not plaintext CPA
// remote-management.secret-key (that field is bcrypt-hashed).
func resolveUsageReport(cfgURL, cfgKey string) {
	url := firstNonEmpty(
		strings.TrimSpace(cfgURL),
		strings.TrimSpace(os.Getenv("USAGE_REPORT_URL")),
		strings.TrimSpace(os.Getenv("CPAMP_USAGE_IMPORT_URL")),
	)
	if url == "" {
		url = probeUsageReportURLSource()
	}
	key := firstNonEmpty(
		strings.TrimSpace(cfgKey),
		strings.TrimSpace(os.Getenv("USAGE_REPORT_KEY")),
		strings.TrimSpace(os.Getenv("CPAMP_ADMIN_KEY")),
		strings.TrimSpace(os.Getenv("CPA_MANAGER_ADMIN_KEY")),
		readSecretFile(os.Getenv("USAGE_REPORT_KEY_FILE")),
		readSecretFile(os.Getenv("CPAMP_ADMIN_KEY_FILE")),
		readSecretFile(os.Getenv("CPA_MANAGER_ADMIN_KEY_FILE")),
		// docker compose secrets default path
		readSecretFile("/run/secrets/cpamp_admin_key"),
		readSecretFile("/run/secrets/cpamp-admin-key"),
		// optional bind-mounts used on this host
		readSecretFile("/CLIProxyAPI/secrets/cpamp-admin-key"),
		readSecretFile("/CLIProxyAPI/secrets/cpamp_admin_key"),
	)
	usageReportMu.Lock()
	usageReportURL = url
	usageReportKey = key
	usageReportMu.Unlock()
}

// probeUsageReportURL tries localhost first (bare-metal + Docker host-network),
// then Docker compose service name. Returns whichever responds; defaults to
// localhost if both fail (better to try localhost than an unreachable hostname).
// probeUsageReportURLSource resolves the usage-report endpoint by probing the
// known local candidates. Tests override it: each probe waits up to two
// seconds, and a config test should not depend on whether some host happens
// to answer.
var probeUsageReportURLSource = probeUsageReportURL

func probeUsageReportURL() string {
	for _, candidate := range []string{defaultUsageReportURL, fallbackUsageReportURL} {
		if probeURL(candidate, 2*time.Second) {
			return candidate
		}
	}
	return defaultUsageReportURL
}

// probeURL does a quick HEAD/GET to check if the endpoint is reachable.
func probeURL(target string, timeout time.Duration) bool {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(target)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	// Any HTTP response (even 401) means the endpoint is reachable;
	// connection refused / DNS failure means not reachable.
	return resp.StatusCode > 0
}

func readSecretFile(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
