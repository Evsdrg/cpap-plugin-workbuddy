package main

import (
	"encoding/json"
	"os"
	"testing"
)

// configYAML 把一段配置包成宿主传给 plugin.register/reconfigure 的载荷。
//
// 载荷里的 config_yaml 是 []byte，Go 的 encoding/json 按 base64 编码它；
// 宿主（internal/pluginhost/rpc_schema.go）也声明成 []byte，所以两边一致。
// 直接塞裸 YAML 字符串会得到「illegal base64」而让整段解析失败。
func configYAML(t *testing.T, yaml string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string][]byte{"config_yaml": []byte(yaml)})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestMain 把会触网/起后台循环的两处一次性换成固定值。
//
// 不能放在单个用例里临时替换：每个用例末尾都有 defer configure(nil)，
// 那时临时替换已经恢复，末尾这次 configure 会走真实探测（每个候选 2 秒）。
// usage-report 探测与 ensureScheduler 都与「配置解析」无关，全局关掉后
// 整包测试从十几秒降到毫秒级。
func TestMain(m *testing.M) {
	probeUsageReportURLSource = func() string { return "" }
	ensureSchedulerSource = func() {}
	os.Exit(m.Run())
}

// configureForTest 调 configure，并把与主机相关的环境变量显式置空，
// 否则断言会依赖跑测试的机器上恰好设了什么。
func configureForTest(t *testing.T, yaml string) {
	t.Helper()
	for _, key := range []string{"USAGE_REPORT_URL", "CPAMP_USAGE_IMPORT_URL", "USAGE_REPORT_KEY", "CPAMP_ADMIN_KEY", "CPA_MANAGER_ADMIN_KEY", "WB_MANAGEMENT_KEY"} {
		t.Setenv(key, "")
	}
	configure(configYAML(t, yaml))
}

// snapshot 读回当前生效的配置，避免每个用例重复加锁。
func snapshot(t *testing.T) (checkin, lifecycle, keepalive, daily bool, scheduler, usageURL, usageKey, mgmt string) {
	t.Helper()
	checkinAutoMu.Lock()
	checkin = checkinAuto
	checkinAutoMu.Unlock()
	lifecycleAutoMu.Lock()
	lifecycle = lifecycleAuto
	lifecycleAutoMu.Unlock()
	keepaliveAutoMu.Lock()
	keepalive = keepaliveAuto
	keepaliveAutoMu.Unlock()
	dailyBonusAutoMu.Lock()
	daily = dailyBonusAuto
	dailyBonusAutoMu.Unlock()
	schedulerModeMu.Lock()
	scheduler = schedulerMode
	schedulerModeMu.Unlock()
	usageReportMu.Lock()
	usageURL, usageKey = usageReportURL, usageReportKey
	usageReportMu.Unlock()
	managementAPIKeyMu.Lock()
	mgmt = managementAPIKey
	managementAPIKeyMu.Unlock()
	return
}

// 逐行 HasPrefix 会把 `checkin_auto: true   # 注释` 解析成 "true   # 注释"，
// 比对失败后静默当成 false——一条带注释的正常配置就这样关掉了自动签到。
func TestConfigureAcceptsTrailingComments(t *testing.T) {
	defer configure(nil)

	configureForTest(t, "checkin_auto: false   # 暂时关掉\n")
	checkin, _, _, _, _, _, _, _ := snapshot(t)
	if checkin {
		t.Fatal("checkin_auto: false with a trailing comment must still disable it")
	}

	configureForTest(t, "checkin_auto: true # 打开\n")
	checkin, _, _, _, _, _, _, _ = snapshot(t)
	if !checkin {
		t.Fatal("checkin_auto: true with a trailing comment must still enable it")
	}
}

// YAML 里不加引号的 1 是 int，不是 bool。逐行匹配只比对字符串，会漏掉它。
func TestConfigureAcceptsNumericBooleans(t *testing.T) {
	defer configure(nil)

	configureForTest(t, "checkin_auto: 1\nlifecycle_auto: 0\n")
	checkin, lifecycle, _, _, _, _, _, _ := snapshot(t)
	if !checkin {
		t.Fatal("checkin_auto: 1 must read as true")
	}
	if lifecycle {
		t.Fatal("lifecycle_auto: 0 must read as false")
	}
}

// flow style 在逐行匹配下完全无效，yaml.Unmarshal 必须能处理。
func TestConfigureAcceptsFlowStyle(t *testing.T) {
	defer configure(nil)

	configureForTest(t, "{checkin_auto: false, token_keepalive: false}\n")
	checkin, _, keepalive, _, _, _, _, _ := snapshot(t)
	if checkin {
		t.Fatal("flow style checkin_auto: false must read as false")
	}
	if keepalive {
		t.Fatal("flow style token_keepalive: false must read as false")
	}
}

// 重新配置时必须回到默认值：被删掉的键不能「粘住」上一次的取值。
func TestConfigureResetsRemovedKeysToDefaults(t *testing.T) {
	defer configure(nil)

	configureForTest(t, "checkin_auto: false\nlifecycle_auto: false\nscheduler_mode: credits\n")
	if checkin, lifecycle, _, _, scheduler, _, _, _ := snapshot(t); checkin || lifecycle || scheduler != schedulerModeCredits {
		t.Fatalf("first configure did not apply: checkin=%v lifecycle=%v scheduler=%q", checkin, lifecycle, scheduler)
	}

	// 空配置：所有键都消失，应回到默认（checkin/lifecycle 默认 true，scheduler 默认 off）。
	configureForTest(t, "")
	checkin, lifecycle, keepalive, daily, scheduler, _, _, _ := snapshot(t)
	if !checkin || !lifecycle || !keepalive || !daily {
		t.Fatalf("defaults not restored: checkin=%v lifecycle=%v keepalive=%v daily=%v",
			checkin, lifecycle, keepalive, daily)
	}
	if scheduler != schedulerModeOff {
		t.Fatalf("scheduler mode = %q, want %q after the key disappeared", scheduler, schedulerModeOff)
	}
}

// 字符串值带引号或不带引号都必须能读出来。
func TestConfigureReadsQuotedAndBareStrings(t *testing.T) {
	defer configure(nil)

	configureForTest(t, "usage_report_url: \"https://example.test/usage\"\nmanagement_key: secret-1\n")
	_, _, _, _, _, usageURL, _, mgmt := snapshot(t)
	if usageURL != "https://example.test/usage" {
		t.Fatalf("usage_report_url = %q", usageURL)
	}
	if mgmt != "secret-1" {
		t.Fatalf("management_key = %q", mgmt)
	}
}

// 数值项解析失败时沿用默认值，不是归零。retry_per_account=0 是合法配置
// （关闭同账号重试），所以「解析失败」不能和「显式写 0」混为一谈。
func TestConfigureRetryNumbersFallBackOnGarbage(t *testing.T) {
	defer configure(nil)

	configureForTest(t, "retry_per_account: not-a-number\nretry_delay_seconds: 7\n")
	if got := sameAccountRetryLimit(); got != defaultRetryPerAccount {
		t.Fatalf("retry_per_account = %d, want the default %d", got, defaultRetryPerAccount)
	}
	if got := sameAccountRetryDelayValue(); got != 7*1e9 {
		t.Fatalf("retry_delay_seconds = %v, want 7s", got)
	}

	configureForTest(t, "retry_per_account: 0\n")
	if got := sameAccountRetryLimit(); got != 0 {
		t.Fatalf("an explicit 0 must be honoured, got %d", got)
	}
}

// 未知键必须被忽略而不是让整份配置解析失败。
func TestConfigureIgnoresUnknownKeys(t *testing.T) {
	defer configure(nil)

	configureForTest(t, "checkin_auto: false\nsome_future_key: whatever\n")
	checkin, _, _, _, _, _, _, _ := snapshot(t)
	if checkin {
		t.Fatal("a known key next to an unknown one must still apply")
	}
}

// 非 YAML 的载荷不能让配置变成空值：解析失败时保持当前默认。
func TestConfigureToleratesMalformedYAML(t *testing.T) {
	defer configure(nil)

	configureForTest(t, "checkin_auto: false\n")
	configureForTest(t, "::: not yaml :::")
	checkin, _, _, _, _, _, _, _ := snapshot(t)
	if !checkin {
		t.Fatal("malformed YAML must reset to defaults rather than keep the previous value")
	}
}
