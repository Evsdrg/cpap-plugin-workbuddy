package main

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestSameModelIDSet(t *testing.T) {
	cases := []struct {
		name     string
		previous []string
		current  []pluginapi.ModelInfo
		want     bool
	}{
		{"identical order", []string{"a", "b"}, []pluginapi.ModelInfo{{ID: "a"}, {ID: "b"}}, true},
		{"different order", []string{"a", "b"}, []pluginapi.ModelInfo{{ID: "b"}, {ID: "a"}}, true},
		{"added", []string{"a"}, []pluginapi.ModelInfo{{ID: "a"}, {ID: "b"}}, false},
		{"removed", []string{"a", "b"}, []pluginapi.ModelInfo{{ID: "a"}}, false},
		{"replaced", []string{"a"}, []pluginapi.ModelInfo{{ID: "b"}}, false},
		{"both empty", nil, nil, true},
		{"from empty", nil, []pluginapi.ModelInfo{{ID: "a"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameModelIDSet(tc.previous, tc.current); got != tc.want {
				t.Fatalf("sameModelIDSet = %v, want %v", got, tc.want)
			}
		})
	}
}

// 顺序与元数据不参与判定：只有 id 集合影响注册结果。一个模型改了 context
// 窗口但 id 没变，不该触发一次凭据回写。
func TestSameModelIDSetIgnoresMetadata(t *testing.T) {
	previous := []string{"glm-5.3"}
	current := []pluginapi.ModelInfo{{ID: "glm-5.3", Name: "renamed", ContextLength: 999}}
	if !sameModelIDSet(previous, current) {
		t.Fatal("a metadata-only change must not count as a catalogue change")
	}
}

// 刷新前必须丢掉缓存，否则重拉会命中旧值、Changed 永远为 false。
func TestInvalidateDynamicModelsCacheClearsSnapshot(t *testing.T) {
	storeDynamicModels("cn:free", []pluginapi.ModelInfo{{ID: "m1"}})
	if ids := dynamicModelIDs(); len(ids) != 1 {
		t.Fatalf("precondition failed: %v", ids)
	}
	invalidateDynamicModelsCache()
	if ids := dynamicModelIDs(); ids != nil {
		t.Fatalf("after invalidation dynamicModelIDs = %v, want nil", ids)
	}
	if _, ok := cachedDynamicModels("cn:free"); ok {
		t.Fatal("cachedDynamicModels must miss after invalidation")
	}
}

// 间隔为 0 时调度器必须保持停止：一次刷新要按账号各走一次上游往返。
func TestCatalogueSchedulerStaysStoppedAtZeroInterval(t *testing.T) {
	defer configureCatalogueRefresh(0)

	configureCatalogueRefresh(0)
	if catalogueScheduler.Enabled() {
		t.Fatal("a zero interval must leave the catalogue scheduler stopped")
	}
}

// model_refresh_ms 必须真的接进调度器：写上间隔后要能启用。
func TestConfigureWiresModelRefreshInterval(t *testing.T) {
	defer configureCatalogueRefresh(0)
	defer catalogueScheduler.Stop()

	configureForTest(t, "model_refresh_ms: 600000\n")
	interval, _, _, _ := catalogueStatus()
	if interval != 10*time.Minute {
		t.Fatalf("scheduler interval = %v, want 10m from model_refresh_ms: 600000", interval)
	}
	if !catalogueScheduler.Enabled() {
		t.Fatal("a non-zero model_refresh_ms must enable the scheduler")
	}
}

// 默认值必须在面板声明与真实行为之间一致。
func TestModelRefreshDefaultsOff(t *testing.T) {
	var declared any
	for _, field := range wbRegistration().Metadata.ConfigFields {
		if field.Name == "model_refresh_ms" {
			declared = field.Default
		}
	}
	if declared == nil {
		t.Fatal("model_refresh_ms is not declared in ConfigFields")
	}
	if got, ok := declared.(int); !ok || got != 0 {
		t.Fatalf("declared default = %#v (%T), want int 0", declared, declared)
	}
}
