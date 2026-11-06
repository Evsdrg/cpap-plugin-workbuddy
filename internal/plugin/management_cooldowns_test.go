package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestSnapshotWorkbuddyCooldownsReturnsActiveAccountAndModelEntries(t *testing.T) {
	for _, id := range []string{"idx-cooldown", "record-cooldown", "uid-cooldown"} {
		resetAuthCooldown(id)
	}
	t.Cleanup(func() {
		for _, id := range []string{"idx-cooldown", "record-cooldown", "uid-cooldown"} {
			resetAuthCooldown(id)
		}
	})

	now := time.Now()
	markAuthCooldownReason("idx-cooldown", now.Add(5*time.Minute), "429")
	markModelCooldown("idx-cooldown", "deepseek-v4.1-flash", now.Add(30*time.Minute), cooldownReasonEmptyStreamQuota)
	markModelCooldown("record-cooldown", "deepseek-v4.1-flash", now.Add(30*time.Minute), cooldownReasonEmptyStreamQuota)
	cooldownMu.Lock()
	cooldowns[modelCooldownKey("uid-cooldown", "glm-5.3")] = authCooldown{NextRetryAt: now.Add(-time.Minute), Reason: cooldownReasonFirstTokenTimeout}
	cooldownMu.Unlock()

	entries := snapshotWorkbuddyCooldowns("idx-cooldown", []string{"idx-cooldown", "record-cooldown", "uid-cooldown"}, now)
	if len(entries) != 2 {
		t.Fatalf("cooldowns = %+v, want one account and one active model cooldown", entries)
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		if entry.AuthIndex != "idx-cooldown" || entry.Source != "workbuddy" || entry.RemainingSeconds <= 0 {
			t.Fatalf("invalid cooldown entry: %+v", entry)
		}
		seen[entry.Scope+":"+entry.Model] = true
	}
	if !seen["account:"] || !seen["model:deepseek-v4.1-flash"] {
		t.Fatalf("cooldown scopes = %v", seen)
	}
}

func TestResetCooldownsForIdentitiesClearsAllRelatedEntriesAndStrikes(t *testing.T) {
	for _, id := range []string{"idx-reset", "record-reset", "other-reset"} {
		resetAuthCooldown(id)
	}
	resetStrikes()
	markAuthCooldownReason("idx-reset", time.Now().Add(time.Hour), "429")
	markModelCooldown("record-reset", "deepseek-v4.1-flash", time.Now().Add(time.Hour), cooldownReasonEmptyStreamQuota)
	markModelCooldown("other-reset", "glm-5.3", time.Now().Add(time.Hour), cooldownReasonEmptyStreamQuota)
	for i := 0; i < emptyStreamStrikeThreshold-1; i++ {
		if recordEmptyStreamStrike("record-reset", "deepseek-v4.1-flash") {
			t.Fatal("strike threshold reached before reset")
		}
	}

	cleared := resetCooldownsForIdentities([]string{"idx-reset", "record-reset"})
	if cleared != 2 {
		t.Fatalf("cleared = %d, want 2 account/model cooldown entries", cleared)
	}
	if isAuthOnCooldown("idx-reset") || isModelOnCooldown("record-reset", "deepseek-v4.1-flash") {
		t.Fatal("matching cooldown remained after reset")
	}
	if _, ok := cooldownFor(modelCooldownKey("other-reset", "glm-5.3")); !ok {
		t.Fatal("unrelated account cooldown was cleared")
	}
	if recordEmptyStreamStrike("record-reset", "deepseek-v4.1-flash") {
		t.Fatal("empty-stream strike counter was not reset")
	}
}

func TestManagementRegistrationDeclaresSharedCooldownContract(t *testing.T) {
	registration := managementRegistration()
	found := map[string]string{}
	for _, route := range registration.Routes {
		found[route.Method+" "+route.Path] = route.Description
	}
	for _, route := range []string{
		"GET /plugins/workbuddy/cooldowns",
		"POST /plugins/workbuddy/cooldowns/reset",
	} {
		if found[route] == "" {
			t.Fatalf("management route %q is not declared", route)
		}
	}
}

func TestCooldownResetRequestAllowsAccountWideResetWithEmptyModel(t *testing.T) {
	request := pluginapi.PluginCooldownResetRequest{AuthIndex: "idx-reset-contract"}
	if request.AuthIndex != "idx-reset-contract" || request.Model != "" {
		t.Fatalf("empty model request = %+v", request)
	}
}

func TestManagementCooldownListAndResetUseHostIdentityAndSharedContract(t *testing.T) {
	for _, id := range []string{"idx-management", "workbuddy-management.json", "uid-management"} {
		resetAuthCooldown(id)
	}
	resetStrikes()
	markModelCooldown("uid-management", "deepseek-v4.1-flash", time.Now().Add(time.Hour), cooldownReasonEmptyStreamQuota)

	abi.SetHostCaller(func(method string, request []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostAuthList:
			return []byte(`{"ok":true,"result":{"files":[{"id":"workbuddy-record","auth_index":"idx-management","name":"workbuddy-management.json"}]}}`), nil
		case pluginabi.MethodHostAuthGet:
			return []byte(`{"ok":true,"result":{"auth_index":"idx-management","name":"workbuddy-management.json","json":{"auth":{"accessToken":"test-access","refreshToken":"test-refresh","domain":"global"},"account":{"uid":"uid-management"}}}}`), nil
		case pluginabi.MethodHostRoutingResetModelCooldown:
			var req pluginapi.HostRoutingResetModelCooldownRequest
			if err := json.Unmarshal(request, &req); err != nil {
				return nil, err
			}
			if req.AuthIndex != "idx-management" || req.Model != "deepseek-v4.1-flash" {
				t.Fatalf("host reset request = %+v", req)
			}
			return []byte(`{"ok":true,"result":{"auth_index":"idx-management","model":"deepseek-v4.1-flash","cleared":true}}`), nil
		case pluginabi.MethodHostRoutingResetCooldown:
			return []byte(`{"ok":true,"result":{"auth_index":"idx-management","models":["deepseek-v4.1-flash"]}}`), nil
		default:
			t.Fatalf("unexpected host callback %q", method)
			return nil, nil
		}
	})
	defer abi.ClearHostCaller()

	files, err := hostAuthList()
	if err != nil || len(files) != 1 || files[0].AuthIndex != "idx-management" {
		t.Fatalf("fake host auth list = %+v err=%v", files, err)
	}
	stored, err := hostAuthGet("idx-management")
	if err != nil || stored == nil || stored.Account.UID != "uid-management" {
		t.Fatalf("fake host auth get = %+v err=%v", stored, err)
	}
	listResponse := handleCooldownsList(pluginapi.ManagementRequest{Query: url.Values{"auth_index": {"idx-management"}}})
	if listResponse.StatusCode != http.StatusOK {
		t.Fatalf("cooldown list status = %d: %s", listResponse.StatusCode, listResponse.Body)
	}
	var listed pluginapi.PluginCooldownListResponse
	if err := json.Unmarshal(listResponse.Body, &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Cooldowns) != 1 || listed.Cooldowns[0].Scope != "model" || listed.Cooldowns[0].Model != "deepseek-v4.1-flash" {
		t.Fatalf("cooldown list = %+v", listed)
	}

	request, err := json.Marshal(pluginapi.PluginCooldownResetRequest{AuthIndex: "idx-management", Model: "deepseek-v4.1-flash"})
	if err != nil {
		t.Fatal(err)
	}
	resetResponse := handleCooldownReset(pluginapi.ManagementRequest{Body: request})
	if resetResponse.StatusCode != http.StatusOK {
		t.Fatalf("cooldown reset status = %d: %s", resetResponse.StatusCode, resetResponse.Body)
	}
	var reset pluginapi.PluginCooldownResetResponse
	if err := json.Unmarshal(resetResponse.Body, &reset); err != nil {
		t.Fatal(err)
	}
	if !reset.Cleared || reset.AuthIndex != "idx-management" || reset.Model != "deepseek-v4.1-flash" {
		t.Fatalf("cooldown reset response = %+v", reset)
	}
	if isModelOnCooldown("uid-management", "deepseek-v4.1-flash") {
		t.Fatal("plugin-private model cooldown remained after shared reset")
	}

	markAuthCooldownReason("uid-management", time.Now().Add(time.Hour), "429")
	markModelCooldown("uid-management", "glm-5.3", time.Now().Add(time.Hour), cooldownReasonEmptyStreamQuota)
	allResetRequest, err := json.Marshal(pluginapi.PluginCooldownResetRequest{AuthIndex: "idx-management"})
	if err != nil {
		t.Fatal(err)
	}
	allResetResponse := handleCooldownReset(pluginapi.ManagementRequest{Body: allResetRequest})
	if allResetResponse.StatusCode != http.StatusOK {
		t.Fatalf("account cooldown reset status = %d: %s", allResetResponse.StatusCode, allResetResponse.Body)
	}
	if isAuthOnCooldown("uid-management") || isModelOnCooldown("uid-management", "glm-5.3") {
		t.Fatal("account-wide reset left plugin-private cooldowns behind")
	}
}
