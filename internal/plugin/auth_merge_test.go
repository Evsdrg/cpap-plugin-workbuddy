// auth_merge_test.go 钉住「保存凭据时不能抹掉宿主自己的路由字段」。
//
// 宿主在 auth 文件里持有 priority / weight，它据此决定选哪个账号。插件保存
// 凭据时只构造自己认识的成员（type/provider/logo/disabled/note/auth/account），
// 若不做合并，这两个字段会被抹掉，凭据静默掉回默认路由档位 —— 一个只在
// 多账号部署里才看得出来的问题。
package main

import (
	"encoding/json"
	"testing"

	"github.com/Evsdrg/cepvor-cpa-plinth/credjson"
)

// TestSaveKeepsHostOwnedRoutingMembers 验证合并后 priority / weight 仍在。
func TestSaveKeepsHostOwnedRoutingMembers(t *testing.T) {
	hostHeld := []byte(`{
		"type":"workbuddy","provider":"workbuddy",
		"disabled":false,"note":"",
		"priority":7,"weight":3,
		"auth":{"access_token":"old"},
		"account":{"uid":"u1"}
	}`)
	pluginWritten := []byte(`{
		"type":"workbuddy","provider":"workbuddy",
		"disabled":false,"note":"refreshed",
		"auth":{"access_token":"new"},
		"account":{"uid":"u1"}
	}`)

	merged := credjson.MergePreserved(hostHeld, pluginWritten)

	var decoded map[string]any
	if err := json.Unmarshal(merged, &decoded); err != nil {
		t.Fatalf("merged JSON invalid: %v", err)
	}
	if got, _ := decoded["priority"].(float64); got != 7 {
		t.Fatalf("priority = %v, want 7 (host-owned member was dropped)", decoded["priority"])
	}
	if got, _ := decoded["weight"].(float64); got != 3 {
		t.Fatalf("weight = %v, want 3 (host-owned member was dropped)", decoded["weight"])
	}
	// 插件自己写的成员必须胜出，不能被宿主旧值覆盖。
	auth, _ := decoded["auth"].(map[string]any)
	if got, _ := auth["access_token"].(string); got != "new" {
		t.Fatalf("access_token = %q, want new (plugin write must win)", got)
	}
	if got, _ := decoded["note"].(string); got != "refreshed" {
		t.Fatalf("note = %q, want refreshed", got)
	}
}

// TestSaveWithoutHostFileIsUnchanged 验证首次登录（宿主还没有文件）时原样写入。
func TestSaveWithoutHostFileIsUnchanged(t *testing.T) {
	pluginWritten := []byte(`{"type":"workbuddy","auth":{"access_token":"new"}}`)
	merged := credjson.MergePreserved(nil, pluginWritten)
	if string(merged) != string(pluginWritten) {
		t.Fatalf("merge with empty base changed the payload: %s", merged)
	}
}
