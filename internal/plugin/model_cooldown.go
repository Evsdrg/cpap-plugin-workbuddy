package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Evsdrg/cepvor-cpa-plinth/abi"
	"github.com/Evsdrg/cepvor-cpa-plinth/logging"
)

const hostRoutingSetModelCooldown = "host.routing.set_model_cooldown"

func setHostModelCooldown(authID, model string, until time.Time, reason string) error {
	files, err := hostAuthList()
	if err != nil {
		return fmt.Errorf("列出账号失败: %w", err)
	}
	authID = strings.TrimSpace(authID)
	for _, file := range files {
		if file.AuthIndex != authID && file.ID != authID && file.Name != authID {
			continue
		}
		request, err := json.Marshal(map[string]any{
			"auth_index": file.AuthIndex,
			"model":      model,
			"until":      until,
			"reason":     reason,
		})
		if err != nil {
			return err
		}
		if _, err := abi.HostCall(hostRoutingSetModelCooldown, json.RawMessage(request)); err != nil {
			return err
		}
		// 冷却写入成功也要留一行：这是「某个模型在这个账号上被摘掉」的唯一
		// 痕迹，否则事后只能从「为什么没选这个账号」反推。
		logging.Warn(&abi.Host{}, "Workbuddy.cooldown：模型已进入冷却",
			logging.Auth(authID),
			logging.Model(model),
			logging.Reason(reason))
		return nil
	}
	return fmt.Errorf("账号 %q 不存在", authID)
}

func hostLogCooldownFailure(authID, model string, cooldownErr error) {
	logging.Error(&abi.Host{}, "Workbuddy.cooldown：模型冷却写入失败",
		logging.Auth(authID),
		logging.Model(model),
		logging.Err(cooldownErr))
}
