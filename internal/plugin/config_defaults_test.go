package main

import (
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// The panel renders `Default` under every control, so a wrong value there is a
// silent lie: the operator sees "默认 X", leaves the field blank, and gets
// whatever the code actually does. This test ties every declared default to the
// value configure() really applies when the key is absent, so the two cannot
// drift apart.
func TestConfigFieldDefaultsMatchConfigure(t *testing.T) {
	defaults := map[string]any{
		"checkin_auto":        true,
		"lifecycle_auto":      true,
		"token_keepalive":     true,
		"daily_bonus":         true,
		"scheduler_mode":      schedulerModeOff,
		"retry_per_account":   defaultRetryPerAccount,
		"retry_delay_seconds": defaultRetryDelaySeconds,
	}

	for _, field := range configurationFields() {
		want, tracked := defaults[field.Name]
		if !tracked {
			continue
		}
		if fmt.Sprint(field.Default) != fmt.Sprint(want) {
			t.Errorf("%s: declared default %#v, configure() applies %#v", field.Name, field.Default, want)
		}
	}
}

// Every field must carry a short label. Without one the panel carves a label
// out of Description, producing rows of wildly different lengths because
// descriptions are full sentences.
func TestEveryConfigFieldDeclaresALabel(t *testing.T) {
	for _, field := range configurationFields() {
		if field.Label == "" {
			t.Errorf("%s declares no label; the panel would derive one from the description", field.Name)
		}
	}
}

// configurationFields reaches the same literal the plugin registers.
func configurationFields() []pluginapi.ConfigField {
	return wbRegistration().Metadata.ConfigFields
}
