package main

import (
	"encoding/json"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// The host decodes plugin model lists into its own ModelInfo, which has a Rate
// field this plugin's vendored v7 ModelInfo lacks. The multiplier therefore has
// to ride along in the wire object; this pins that it survives the round trip.
func TestModelResponseWithRatesCarriesTheMultiplier(t *testing.T) {
	storeModelRate("glm-5.3", 0.79)
	storeModelRate("hy4-preview", 0)
	models := withRates([]pluginapi.ModelInfo{
		{ID: "glm-5.3", Name: "GLM-5.3"},
		{ID: "hy4-preview", Name: "Hy4 preview"},
	})
	raw, err := json.Marshal(modelResponseWithRates{Provider: providerName, Models: models})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded struct {
		Models []struct {
			ID   string
			Rate float64
		}
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Models[0].Rate != 0.79 {
		t.Fatalf("glm-5.3 rate = %v, want 0.79 (wire: %s)", decoded.Models[0].Rate, raw)
	}
	if decoded.Models[1].Rate != 0 {
		t.Fatalf("hy4-preview rate = %v, want 0 (no rate reported)", decoded.Models[1].Rate)
	}
}

func TestParseCreditsRateHandlesObservedShapes(t *testing.T) {
	cases := map[string]float64{
		"x0.79 credits": 0.79,
		"x0.00":         0,
		"x3.47":         3.47,
		"x3.31 credits": 3.31,
		"":              0,
		"garbage":       0,
	}
	for raw, want := range cases {
		if got := parseCreditsRate(raw); got != want {
			t.Fatalf("parseCreditsRate(%q) = %v, want %v", raw, got, want)
		}
	}
}
