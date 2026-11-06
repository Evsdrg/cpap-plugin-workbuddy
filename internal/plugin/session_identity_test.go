package main

import "testing"

// TestCanonicalSessionIDWins pins the precedence: when the host supplies a
// canonical session id it must be used verbatim, not replaced by a body hash.
func TestCanonicalSessionIDWins(t *testing.T) {
	metadata := map[string]any{canonicalSessionIDMetadataKey: "zcode:abc-123"}
	got := canonicalSessionID(metadata)
	if got != "zcode:abc-123" {
		t.Fatalf("canonicalSessionID = %q, want the host value", got)
	}
}

// TestCanonicalSessionIDMissing covers the fallback contract: absent, nil or a
// non-string value must all yield empty so the caller derives a body bucket.
func TestCanonicalSessionIDMissing(t *testing.T) {
	cases := map[string]map[string]any{
		"nil map":       nil,
		"empty map":     {},
		"nil value":     {canonicalSessionIDMetadataKey: nil},
		"non-string":    {canonicalSessionIDMetadataKey: 42},
		"empty string":  {canonicalSessionIDMetadataKey: "   "},
		"different key": {"session": "x"},
	}
	for name, metadata := range cases {
		t.Run(name, func(t *testing.T) {
			if got := canonicalSessionID(metadata); got != "" {
				t.Fatalf("canonicalSessionID = %q, want empty", got)
			}
		})
	}
}
