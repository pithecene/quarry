package lode

import (
	"testing"

	"github.com/pithecene/quarry/types"
)

func TestToEventRecordMap_IncludesPolicy(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-06",
		RunID:    "run-001",
		Policy:   "strict",
	}

	envelope := &types.EventEnvelope{
		ContractVersion: "1.0.0",
		EventID:         "evt-1",
		RunID:           "run-001",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2026-02-06T12:00:00Z",
		Payload:         map[string]any{"key": "value"},
		Attempt:         1,
	}

	record := toEventRecordMap(envelope, cfg)

	got, ok := record["policy"]
	if !ok {
		t.Fatal("record missing 'policy' key")
	}
	if got != "strict" {
		t.Errorf("policy = %q, want %q", got, "strict")
	}
}

func TestToArtifactCommitRecordMap_IncludesPolicy(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-06",
		RunID:    "run-001",
		Policy:   "buffered",
	}

	envelope := &types.EventEnvelope{
		ContractVersion: "1.0.0",
		EventID:         "evt-2",
		RunID:           "run-001",
		Seq:             2,
		Type:            types.EventTypeArtifact,
		Ts:              "2026-02-06T12:00:01Z",
		Payload: map[string]any{
			"artifact_id":  "art-001",
			"name":         "screenshot.png",
			"content_type": "image/png",
			"size_bytes":   float64(1024),
		},
		Attempt: 1,
	}

	record := toArtifactCommitRecordMap(envelope, cfg)

	got, ok := record["policy"]
	if !ok {
		t.Fatal("record missing 'policy' key")
	}
	if got != "buffered" {
		t.Errorf("policy = %q, want %q", got, "buffered")
	}
}
