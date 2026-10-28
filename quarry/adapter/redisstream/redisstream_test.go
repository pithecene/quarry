package redisstream

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

func testEvents() []*types.EventEnvelope {
	return []*types.EventEnvelope{
		{
			ContractVersion: "0.14.2",
			EventID:         "evt-001",
			RunID:           "run-001",
			Seq:             1,
			Type:            types.EventTypeItem,
			Ts:              "2026-03-12T12:00:00Z",
			Payload:         map[string]any{"item_type": "page", "data": map[string]any{"url": "https://example.com"}},
			Attempt:         1,
		},
		{
			ContractVersion: "0.14.2",
			EventID:         "evt-002",
			RunID:           "run-001",
			Seq:             2,
			Type:            types.EventTypeLog,
			Ts:              "2026-03-12T12:00:01Z",
			Payload:         map[string]any{"level": "info", "message": "page loaded"},
			Attempt:         1,
		},
	}
}

// streamValues converts a miniredis StreamEntry's Values (alternating k/v slice)
// into a map for easier assertions.
func streamValues(entry miniredis.StreamEntry) map[string]string {
	m := make(map[string]string, len(entry.Values)/2)
	for i := 0; i+1 < len(entry.Values); i += 2 {
		m[entry.Values[i]] = entry.Values[i+1]
	}
	return m
}

func TestWriteEvents_Success(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	s, err := New(Config{
		URL:      "redis://" + mr.Addr(),
		Source:   "test-source",
		Category: "default",
		Retries:  0,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	events := testEvents()
	if err := s.WriteEvents(t.Context(), events); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Verify stream entries via miniredis
	stream, err := mr.Stream(DefaultStreamKey)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if len(stream) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(stream))
	}

	// Check first entry fields
	vals := streamValues(stream[0])
	if vals["run_id"] != "run-001" {
		t.Errorf("expected run_id=run-001, got %s", vals["run_id"])
	}
	if vals["event_type"] != "item" {
		t.Errorf("expected event_type=item, got %s", vals["event_type"])
	}
	if vals["seq"] != "1" {
		t.Errorf("expected seq=1, got %s", vals["seq"])
	}
	if vals["source"] != "test-source" {
		t.Errorf("expected source=test-source, got %s", vals["source"])
	}
	if vals["category"] != "default" {
		t.Errorf("expected category=default, got %s", vals["category"])
	}

	// Verify payload is valid JSON
	var payload map[string]any
	if err := json.Unmarshal([]byte(vals["payload"]), &payload); err != nil {
		t.Fatalf("payload not valid JSON: %v", err)
	}
	if payload["item_type"] != "page" {
		t.Errorf("expected item_type=page in payload, got %v", payload["item_type"])
	}

	// Check second entry
	vals2 := streamValues(stream[1])
	if vals2["event_type"] != "log" {
		t.Errorf("expected event_type=log, got %s", vals2["event_type"])
	}
	if vals2["seq"] != "2" {
		t.Errorf("expected seq=2, got %s", vals2["seq"])
	}
}

func TestWriteEvents_EmptyBatch(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	s, err := New(Config{URL: "redis://" + mr.Addr(), Retries: 0})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	// Empty batch should be a no-op
	if err := s.WriteEvents(t.Context(), nil); err != nil {
		t.Fatalf("write nil: %v", err)
	}
	if err := s.WriteEvents(t.Context(), []*types.EventEnvelope{}); err != nil {
		t.Fatalf("write empty: %v", err)
	}
}

func TestWriteEvents_CustomStreamKey(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	customKey := "myapp:events"
	s, err := New(Config{
		URL:       "redis://" + mr.Addr(),
		StreamKey: customKey,
		Retries:   0,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	if err := s.WriteEvents(t.Context(), testEvents()); err != nil {
		t.Fatalf("write: %v", err)
	}

	stream, err := mr.Stream(customKey)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if len(stream) != 2 {
		t.Fatalf("expected 2 entries in custom stream, got %d", len(stream))
	}
}

func TestWriteEvents_ExhaustsRetries(t *testing.T) {
	t.Parallel()

	s, err := New(Config{
		URL:     "redis://127.0.0.1:1",
		Retries: 1,
		Timeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	err = s.WriteEvents(t.Context(), testEvents())
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
}

func TestWriteEvents_ContextCanceled(t *testing.T) {
	t.Parallel()

	s, err := New(Config{
		URL:     "redis://127.0.0.1:1",
		Retries: 5,
		Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err = s.WriteEvents(ctx, testEvents())
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}

func TestNew_RequiresURL(t *testing.T) {
	t.Parallel()

	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestNew_InvalidURL(t *testing.T) {
	t.Parallel()

	_, err := New(Config{URL: "not-a-redis-url"})
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestNew_RejectsNegativeRetries(t *testing.T) {
	t.Parallel()

	_, err := New(Config{URL: "redis://localhost:6379", Retries: -1})
	if err == nil {
		t.Fatal("expected error for negative retries")
	}
}

func TestNew_DefaultsApplied(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	s, err := New(Config{URL: "redis://" + mr.Addr()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	if s.config.StreamKey != DefaultStreamKey {
		t.Errorf("expected default stream key %q, got %q", DefaultStreamKey, s.config.StreamKey)
	}
	if s.config.MaxLen != DefaultMaxLen {
		t.Errorf("expected default max len %d, got %d", DefaultMaxLen, s.config.MaxLen)
	}
	if s.config.TTL != DefaultTTL {
		t.Errorf("expected default TTL %v, got %v", DefaultTTL, s.config.TTL)
	}
	if s.config.Timeout != DefaultTimeout {
		t.Errorf("expected default timeout %v, got %v", DefaultTimeout, s.config.Timeout)
	}
}

func TestClose_ClosesConnection(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	s, err := New(Config{URL: "redis://" + mr.Addr()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Write after close should fail
	err = s.WriteEvents(t.Context(), testEvents())
	if err == nil {
		t.Fatal("expected error after close")
	}
}

func TestWriteEvents_AppliesTTL(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	s, err := New(Config{
		URL:     "redis://" + mr.Addr(),
		TTL:     1 * time.Hour,
		Retries: 0,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	if err := s.WriteEvents(t.Context(), testEvents()); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Verify TTL was set on the stream key
	ttl := mr.TTL(DefaultStreamKey)
	if ttl <= 0 {
		t.Errorf("expected positive TTL on stream key, got %v", ttl)
	}
}

func TestWriteEvents_TTLDisabled(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	s, err := New(Config{
		URL:     "redis://" + mr.Addr(),
		TTL:     -1, // disabled
		Retries: 0,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	if err := s.WriteEvents(t.Context(), testEvents()); err != nil {
		t.Fatalf("write: %v", err)
	}

	// No TTL should be set
	ttl := mr.TTL(DefaultStreamKey)
	if ttl > 0 {
		t.Errorf("expected no TTL when disabled, got %v", ttl)
	}
}

func TestWriteEvents_MaxLenDisabled(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	s, err := New(Config{
		URL:     "redis://" + mr.Addr(),
		MaxLen:  -1, // disabled
		TTL:     -1,
		Retries: 0,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	if err := s.WriteEvents(t.Context(), testEvents()); err != nil {
		t.Fatalf("write: %v", err)
	}

	// All entries should be present (no trimming)
	stream, err := mr.Stream(DefaultStreamKey)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if len(stream) != 2 {
		t.Fatalf("expected 2 entries (no trimming), got %d", len(stream))
	}
}

func TestWriteEvents_TTLAppliedOnce(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	s, err := New(Config{
		URL:     "redis://" + mr.Addr(),
		TTL:     2 * time.Hour,
		Retries: 0,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	// First write sets TTL
	if err := s.WriteEvents(t.Context(), testEvents()); err != nil {
		t.Fatalf("first write: %v", err)
	}
	ttl1 := mr.TTL(DefaultStreamKey)
	if ttl1 <= 0 {
		t.Fatalf("expected positive TTL after first write, got %v", ttl1)
	}

	// Fast-forward time so we can detect if TTL is re-applied
	mr.FastForward(30 * time.Minute)

	// Second write should NOT reset TTL
	if err := s.WriteEvents(t.Context(), testEvents()); err != nil {
		t.Fatalf("second write: %v", err)
	}
	ttl2 := mr.TTL(DefaultStreamKey)

	// TTL should be lower than original (time passed), not reset to 2h
	if ttl2 > ttl1 {
		t.Errorf("TTL should not increase after second write: was %v, now %v", ttl1, ttl2)
	}
}

func TestWriteEvents_MaxLenEnabled(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	s, err := New(Config{
		URL:     "redis://" + mr.Addr(),
		MaxLen:  3,
		TTL:     -1,
		Retries: 0,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	// Write 2 events twice (4 total), MaxLen=3 should trim
	if err := s.WriteEvents(t.Context(), testEvents()); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := s.WriteEvents(t.Context(), testEvents()); err != nil {
		t.Fatalf("second write: %v", err)
	}

	stream, err := mr.Stream(DefaultStreamKey)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	// With approximate trimming, miniredis may keep up to MaxLen entries
	if len(stream) > 3 {
		t.Errorf("expected at most 3 entries with MaxLen=3, got %d", len(stream))
	}
}

func TestWriteEvents_StreamEntryFields(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	s, err := New(Config{
		URL:      "redis://" + mr.Addr(),
		Source:   "src-golden",
		Category: "cat-golden",
		TTL:      -1,
		Retries:  0,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(s)

	events := []*types.EventEnvelope{{
		ContractVersion: "0.14.2",
		EventID:         "evt-golden",
		RunID:           "run-golden",
		Seq:             7,
		Type:            types.EventTypeItem,
		Ts:              "2026-03-12T15:30:00Z",
		Payload:         map[string]any{"item_type": "product", "data": map[string]any{"sku": "ABC"}},
		Attempt:         2,
	}}
	if err := s.WriteEvents(t.Context(), events); err != nil {
		t.Fatalf("write: %v", err)
	}

	stream, err := mr.Stream(DefaultStreamKey)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if len(stream) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(stream))
	}

	vals := streamValues(stream[0])

	// Assert all required fields are present with expected values
	requiredFields := map[string]string{
		"run_id":     "run-golden",
		"event_type": "item",
		"seq":        "7",
		"timestamp":  "2026-03-12T15:30:00Z",
		"source":     "src-golden",
		"category":   "cat-golden",
	}
	for field, want := range requiredFields {
		got, ok := vals[field]
		if !ok {
			t.Errorf("missing required field %q in stream entry", field)
			continue
		}
		if got != want {
			t.Errorf("field %q: got %q, want %q", field, got, want)
		}
	}

	// Verify payload field is present and is valid JSON
	payloadStr, ok := vals["payload"]
	if !ok {
		t.Fatal("missing payload field")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	if payload["item_type"] != "product" {
		t.Errorf("payload item_type: got %v, want product", payload["item_type"])
	}
}
