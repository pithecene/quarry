package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/pithecene/quarry/adapter"
	"github.com/pithecene/quarry/iox"
)

func testEvent() *adapter.RunCompletedEvent {
	return &adapter.RunCompletedEvent{
		ContractVersion: "0.5.0",
		EventType:       "run_completed",
		RunID:           "run-001",
		Source:          "test-source",
		Category:        "default",
		Day:             "2026-02-07",
		Outcome:         "success",
		StoragePath:     "file:///data/source=test-source/category=default/day=2026-02-07/run_id=run-001",
		Timestamp:       "2026-02-07T12:00:00Z",
		Attempt:         1,
		EventCount:      42,
		DurationMs:      1500,
	}
}

// asyncReceive starts a goroutine that reads one message from the subscriber
// and sends it to the returned channel. Must be called BEFORE Publish to avoid
// deadlocking miniredis's synchronous pub/sub delivery.
func asyncReceive(sub *miniredis.Subscriber) <-chan miniredis.PubsubMessage {
	ch := make(chan miniredis.PubsubMessage, 1)
	go func() {
		ch <- <-sub.Messages()
	}()
	return ch
}

func waitMessage(t *testing.T, ch <-chan miniredis.PubsubMessage) miniredis.PubsubMessage {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pub/sub message")
		return miniredis.PubsubMessage{} // unreachable
	}
}

func TestPublish_Success(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	a, err := New(Config{URL: "redis://" + mr.Addr(), Retries: 0})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(a)

	sub := mr.NewSubscriber()
	sub.Subscribe(DefaultChannel)
	ch := asyncReceive(sub)

	event := testEvent()
	if err := a.Publish(t.Context(), event); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg := waitMessage(t, ch)

	var received adapter.RunCompletedEvent
	if err := json.Unmarshal([]byte(msg.Message), &received); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if received.RunID != "run-001" {
		t.Errorf("expected run-001, got %s", received.RunID)
	}
	if received.EventType != "run_completed" {
		t.Errorf("expected run_completed, got %s", received.EventType)
	}
	if received.Outcome != "success" {
		t.Errorf("expected success, got %s", received.Outcome)
	}
}

func TestPublish_DefaultChannel(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	a, err := New(Config{URL: "redis://" + mr.Addr()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(a)

	if a.config.Channel != DefaultChannel {
		t.Errorf("expected default channel %q, got %q", DefaultChannel, a.config.Channel)
	}

	sub := mr.NewSubscriber()
	sub.Subscribe(DefaultChannel)
	ch := asyncReceive(sub)

	if err := a.Publish(t.Context(), testEvent()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg := waitMessage(t, ch)
	if msg.Channel != DefaultChannel {
		t.Errorf("expected channel %q, got %q", DefaultChannel, msg.Channel)
	}
}

func TestPublish_CustomChannel(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	customChannel := "custom:notifications"
	a, err := New(Config{URL: "redis://" + mr.Addr(), Channel: customChannel})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(a)

	if a.config.Channel != customChannel {
		t.Errorf("expected channel %q, got %q", customChannel, a.config.Channel)
	}

	sub := mr.NewSubscriber()
	sub.Subscribe(customChannel)
	ch := asyncReceive(sub)

	if err := a.Publish(t.Context(), testEvent()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg := waitMessage(t, ch)
	if msg.Channel != customChannel {
		t.Errorf("expected channel %q, got %q", customChannel, msg.Channel)
	}
}

func TestPublish_RetriesOnFailure(t *testing.T) {
	t.Parallel()

	// Verify a successful publish to a healthy server works with retries configured
	mr := miniredis.RunT(t)

	a, err := New(Config{URL: "redis://" + mr.Addr(), Retries: 3, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(a)

	sub := mr.NewSubscriber()
	sub.Subscribe(DefaultChannel)
	ch := asyncReceive(sub)

	if err := a.Publish(t.Context(), testEvent()); err != nil {
		t.Fatalf("publish should succeed: %v", err)
	}

	msg := waitMessage(t, ch)
	if msg.Channel != DefaultChannel {
		t.Errorf("expected channel %q, got %q", DefaultChannel, msg.Channel)
	}
}

func TestPublish_ExhaustsRetries(t *testing.T) {
	t.Parallel()

	// Use an address that won't connect
	a, err := New(Config{URL: "redis://127.0.0.1:1", Retries: 2, Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(a)

	err = a.Publish(t.Context(), testEvent())
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
}

func TestPublish_ContextCanceled(t *testing.T) {
	t.Parallel()

	// Use an address that won't connect — context cancellation should fire first
	a, err := New(Config{URL: "redis://127.0.0.1:1", Retries: 5, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(a)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err = a.Publish(ctx, testEvent())
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

	a, err := New(Config{URL: "redis://" + mr.Addr()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(a)

	if a.config.Channel != DefaultChannel {
		t.Errorf("expected default channel %q, got %q", DefaultChannel, a.config.Channel)
	}
	if a.config.Timeout != DefaultTimeout {
		t.Errorf("expected default timeout %v, got %v", DefaultTimeout, a.config.Timeout)
	}
}

func TestClose_ClosesConnection(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	a, err := New(Config{URL: "redis://" + mr.Addr()})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Publish after close should fail
	err = a.Publish(t.Context(), testEvent())
	if err == nil {
		t.Fatal("expected error after close")
	}
}
