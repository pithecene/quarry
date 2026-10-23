package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pithecene/quarry/adapter"
	"github.com/pithecene/quarry/iox"
)

func testEvent() *adapter.RunCompletedEvent {
	return &adapter.RunCompletedEvent{
		ContractVersion: "0.4.0",
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

func TestPublish_Success(t *testing.T) {
	t.Parallel()

	var received adapter.RunCompletedEvent
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("expected application/json, got %s", ct)
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &received); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	a, err := New(Config{URL: ts.URL, Retries: 0})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(a)

	event := testEvent()
	if err := a.Publish(t.Context(), event); err != nil {
		t.Fatalf("publish: %v", err)
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

func TestPublish_CustomHeaders(t *testing.T) {
	t.Parallel()

	var authHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	a, err := New(Config{
		URL:     ts.URL,
		Headers: map[string]string{"Authorization": "Bearer test-token"},
		Retries: 0,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(a)

	if err := a.Publish(t.Context(), testEvent()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if authHeader != "Bearer test-token" {
		t.Errorf("expected Bearer test-token, got %s", authHeader)
	}
}

func TestPublish_RetriesOnFailure(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	a, err := New(Config{URL: ts.URL, Retries: 3, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(a)

	if err := a.Publish(t.Context(), testEvent()); err != nil {
		t.Fatalf("publish should succeed after retries: %v", err)
	}

	if got := attempts.Load(); got != 3 {
		t.Errorf("expected 3 attempts, got %d", got)
	}
}

func TestPublish_ExhaustsRetries(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	a, err := New(Config{URL: ts.URL, Retries: 2, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer iox.DiscardClose(a)

	err = a.Publish(t.Context(), testEvent())
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}

	// 1 initial + 2 retries = 3
	if got := attempts.Load(); got != 3 {
		t.Errorf("expected 3 attempts, got %d", got)
	}
}

func TestPublish_ContextCanceled(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	a, err := New(Config{URL: ts.URL, Retries: 0, Timeout: 10 * time.Second})
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

func TestNew_RejectsNegativeRetries(t *testing.T) {
	t.Parallel()

	_, err := New(Config{URL: "http://example.com", Retries: -1})
	if err == nil {
		t.Fatal("expected error for negative retries")
	}
}

func TestNew_DefaultTimeout(t *testing.T) {
	t.Parallel()

	a, err := New(Config{URL: "http://example.com"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if a.config.Timeout != DefaultTimeout {
		t.Errorf("expected default timeout %v, got %v", DefaultTimeout, a.config.Timeout)
	}
}

func TestNew_ExplicitRetries(t *testing.T) {
	t.Parallel()

	a, err := New(Config{URL: "http://example.com", Retries: 5})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if a.config.Retries != 5 {
		t.Errorf("expected 5 retries, got %d", a.config.Retries)
	}
}

func TestPublish_Accepts2xxRange(t *testing.T) {
	t.Parallel()

	codes := []int{200, 201, 202, 204}
	for _, code := range codes {
		t.Run(http.StatusText(code), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(code)
			}))
			defer ts.Close()

			a, err := New(Config{URL: ts.URL, Retries: 0})
			if err != nil {
				t.Fatalf("new: %v", err)
			}
			defer iox.DiscardClose(a)

			if err := a.Publish(t.Context(), testEvent()); err != nil {
				t.Fatalf("expected success for %d, got %v", code, err)
			}
		})
	}
}

func TestPublish_4xxFailsImmediately(t *testing.T) {
	t.Parallel()

	codes := []int{400, 401, 403, 404}
	for _, code := range codes {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var attempts atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				w.WriteHeader(code)
			}))
			defer ts.Close()

			a, err := New(Config{URL: ts.URL, Retries: 3})
			if err != nil {
				t.Fatalf("new: %v", err)
			}
			defer iox.DiscardClose(a)

			err = a.Publish(t.Context(), testEvent())
			if err == nil {
				t.Fatalf("expected error for %d", code)
			}

			// 4xx must not retry — only 1 attempt
			if got := attempts.Load(); got != 1 {
				t.Errorf("expected 1 attempt for %d, got %d", code, got)
			}
		})
	}
}

func TestPublish_5xxRetriesAndFails(t *testing.T) {
	t.Parallel()

	codes := []int{500, 502, 503}
	for _, code := range codes {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var attempts atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attempts.Add(1)
				w.WriteHeader(code)
			}))
			defer ts.Close()

			a, err := New(Config{URL: ts.URL, Retries: 2, Timeout: 5 * time.Second})
			if err != nil {
				t.Fatalf("new: %v", err)
			}
			defer iox.DiscardClose(a)

			err = a.Publish(t.Context(), testEvent())
			if err == nil {
				t.Fatalf("expected error for %d", code)
			}

			// 5xx must retry: 1 initial + 2 retries = 3
			if got := attempts.Load(); got != 3 {
				t.Errorf("expected 3 attempts for %d, got %d", code, got)
			}
		})
	}
}
