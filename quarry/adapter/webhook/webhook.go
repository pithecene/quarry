// Package webhook implements an HTTP POST adapter per CONTRACT_INTEGRATION.md.
//
// Publishes run completion events as JSON to a configurable URL.
// Retries with exponential backoff on transient failures.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pithecene/quarry/adapter"
	"github.com/pithecene/quarry/iox"
)

// DefaultTimeout is the default HTTP request timeout.
const DefaultTimeout = 10 * time.Second

// DefaultRetries is the default number of retry attempts.
const DefaultRetries = 3

// Config configures the webhook adapter.
type Config struct {
	// URL is the HTTP endpoint to POST to (required).
	URL string
	// Headers are custom HTTP headers added to each request.
	Headers map[string]string
	// Timeout is the per-request timeout (default 10s).
	Timeout time.Duration
	// Retries is the number of retry attempts on failure (default 3).
	Retries int
}

// Adapter publishes run completion events via HTTP POST.
type Adapter struct {
	config Config
	client *http.Client
}

// New creates a webhook adapter from the given config.
// Returns an error if the URL is empty.
func New(cfg Config) (*Adapter, error) {
	if cfg.URL == "" {
		return nil, errors.New("webhook adapter requires a URL")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.Retries < 0 {
		return nil, fmt.Errorf("retries must be >= 0, got %d", cfg.Retries)
	}

	return &Adapter{
		config: cfg,
		client: &http.Client{Timeout: cfg.Timeout},
	}, nil
}

// Publish sends the event as a JSON POST request.
// Retries with exponential backoff on 5xx responses and network errors.
// 4xx responses are non-retriable and fail immediately.
func (a *Adapter) Publish(ctx context.Context, event *adapter.RunCompletedEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("webhook: marshal event: %w", err)
	}

	var lastErr error
	// attempts = 1 initial + retries
	attempts := 1 + a.config.Retries

	for i := range attempts {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("webhook: context canceled: %w", err)
		}

		// Exponential backoff before retries (not before first attempt)
		if i > 0 {
			backoff := time.Duration(1<<uint(i-1)) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return fmt.Errorf("webhook: context canceled during backoff: %w", ctx.Err())
			case <-time.After(backoff):
			}
		}

		lastErr = a.doRequest(ctx, body)
		if lastErr == nil {
			return nil
		}

		// 4xx errors are non-retriable — stop immediately
		var statusErr *StatusError
		if errors.As(lastErr, &statusErr) && statusErr.Code >= 400 && statusErr.Code < 500 {
			return fmt.Errorf("webhook: non-retriable error: %w", lastErr)
		}
	}

	return fmt.Errorf("webhook: failed after %d attempts: %w", attempts, lastErr)
}

// StatusError is returned for non-2xx HTTP responses.
// Wrapping the status code allows callers to distinguish retriable (5xx)
// from non-retriable (4xx) failures.
type StatusError struct {
	Code int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("unexpected status %d", e.Code)
}

// doRequest performs a single HTTP POST and returns nil on 2xx.
func (a *Adapter) doRequest(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.config.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range a.config.Headers {
		req.Header.Set(k, v)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer iox.DiscardClose(resp.Body)

	// Drain body to allow connection reuse
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &StatusError{Code: resp.StatusCode}
	}

	return nil
}

// Close releases adapter resources.
func (a *Adapter) Close() error {
	a.client.CloseIdleConnections()
	return nil
}

// Verify Adapter implements the adapter interface.
var _ adapter.Adapter = (*Adapter)(nil)
