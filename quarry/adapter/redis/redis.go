// Package redis implements a Redis pub/sub adapter per CONTRACT_INTEGRATION.md.
//
// Publishes run completion events as JSON to a configurable Redis channel.
// Retries with exponential backoff on connection errors.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/pithecene/quarry/adapter"
)

// DefaultChannel is the default pub/sub channel name.
const DefaultChannel = "quarry:run_completed"

// DefaultTimeout is the default per-publish timeout.
const DefaultTimeout = 5 * time.Second

// DefaultRetries is the default number of retry attempts.
const DefaultRetries = 3

// Config configures the Redis pub/sub adapter.
type Config struct {
	// URL is the Redis connection URL (required).
	// Format: redis://[:password@]host:port[/db]
	URL string
	// Channel is the pub/sub channel name (default: quarry:run_completed).
	Channel string
	// Timeout is the per-publish timeout (default 5s).
	Timeout time.Duration
	// Retries is the number of retry attempts on failure (default 3).
	Retries int
}

// Adapter publishes run completion events via Redis PUBLISH.
type Adapter struct {
	config Config
	client *goredis.Client
}

// New creates a Redis pub/sub adapter from the given config.
// Returns an error if the URL is empty or invalid.
func New(cfg Config) (*Adapter, error) {
	if cfg.URL == "" {
		return nil, errors.New("redis adapter requires a URL")
	}

	opts, err := goredis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("redis adapter: invalid URL: %w", err)
	}

	if cfg.Channel == "" {
		cfg.Channel = DefaultChannel
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.Retries < 0 {
		return nil, fmt.Errorf("retries must be >= 0, got %d", cfg.Retries)
	}

	return &Adapter{
		config: cfg,
		client: goredis.NewClient(opts),
	}, nil
}

// Publish sends the event as a JSON PUBLISH to the configured channel.
// Retries with exponential backoff on failures.
func (a *Adapter) Publish(ctx context.Context, event *adapter.RunCompletedEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("redis: marshal event: %w", err)
	}

	var lastErr error
	// attempts = 1 initial + retries
	attempts := 1 + a.config.Retries

	for i := range attempts {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("redis: context canceled: %w", err)
		}

		// Exponential backoff before retries (not before first attempt)
		if i > 0 {
			backoff := time.Duration(1<<uint(i-1)) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return fmt.Errorf("redis: context canceled during backoff: %w", ctx.Err())
			case <-time.After(backoff):
			}
		}

		publishCtx, cancel := context.WithTimeout(ctx, a.config.Timeout)
		lastErr = a.client.Publish(publishCtx, a.config.Channel, body).Err()
		cancel()

		if lastErr == nil {
			return nil
		}
	}

	return fmt.Errorf("redis: failed after %d attempts: %w", attempts, lastErr)
}

// Close releases adapter resources.
func (a *Adapter) Close() error {
	return a.client.Close()
}

// Verify Adapter implements the adapter interface.
var _ adapter.Adapter = (*Adapter)(nil)
