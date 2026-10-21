// Package redisstream implements a Redis Streams event sink per CONTRACT_INTEGRATION.md.
//
// Publishes granular runtime events via XADD to a shared Redis Stream.
// Supports MAXLEN approximate trimming and per-key TTL expiry.
// Retries with exponential backoff on connection errors.
package redisstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// DefaultStreamKey is the default Redis Stream key.
const DefaultStreamKey = "quarry:events"

// DefaultMaxLen is the default approximate MAXLEN for stream trimming.
const DefaultMaxLen int64 = 100_000

// DefaultTTL is the default stream key expiry.
const DefaultTTL = 24 * time.Hour

// DefaultTimeout is the default per-write timeout.
const DefaultTimeout = 2 * time.Second

// DefaultRetries is the default number of retry attempts.
const DefaultRetries = 2

// Config configures the Redis Streams event sink.
type Config struct {
	// URL is the Redis connection URL (required).
	// Format: redis://[:password@]host:port[/db]
	URL string
	// StreamKey is the Redis Stream key (default: quarry:events).
	StreamKey string
	// MaxLen is the approximate MAXLEN for stream trimming (default: 100000).
	// Set to -1 to disable trimming. Zero applies the default.
	MaxLen int64
	// TTL is the stream key expiry duration (default: 24h).
	// Set to -1 to disable expiry. Zero applies the default.
	TTL time.Duration
	// Timeout is the per-write timeout (default: 2s).
	Timeout time.Duration
	// Retries is the number of retry attempts on failure (default: 2).
	Retries int

	// Source is the run's source partition key, included in every stream entry.
	Source string
	// Category is the run's category partition key, included in every stream entry.
	Category string
}

// Sink publishes events to a Redis Stream via XADD.
type Sink struct {
	config  Config
	client  *goredis.Client
	ttlOnce atomic.Bool // ensures TTL is set at most once per sink lifetime
}

// New creates a Redis Streams event sink from the given config.
// Returns an error if the URL is empty or invalid.
func New(cfg Config) (*Sink, error) {
	if cfg.URL == "" {
		return nil, errors.New("redis stream sink requires a URL")
	}

	opts, err := goredis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("redis stream sink: invalid URL: %w", err)
	}

	if cfg.StreamKey == "" {
		cfg.StreamKey = DefaultStreamKey
	}
	if cfg.MaxLen == 0 {
		cfg.MaxLen = DefaultMaxLen
	}
	if cfg.TTL == 0 {
		cfg.TTL = DefaultTTL
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.Retries < 0 {
		return nil, fmt.Errorf("retries must be >= 0, got %d", cfg.Retries)
	}

	return &Sink{
		config: cfg,
		client: goredis.NewClient(opts),
	}, nil
}

// WriteEvents publishes each event as an XADD entry to the configured stream.
// Uses a Redis pipeline for batch efficiency.
// Retries the full batch with exponential backoff on failure.
func (s *Sink) WriteEvents(ctx context.Context, events []*types.EventEnvelope) error {
	if len(events) == 0 {
		return nil
	}

	var lastErr error
	attempts := 1 + s.config.Retries

	for i := range attempts {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("redis stream: context canceled: %w", err)
		}

		if i > 0 {
			backoff := time.Duration(1<<uint(i-1)) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return fmt.Errorf("redis stream: context canceled during backoff: %w", ctx.Err())
			case <-time.After(backoff):
			}
		}

		writeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		lastErr = s.xaddBatch(writeCtx, events)
		cancel()

		if lastErr == nil {
			s.applyTTLOnce(ctx)
			return nil
		}
	}

	return fmt.Errorf("redis stream: failed after %d attempts: %w", attempts, lastErr)
}

// xaddBatch pipelines XADD commands for the event batch.
func (s *Sink) xaddBatch(ctx context.Context, events []*types.EventEnvelope) error {
	pipe := s.client.Pipeline()
	for _, ev := range events {
		args, err := s.buildXAddArgs(ev)
		if err != nil {
			return err
		}
		pipe.XAdd(ctx, args)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// buildXAddArgs constructs the XADD arguments for a single event envelope.
func (s *Sink) buildXAddArgs(ev *types.EventEnvelope) (*goredis.XAddArgs, error) {
	payload, err := json.Marshal(ev.Payload)
	if err != nil {
		return nil, fmt.Errorf("redis stream: marshal payload: %w", err)
	}
	args := &goredis.XAddArgs{
		Stream: s.config.StreamKey,
		Values: map[string]any{
			"run_id":     ev.RunID,
			"event_type": string(ev.Type),
			"seq":        strconv.FormatInt(ev.Seq, 10),
			"timestamp":  ev.Ts,
			"payload":    string(payload),
			"source":     s.config.Source,
			"category":   s.config.Category,
		},
	}
	if s.config.MaxLen > 0 {
		args.MaxLen = s.config.MaxLen
		args.Approx = true
	}
	return args, nil
}

// applyTTLOnce sets EXPIRE on the stream key exactly once per sink lifetime.
// Called after the first successful write so the key exists.
// TTL failures are best-effort — the stream still works without expiry.
func (s *Sink) applyTTLOnce(ctx context.Context) {
	if s.config.TTL < 0 {
		return // TTL disabled
	}
	if !s.ttlOnce.CompareAndSwap(false, true) {
		return // already applied
	}
	ttlCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	// Best-effort: EXPIRE failure doesn't affect event delivery.
	_ = s.client.Expire(ttlCtx, s.config.StreamKey, s.config.TTL).Err()
}

// Close releases sink resources.
func (s *Sink) Close() error {
	return s.client.Close()
}

// Verify Sink implements policy.EventSink.
var _ policy.EventSink = (*Sink)(nil)
