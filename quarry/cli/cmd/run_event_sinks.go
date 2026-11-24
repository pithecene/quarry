package cmd

import (
	"errors"
	"fmt"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/adapter/redisstream"
	quarryconfig "github.com/pithecene/quarry/cli/config"
	"github.com/pithecene/quarry/policy"
)

// eventSinkChoice holds parsed event sink configuration.
type eventSinkChoice struct {
	sinkType  string // "lode" or "redis"
	delivery  policy.DeliveryMode
	url       string        // redis only
	streamKey string        // redis only
	maxLen    int64         // redis only
	ttl       time.Duration // redis only
	timeout   time.Duration // redis only
	retries   int           // redis only
}

// parseEventSinkConfig resolves event sink configuration from CLI flags and config file.
// Returns nil (not empty) when no event sinks are explicitly configured, which signals
// buildPolicy to use the default Lode-only path.
func parseEventSinkConfig(c *cli.Context, cfg *quarryconfig.Config) ([]eventSinkChoice, error) {
	cliSinks := c.StringSlice("event-sink")
	var configSinks []quarryconfig.EventSinkConfig
	if cfg != nil {
		configSinks = cfg.Events.Sinks
	}

	// No CLI flags and no config → nil (default Lode-only behavior)
	if len(cliSinks) == 0 && len(configSinks) == 0 {
		return nil, nil
	}

	// CLI flags take precedence over config file
	if len(cliSinks) > 0 {
		return parseEventSinkFromCLI(c, cliSinks)
	}
	return parseEventSinkFromConfig(configSinks)
}

func parseEventSinkFromCLI(c *cli.Context, sinkTypes []string) ([]eventSinkChoice, error) {
	var choices []eventSinkChoice
	seen := make(map[string]bool)

	for _, st := range sinkTypes {
		if seen[st] {
			return nil, fmt.Errorf("duplicate event sink type: %s", st)
		}
		seen[st] = true

		ch, err := parseSingleCLISink(c, st)
		if err != nil {
			return nil, err
		}
		choices = append(choices, ch)
	}
	return choices, nil
}

func parseSingleCLISink(c *cli.Context, sinkType string) (eventSinkChoice, error) {
	switch sinkType {
	case "lode":
		delivery, err := parseDeliveryMode(c.String("event-sink-lode-delivery"))
		if err != nil {
			return eventSinkChoice{}, fmt.Errorf("event-sink-lode-delivery: %w", err)
		}
		return eventSinkChoice{sinkType: "lode", delivery: delivery}, nil

	case "redis":
		url := c.String("event-sink-redis-url")
		if url == "" {
			return eventSinkChoice{}, errors.New("--event-sink-redis-url is required when --event-sink includes redis")
		}
		delivery, err := parseDeliveryMode(c.String("event-sink-redis-delivery"))
		if err != nil {
			return eventSinkChoice{}, fmt.Errorf("event-sink-redis-delivery: %w", err)
		}
		return eventSinkChoice{
			sinkType:  "redis",
			delivery:  delivery,
			url:       url,
			streamKey: c.String("event-sink-redis-stream-key"),
			maxLen:    c.Int64("event-sink-redis-max-len"),
			ttl:       c.Duration("event-sink-redis-ttl"),
			timeout:   c.Duration("event-sink-redis-timeout"),
			retries:   c.Int("event-sink-redis-retries"),
		}, nil

	default:
		return eventSinkChoice{}, fmt.Errorf("unknown event sink type: %q (valid: lode, redis)", sinkType)
	}
}

func parseEventSinkFromConfig(sinks []quarryconfig.EventSinkConfig) ([]eventSinkChoice, error) {
	var choices []eventSinkChoice
	seen := make(map[string]bool)
	for _, s := range sinks {
		if seen[s.Type] {
			return nil, fmt.Errorf("duplicate event sink type: %s", s.Type)
		}
		seen[s.Type] = true

		ch, err := parseSingleConfigSink(s)
		if err != nil {
			return nil, err
		}
		choices = append(choices, ch)
	}
	return choices, nil
}

func parseSingleConfigSink(s quarryconfig.EventSinkConfig) (eventSinkChoice, error) {
	delivery, err := parseDeliveryMode(s.Delivery)
	if err != nil {
		return eventSinkChoice{}, fmt.Errorf("event sink %q delivery: %w", s.Type, err)
	}

	switch s.Type {
	case "lode":
		return eventSinkChoice{sinkType: "lode", delivery: delivery}, nil

	case "redis":
		if s.URL == "" {
			return eventSinkChoice{}, errors.New("event sink redis requires a url")
		}
		ch := eventSinkChoice{
			sinkType:  "redis",
			delivery:  delivery,
			url:       s.URL,
			streamKey: s.StreamKey,
			timeout:   s.Timeout.Duration,
			ttl:       s.TTL.Duration,
		}
		if s.MaxLen != nil {
			ch.maxLen = *s.MaxLen
		}
		// Nil means "not specified" → apply default. Zero is a valid explicit
		// value meaning "no retries", so the pointer distinguishes the two.
		if s.Retries != nil {
			ch.retries = *s.Retries
		} else {
			ch.retries = redisstream.DefaultRetries
		}
		return ch, nil

	default:
		return eventSinkChoice{}, fmt.Errorf("unknown event sink type: %q", s.Type)
	}
}

func parseDeliveryMode(s string) (policy.DeliveryMode, error) {
	switch s {
	case "", "mandatory":
		return policy.DeliveryMandatory, nil
	case "best_effort":
		return policy.DeliveryBestEffort, nil
	default:
		return 0, fmt.Errorf("unknown delivery mode: %q (valid: mandatory, best_effort)", s)
	}
}

// buildEffectiveSink wraps the Lode sink with fan-out when event sinks are
// configured. Returns lodeSink unchanged when no event sink config is
// present (backward compatible default).
func buildEffectiveSink(configs []eventSinkChoice, lodeSink policy.Sink, source, category string) (policy.Sink, error) {
	if len(configs) == 0 {
		return lodeSink, nil
	}
	entries, err := buildEventSinkEntries(configs, lodeSink, source, category)
	if err != nil {
		return nil, fmt.Errorf("failed to create event sinks: %w", err)
	}
	return policy.NewFanoutEventSink(entries), nil
}

// buildEntry constructs a SinkEntry from the parsed config.
// lodeSink is reused when sinkType is "lode" (no duplicate Lode client).
// source and category are injected into Redis stream entries.
func (esc eventSinkChoice) buildEntry(lodeSink policy.Sink, source, category string) (policy.SinkEntry, error) {
	switch esc.sinkType {
	case "lode":
		// Safe: lode.Sink and lode.InstrumentedSink both implement EventSink
		// (verified by compile-time assertions in lode/sink.go).
		return policy.SinkEntry{
			Sink:     lodeSink.(policy.EventSink),
			Delivery: esc.delivery,
			Label:    "lode",
		}, nil

	case "redis":
		rs, err := redisstream.New(redisstream.Config{
			URL:       esc.url,
			StreamKey: esc.streamKey,
			MaxLen:    esc.maxLen,
			TTL:       esc.ttl,
			Timeout:   esc.timeout,
			Retries:   esc.retries,
			Source:    source,
			Category:  category,
		})
		if err != nil {
			return policy.SinkEntry{}, fmt.Errorf("redis event sink: %w", err)
		}
		return policy.SinkEntry{
			Sink:     rs,
			Delivery: esc.delivery,
			Label:    "redis",
		}, nil

	default:
		// Unreachable: parse functions validate sink types upstream.
		return policy.SinkEntry{}, fmt.Errorf("unknown event sink type in build: %q", esc.sinkType)
	}
}

// buildEventSinkEntries creates SinkEntry instances from parsed configs.
func buildEventSinkEntries(configs []eventSinkChoice, lodeSink policy.Sink, source, category string) ([]policy.SinkEntry, error) {
	entries := make([]policy.SinkEntry, 0, len(configs))
	for _, cfg := range configs {
		entry, err := cfg.buildEntry(lodeSink, source, category)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
