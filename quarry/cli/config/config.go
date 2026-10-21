package config

import (
	"fmt"
	"sort"
	"time"

	"github.com/pithecene/quarry/types"
)

// Config represents a quarry.yaml configuration file.
// All values are optional and act as defaults for quarry run flags.
// CLI flags always override config values.
type Config struct {
	Source            string                     `yaml:"source"`
	Category          string                     `yaml:"category"`
	Executor          string                     `yaml:"executor"`
	BrowserWSEndpoint string                     `yaml:"browser_ws_endpoint"`
	NoBrowserReuse    bool                       `yaml:"no_browser_reuse"`
	ResolveFrom       string                     `yaml:"resolve_from"`
	Storage           StorageConfig              `yaml:"storage"`
	Policy            PolicyConfig               `yaml:"policy"`
	Proxies           map[string]ProxyPoolConfig `yaml:"proxies"`
	Proxy             ProxySelection             `yaml:"proxy"`
	Adapter           AdapterConfig              `yaml:"adapter"`
	Events            EventSinksConfig           `yaml:"events"`
}

// StorageConfig holds storage defaults from the config file.
type StorageConfig struct {
	Dataset     string `yaml:"dataset"`
	Backend     string `yaml:"backend"`
	Path        string `yaml:"path"`
	Region      string `yaml:"region"`
	Endpoint    string `yaml:"endpoint"`
	S3PathStyle bool   `yaml:"s3_path_style"`
}

// PolicyConfig holds policy defaults from the config file.
type PolicyConfig struct {
	Name          string   `yaml:"name"`
	FlushMode     string   `yaml:"flush_mode"`
	BufferEvents  int      `yaml:"buffer_events"`
	BufferBytes   int64    `yaml:"buffer_bytes"`
	FlushCount    int      `yaml:"flush_count"`
	FlushInterval Duration `yaml:"flush_interval"`
}

// ProxyPoolConfig is a proxy pool definition within the config file.
// Name is derived from the map key, not stored in the struct.
type ProxyPoolConfig struct {
	Strategy      types.ProxyStrategy   `yaml:"strategy"`
	Endpoints     []types.ProxyEndpoint `yaml:"endpoints"`
	Sticky        *types.ProxySticky    `yaml:"sticky,omitempty"`
	RecencyWindow *int                  `yaml:"recency_window,omitempty"`
}

// ProxySelection holds proxy selection defaults from the config file.
type ProxySelection struct {
	Pool     string `yaml:"pool"`
	Strategy string `yaml:"strategy"`
}

// AdapterConfig holds adapter defaults from the config file.
type AdapterConfig struct {
	Type    string            `yaml:"type"`
	URL     string            `yaml:"url"`
	Channel string            `yaml:"channel,omitempty"`
	Headers map[string]string `yaml:"headers,omitempty"`
	Timeout Duration          `yaml:"timeout,omitempty"`
	Retries *int              `yaml:"retries,omitempty"`
}

// EventSinksConfig holds the optional events.sinks configuration.
// When absent, defaults to a single Lode sink (backward compatible).
type EventSinksConfig struct {
	Sinks []EventSinkConfig `yaml:"sinks"`
}

// EventSinkConfig configures a single event sink.
type EventSinkConfig struct {
	// Type is the sink type: "lode" or "redis".
	Type string `yaml:"type"`
	// Delivery controls failure handling: "mandatory" (default) or "best_effort".
	Delivery string `yaml:"delivery,omitempty"`
	// URL is the Redis connection URL (required for type=redis).
	URL string `yaml:"url,omitempty"`
	// StreamKey is the Redis Stream key (default: quarry:events, type=redis only).
	StreamKey string `yaml:"stream_key,omitempty"`
	// MaxLen is the approximate MAXLEN for stream trimming (type=redis only).
	MaxLen *int64 `yaml:"max_len,omitempty"`
	// TTL is the stream key expiry (type=redis only).
	TTL Duration `yaml:"ttl,omitempty"`
	// Timeout is the per-write timeout (type=redis only).
	Timeout Duration `yaml:"timeout,omitempty"`
	// Retries is the number of retry attempts (type=redis only).
	Retries *int `yaml:"retries,omitempty"`
}

// Duration wraps time.Duration for YAML string parsing (e.g. "10s", "5m").
type Duration struct {
	time.Duration
}

// UnmarshalYAML parses a duration string like "10s" or "5m30s".
func (d *Duration) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	if s == "" {
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = parsed
	return nil
}

// ProxyPools converts the map-keyed proxy pool config into a sorted slice
// of types.ProxyPool. Sorting by name ensures deterministic ordering.
func (c *Config) ProxyPools() []types.ProxyPool {
	if len(c.Proxies) == 0 {
		return nil
	}

	// Collect and sort names for deterministic order
	names := make([]string, 0, len(c.Proxies))
	for name := range c.Proxies {
		names = append(names, name)
	}
	sort.Strings(names)

	pools := make([]types.ProxyPool, 0, len(names))
	for _, name := range names {
		pc := c.Proxies[name]
		pools = append(pools, types.ProxyPool{
			Name:          name,
			Strategy:      pc.Strategy,
			Endpoints:     pc.Endpoints,
			Sticky:        pc.Sticky,
			RecencyWindow: pc.RecencyWindow,
		})
	}
	return pools
}
