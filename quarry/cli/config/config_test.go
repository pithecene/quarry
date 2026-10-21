package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/quarry/types"
)

func TestLoad_FullConfig(t *testing.T) {
	yaml := `source: my-source
category: production
executor: ./executor.js

storage:
  dataset: quarry
  backend: s3
  path: my-bucket/prefix
  region: us-east-1
  endpoint: https://example.com
  s3_path_style: true

policy:
  name: buffered
  flush_mode: at_least_once
  buffer_events: 1000
  buffer_bytes: 10485760

proxies:
  pool_a:
    strategy: round_robin
    endpoints:
      - protocol: https
        host: proxy.example.com
        port: 8080

proxy:
  pool: pool_a
  strategy: round_robin

adapter:
  type: webhook
  url: https://hooks.example.com/quarry
  headers:
    Authorization: Bearer token123
  timeout: 10s
  retries: 3
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Top-level fields
	assertEqual(t, "source", cfg.Source, "my-source")
	assertEqual(t, "category", cfg.Category, "production")
	assertEqual(t, "executor", cfg.Executor, "./executor.js")

	// Storage
	assertEqual(t, "storage.backend", cfg.Storage.Backend, "s3")
	assertEqual(t, "storage.path", cfg.Storage.Path, "my-bucket/prefix")
	assertEqual(t, "storage.region", cfg.Storage.Region, "us-east-1")
	assertEqual(t, "storage.endpoint", cfg.Storage.Endpoint, "https://example.com")
	if !cfg.Storage.S3PathStyle {
		t.Error("expected storage.s3_path_style=true")
	}

	// Policy
	assertEqual(t, "policy.name", cfg.Policy.Name, "buffered")
	assertEqual(t, "policy.flush_mode", cfg.Policy.FlushMode, "at_least_once")
	if cfg.Policy.BufferEvents != 1000 {
		t.Errorf("expected buffer_events=1000, got %d", cfg.Policy.BufferEvents)
	}
	if cfg.Policy.BufferBytes != 10485760 {
		t.Errorf("expected buffer_bytes=10485760, got %d", cfg.Policy.BufferBytes)
	}

	// Proxy selection
	assertEqual(t, "proxy.pool", cfg.Proxy.Pool, "pool_a")
	assertEqual(t, "proxy.strategy", cfg.Proxy.Strategy, "round_robin")

	// Adapter
	assertEqual(t, "adapter.type", cfg.Adapter.Type, "webhook")
	assertEqual(t, "adapter.url", cfg.Adapter.URL, "https://hooks.example.com/quarry")
	if cfg.Adapter.Timeout.Duration != 10*time.Second {
		t.Errorf("expected adapter.timeout=10s, got %v", cfg.Adapter.Timeout.Duration)
	}
	if cfg.Adapter.Retries == nil || *cfg.Adapter.Retries != 3 {
		t.Errorf("expected adapter.retries=3")
	}
	if cfg.Adapter.Headers["Authorization"] != "Bearer token123" {
		t.Errorf("expected Authorization header")
	}
}

func TestLoad_EmptyConfig(t *testing.T) {
	path := writeTemp(t, "")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Source != "" {
		t.Errorf("expected empty source, got %q", cfg.Source)
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/quarry.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	path := writeTemp(t, "{{invalid yaml")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoad_EnvExpansion(t *testing.T) {
	t.Setenv("TEST_SOURCE", "expanded-source")

	yaml := `source: ${TEST_SOURCE}`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	assertEqual(t, "source", cfg.Source, "expanded-source")
}

func TestProxyPools_Conversion(t *testing.T) {
	cfg := &Config{
		Proxies: map[string]ProxyPoolConfig{
			"beta_pool": {
				Strategy: types.ProxyStrategyRandom,
				Endpoints: []types.ProxyEndpoint{
					{Protocol: types.ProxyProtocolHTTP, Host: "b.example.com", Port: 8080},
				},
			},
			"alpha_pool": {
				Strategy: types.ProxyStrategyRoundRobin,
				Endpoints: []types.ProxyEndpoint{
					{Protocol: types.ProxyProtocolHTTPS, Host: "a.example.com", Port: 443},
				},
			},
		},
	}

	pools := cfg.ProxyPools()
	if len(pools) != 2 {
		t.Fatalf("expected 2 pools, got %d", len(pools))
	}

	// Sorted by name: alpha_pool before beta_pool
	if pools[0].Name != "alpha_pool" {
		t.Errorf("expected first pool name=alpha_pool, got %q", pools[0].Name)
	}
	if pools[1].Name != "beta_pool" {
		t.Errorf("expected second pool name=beta_pool, got %q", pools[1].Name)
	}

	if pools[0].Strategy != types.ProxyStrategyRoundRobin {
		t.Errorf("expected alpha_pool strategy=round_robin, got %q", pools[0].Strategy)
	}
}

func TestProxyPools_Empty(t *testing.T) {
	cfg := &Config{}
	pools := cfg.ProxyPools()
	if pools != nil {
		t.Errorf("expected nil for empty proxies, got %v", pools)
	}
}

func TestProxyPools_WithSticky(t *testing.T) {
	ttl := int64(3600000)
	cfg := &Config{
		Proxies: map[string]ProxyPoolConfig{
			"sticky_pool": {
				Strategy: types.ProxyStrategySticky,
				Sticky: &types.ProxySticky{
					Scope: types.ProxyStickyDomain,
					TTLMs: &ttl,
				},
				Endpoints: []types.ProxyEndpoint{
					{Protocol: types.ProxyProtocolHTTP, Host: "proxy.example.com", Port: 8080},
				},
			},
		},
	}

	pools := cfg.ProxyPools()
	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(pools))
	}
	if pools[0].Sticky == nil {
		t.Fatal("expected sticky config")
	}
	if pools[0].Sticky.Scope != types.ProxyStickyDomain {
		t.Errorf("expected sticky scope=domain, got %q", pools[0].Sticky.Scope)
	}
	if pools[0].Sticky.TTLMs == nil || *pools[0].Sticky.TTLMs != 3600000 {
		t.Error("expected sticky TTL=3600000")
	}
}

func TestProxyPools_WithRecencyWindow(t *testing.T) {
	w := 3
	cfg := &Config{
		Proxies: map[string]ProxyPoolConfig{
			"recency_pool": {
				Strategy: types.ProxyStrategyRandom,
				Endpoints: []types.ProxyEndpoint{
					{Protocol: types.ProxyProtocolHTTP, Host: "proxy.example.com", Port: 8080},
				},
				RecencyWindow: &w,
			},
		},
	}

	pools := cfg.ProxyPools()
	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(pools))
	}
	if pools[0].RecencyWindow == nil {
		t.Fatal("expected recency_window to be set")
	}
	if *pools[0].RecencyWindow != 3 {
		t.Errorf("expected recency_window=3, got %d", *pools[0].RecencyWindow)
	}
}

func TestLoad_UnknownKeyRejected(t *testing.T) {
	yaml := `source: my-source
bogus_key: should_fail
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for unknown key, got nil")
	}
	if !strings.Contains(err.Error(), "bogus_key") {
		t.Errorf("error should mention the unknown key, got: %v", err)
	}
}

func TestLoad_UnknownNestedKeyRejected(t *testing.T) {
	yaml := `storage:
  backend: fs
  path: ./data
  unknown_field: bad
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for unknown nested key, got nil")
	}
	if !strings.Contains(err.Error(), "unknown_field") {
		t.Errorf("error should mention the unknown key, got: %v", err)
	}
}

func TestLoad_WhitespaceOnlyConfig(t *testing.T) {
	path := writeTemp(t, "   \n  \n  \n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed for whitespace-only config: %v", err)
	}
	if cfg.Source != "" {
		t.Errorf("expected empty source, got %q", cfg.Source)
	}
}

func TestLoad_CommentsOnlyConfig(t *testing.T) {
	path := writeTemp(t, "# This is a comment\n# Another comment\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed for comments-only config: %v", err)
	}
	if cfg.Source != "" {
		t.Errorf("expected empty source, got %q", cfg.Source)
	}
}

func TestLoad_RetriesZeroDistinctFromNil(t *testing.T) {
	// retries: 0 should parse as *int(0), not nil.
	yaml := `adapter:
  type: webhook
  url: https://example.com
  retries: 0
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Adapter.Retries == nil {
		t.Fatal("expected retries to be non-nil (*int(0)), got nil")
	}
	if *cfg.Adapter.Retries != 0 {
		t.Errorf("expected retries=0, got %d", *cfg.Adapter.Retries)
	}
}

func TestLoad_RetriesOmittedIsNil(t *testing.T) {
	// Omitting retries should leave the pointer nil.
	yaml := `adapter:
  type: webhook
  url: https://example.com
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Adapter.Retries != nil {
		t.Errorf("expected retries to be nil, got %d", *cfg.Adapter.Retries)
	}
}

func TestDuration_InvalidFormat(t *testing.T) {
	yaml := `adapter:
  timeout: not-a-duration
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid duration")
	}
	if !strings.Contains(err.Error(), "invalid duration") {
		t.Errorf("error should mention invalid duration, got: %v", err)
	}
}

func TestDuration_EmptyIsZero(t *testing.T) {
	yaml := `adapter:
  type: webhook
  url: https://example.com
  timeout: ""
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Adapter.Timeout.Duration != 0 {
		t.Errorf("expected zero duration, got %v", cfg.Adapter.Timeout.Duration)
	}
}

func TestDuration_UnmarshalYAML(t *testing.T) {
	yaml := `timeout: 30s`
	path := writeTemp(t, "adapter:\n  "+yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Adapter.Timeout.Duration != 30*time.Second {
		t.Errorf("expected 30s, got %v", cfg.Adapter.Timeout.Duration)
	}
}

func TestLoad_RedisAdapterConfig(t *testing.T) {
	yaml := `adapter:
  type: redis
  url: redis://localhost:6379/0
  channel: quarry:run_completed
  timeout: 5s
  retries: 3
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	assertEqual(t, "adapter.type", cfg.Adapter.Type, "redis")
	assertEqual(t, "adapter.url", cfg.Adapter.URL, "redis://localhost:6379/0")
	assertEqual(t, "adapter.channel", cfg.Adapter.Channel, "quarry:run_completed")
	if cfg.Adapter.Timeout.Duration != 5*time.Second {
		t.Errorf("expected adapter.timeout=5s, got %v", cfg.Adapter.Timeout.Duration)
	}
	if cfg.Adapter.Retries == nil || *cfg.Adapter.Retries != 3 {
		t.Errorf("expected adapter.retries=3")
	}
}

func TestLoad_RedisAdapterChannelOmitted(t *testing.T) {
	yaml := `adapter:
  type: redis
  url: redis://localhost:6379/0
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	assertEqual(t, "adapter.type", cfg.Adapter.Type, "redis")
	assertEqual(t, "adapter.channel", cfg.Adapter.Channel, "")
}

func TestLoad_BrowserWSEndpoint(t *testing.T) {
	yaml := `source: my-source
browser_ws_endpoint: ws://127.0.0.1:9222/devtools/browser/abc-123
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	assertEqual(t, "browser_ws_endpoint", cfg.BrowserWSEndpoint, "ws://127.0.0.1:9222/devtools/browser/abc-123")
}

func TestLoad_BrowserWSEndpointOmitted(t *testing.T) {
	yaml := `source: my-source`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	assertEqual(t, "browser_ws_endpoint", cfg.BrowserWSEndpoint, "")
}

func TestLoad_EventSinksConfig(t *testing.T) {
	yaml := `events:
  sinks:
    - type: lode
      delivery: mandatory
    - type: redis
      url: redis://localhost:6379
      stream_key: myapp:events
      delivery: best_effort
      max_len: 50000
      ttl: 12h
      timeout: 3s
      retries: 1
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(cfg.Events.Sinks) != 2 {
		t.Fatalf("expected 2 sinks, got %d", len(cfg.Events.Sinks))
	}

	lode := cfg.Events.Sinks[0]
	assertEqual(t, "lode.type", lode.Type, "lode")
	assertEqual(t, "lode.delivery", lode.Delivery, "mandatory")

	redis := cfg.Events.Sinks[1]
	assertEqual(t, "redis.type", redis.Type, "redis")
	assertEqual(t, "redis.url", redis.URL, "redis://localhost:6379")
	assertEqual(t, "redis.stream_key", redis.StreamKey, "myapp:events")
	assertEqual(t, "redis.delivery", redis.Delivery, "best_effort")
	if redis.MaxLen == nil || *redis.MaxLen != 50000 {
		t.Errorf("redis.max_len: got %v, want 50000", redis.MaxLen)
	}
	if redis.TTL.Duration != 12*time.Hour {
		t.Errorf("redis.ttl: got %v, want 12h", redis.TTL.Duration)
	}
	if redis.Timeout.Duration != 3*time.Second {
		t.Errorf("redis.timeout: got %v, want 3s", redis.Timeout.Duration)
	}
	if redis.Retries == nil || *redis.Retries != 1 {
		t.Errorf("redis.retries: got %v, want 1", redis.Retries)
	}
}

func TestLoad_EventSinksNoConfig(t *testing.T) {
	yaml := `source: test
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(cfg.Events.Sinks) != 0 {
		t.Errorf("expected 0 sinks when unconfigured, got %d", len(cfg.Events.Sinks))
	}
}

// writeTemp writes content to a temp file and returns the path.
func writeTemp(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "quarry.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	return path
}

func assertEqual(t *testing.T, field, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %q, want %q", field, got, want)
	}
}
