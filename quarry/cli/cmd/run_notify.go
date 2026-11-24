package cmd

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/adapter"
	redisadapter "github.com/pithecene/quarry/adapter/redis"
	"github.com/pithecene/quarry/adapter/webhook"
	quarryconfig "github.com/pithecene/quarry/cli/config"
	"github.com/pithecene/quarry/runtime"
	"github.com/pithecene/quarry/types"
)

// adapterChoice holds parsed adapter configuration.
type adapterChoice struct {
	adapterType string
	url         string
	channel     string
	headers     map[string]string
	timeout     time.Duration
	retries     int
	// publishEndpointPath allows a notification to carry the path of the storage
	// endpoint, which no parser can tell from a credential.
	publishEndpointPath bool
}

// parseAdapterConfigWithPrecedence builds adapter config using CLI > config > defaults.
func parseAdapterConfigWithPrecedence(c *cli.Context, cfg *quarryconfig.Config, adapterType string) (adapterChoice, error) {
	ac := adapterChoice{
		adapterType: adapterType,
		url:         resolveString(c, "adapter-url", configVal(cfg, func(c *quarryconfig.Config) string { return c.Adapter.URL })),
		timeout:     resolveDuration(c, "adapter-timeout", configDurationVal(cfg)),
		headers:     make(map[string]string),
		publishEndpointPath: resolveBool(c, "adapter-publish-endpoint-path",
			configBoolVal(cfg, func(c *quarryconfig.Config) bool { return c.Adapter.PublishEndpointPath })),
	}

	// Retries: CLI > config > urfave default
	switch {
	case c.IsSet("adapter-retries"):
		ac.retries = c.Int("adapter-retries")
	case cfg != nil && cfg.Adapter.Retries != nil:
		ac.retries = *cfg.Adapter.Retries
	default:
		ac.retries = c.Int("adapter-retries")
	}

	if ac.retries < 0 {
		return ac, fmt.Errorf("--adapter-retries must be >= 0, got %d", ac.retries)
	}

	switch ac.adapterType {
	case "webhook":
		if ac.url == "" {
			return ac, errors.New("--adapter-url is required when --adapter=webhook")
		}
	case "redis":
		if ac.url == "" {
			return ac, errors.New("--adapter-url is required when --adapter=redis")
		}
		ac.channel = resolveString(c, "adapter-channel", configVal(cfg, func(c *quarryconfig.Config) string { return c.Adapter.Channel }))
	default:
		return ac, fmt.Errorf("unknown adapter type: %q (supported: webhook, redis)", ac.adapterType)
	}

	// Merge config headers first, then CLI headers override
	if cfg != nil {
		maps.Copy(ac.headers, cfg.Adapter.Headers)
	}
	for _, h := range c.StringSlice("adapter-header") {
		k, v, ok := strings.Cut(h, "=")
		if !ok || k == "" {
			return ac, fmt.Errorf("invalid --adapter-header %q: expected key=value", h)
		}
		ac.headers[k] = v
	}

	// Warn about irrelevant flags for the chosen adapter type
	if ac.adapterType == "redis" && len(ac.headers) > 0 {
		fmt.Fprintf(os.Stderr, "Warning: --adapter-header is ignored for redis adapter\n")
	}

	return ac, nil
}

// buildAdapter creates an adapter from parsed config.
func buildAdapter(ac adapterChoice) (adapter.Adapter, error) {
	switch ac.adapterType {
	case "webhook":
		return webhook.New(webhook.Config{
			URL:     ac.url,
			Headers: ac.headers,
			Timeout: ac.timeout,
			Retries: ac.retries,
		})
	case "redis":
		return redisadapter.New(redisadapter.Config{
			URL:     ac.url,
			Channel: ac.channel,
			Timeout: ac.timeout,
			Retries: ac.retries,
		})
	default:
		return nil, fmt.Errorf("unknown adapter type: %q", ac.adapterType)
	}
}

// buildRunCompletedEvent constructs the adapter event from run result and config.
func buildRunCompletedEvent(
	result *runtime.RunResult,
	storageConfig storageChoice,
	dataset, source, category, day string,
	duration time.Duration,
) *adapter.RunCompletedEvent {
	event := &adapter.RunCompletedEvent{
		ContractVersion: types.ContractVersion,
		EventType:       "run_completed",
		RunID:           result.RunMeta.RunID,
		Source:          source,
		Category:        category,
		Day:             day,
		Outcome:         string(result.Outcome.Status),
		StoragePath:     buildStoragePath(storageConfig, dataset, source, category, day, result.RunMeta.RunID),
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
		Attempt:         result.RunMeta.Attempt,
		EventCount:      result.EventCount,
		DurationMs:      duration.Milliseconds(),
		Locator: adapter.Locator{
			Run: adapter.RunLocator{Source: source, Category: category, Day: day, RunID: result.RunMeta.RunID},
		},
	}
	if result.RunMeta.JobID != nil {
		event.JobID = *result.RunMeta.JobID
	}
	return event
}

// buildStorageLocator is the storage as a reader takes it, without credentials.
func buildStorageLocator(storageConfig storageChoice, dataset string) (adapter.StorageLocator, error) {
	locator := adapter.StorageLocator{Backend: storageConfig.backend, Path: storageConfig.path, Dataset: dataset}
	switch storageConfig.backend {
	case "fs":
		root, err := storageRoot(storageConfig.path)
		if err != nil {
			return adapter.StorageLocator{}, fmt.Errorf("resolve storage path: %w", err)
		}
		locator.Path = root
	case "s3":
		locator.Region = storageConfig.region
		locator.Endpoint = redactEndpoint(storageConfig.endpoint)
		locator.S3PathStyle = storageConfig.usePathStyle
	}
	return locator, nil
}

// validateLocatorEndpoint rejects an S3 endpoint that a notification cannot carry:
// one that is no URL with a host, or one with a path unless publishPath allows it.
// The errors do not repeat the endpoint, which can hold a credential.
func validateLocatorEndpoint(storageConfig storageChoice, publishPath bool) error {
	if storageConfig.backend != "s3" || storageConfig.endpoint == "" {
		return nil
	}
	u, err := url.Parse(storageConfig.endpoint)
	if err != nil || u.Host == "" {
		return errors.New("--storage-endpoint must be a URL with a host when --adapter is set")
	}
	// The path as redactEndpoint publishes it.
	if path := u.EscapedPath(); path != "" && path != "/" && !publishPath {
		return errors.New("--storage-endpoint has a path, which notifications would publish: set --adapter-publish-endpoint-path if it holds no secret")
	}
	return nil
}

// redactEndpoint is the endpoint URL without its user information, query, and
// fragment, or "" when it does not parse as one.
func redactEndpoint(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	u.User, u.RawQuery, u.Fragment, u.RawFragment = nil, "", "", ""
	return u.String()
}
