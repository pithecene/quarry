package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/adapter"
	redisadapter "github.com/pithecene/quarry/adapter/redis"
	"github.com/pithecene/quarry/adapter/redisstream"
	"github.com/pithecene/quarry/adapter/webhook"
	quarryconfig "github.com/pithecene/quarry/cli/config"
	"github.com/pithecene/quarry/executor"
	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/proxy"
	"github.com/pithecene/quarry/runtime"
	"github.com/pithecene/quarry/types"
)

// Exit codes per CONTRACT_RUN.md.
const (
	exitSuccess       = 0
	exitScriptError   = 1
	exitExecutorCrash = 2
	exitPolicyFailure = 3
)

// exitConfigError is used for CLI/input validation failures.
// These are pre-execution errors (not script failures).
// Maps to exitExecutorCrash since they prevent execution.
const exitConfigError = exitExecutorCrash

// RunCommand returns the run command.
// This is the only command that executes work per CONTRACT_CLI.md.
func RunCommand() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Usage: "Execute a script run (the only execution entrypoint)",
		UsageText: `quarry run --script <path> --run-id <id> --source <name> \
    --storage-backend <fs|s3> --storage-path <path> [options]

EXAMPLES:
  # Run a script with filesystem storage
  quarry run --script ./script.ts --run-id run-001 --source my-source \
    --storage-backend fs --storage-path ./data

  # Run with inline job payload
  quarry run --script ./script.ts --run-id run-002 --source my-source \
    --storage-backend fs --storage-path ./data \
    --job '{"url": "https://example.com"}'

  # Run with job payload from file
  quarry run --script ./script.ts --run-id run-003 --source my-source \
    --storage-backend fs --storage-path ./data \
    --job-json ./jobs/crawl-config.json

  # Run with S3 storage
  quarry run --script ./script.ts --run-id run-004 --source my-source \
    --storage-backend s3 --storage-path my-bucket/prefix \
    --storage-region us-east-1

  # Run with Cloudflare R2 (S3-compatible)
  quarry run --script ./script.ts --run-id run-005 --source my-source \
    --storage-backend s3 --storage-path my-bucket/prefix \
    --storage-endpoint https://ACCOUNT_ID.r2.cloudflarestorage.com \
    --storage-s3-path-style

ADVANCED:
  # Override executor path (troubleshooting)
  quarry run --script ./script.ts --run-id run-006 --source my-source \
    --storage-backend fs --storage-path ./data \
    --executor /custom/path/to/executor.js`,
		Flags: []cli.Flag{
			// Config file flag
			&cli.StringFlag{
				Name:  "config",
				Usage: "Path to YAML config file (project-level defaults for quarry run)",
			},
			// Execution flags
			&cli.StringFlag{
				Name:     "script",
				Usage:    "Path to script file",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "run-id",
				Usage:    "Run ID",
				Required: true,
			},
			&cli.IntFlag{
				Name:  "attempt",
				Usage: "Attempt number (starts at 1)",
				Value: 1,
			},
			&cli.StringFlag{
				Name:  "job-id",
				Usage: "Job ID (optional)",
			},
			&cli.StringFlag{
				Name:  "parent-run-id",
				Usage: "Parent run ID (required for retries)",
			},
			&cli.StringFlag{
				Name:  "job",
				Usage: "Job payload as inline JSON object (mutually exclusive with --job-json)",
			},
			&cli.StringFlag{
				Name:  "job-json",
				Usage: "Path to JSON file containing job payload object (mutually exclusive with --job)",
			},
			&cli.StringFlag{
				Name:  "executor",
				Usage: "Path to executor binary (advanced: auto-resolved by default)",
			},
			&cli.StringFlag{
				Name:    "browser-ws-endpoint",
				Usage:   "WebSocket URL of an externally managed browser (connect instead of launch)",
				EnvVars: []string{"QUARRY_BROWSER_ENDPOINT"},
			},
			&cli.BoolFlag{
				Name:  "quiet",
				Usage: "Suppress result output",
			},
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "Validate script loadability without executing a run (no browser, no storage)",
			},
			&cli.StringFlag{
				Name:  "report",
				Usage: "Write structured JSON report to path on exit (use - for stderr)",
			},
			// Partition key flags
			&cli.StringFlag{
				Name:  "source",
				Usage: "Source identifier for partitioning (required)",
			},
			&cli.StringFlag{
				Name:  "category",
				Usage: "Category identifier for partitioning",
				Value: "default",
			},
			// Policy flags
			&cli.StringFlag{
				Name:  "policy",
				Usage: "Ingestion policy: strict, buffered, or streaming",
				Value: "strict",
			},
			&cli.StringFlag{
				Name:  "flush-mode",
				Usage: "Flush mode for buffered policy: at_least_once, chunks_first, two_phase",
				Value: "at_least_once",
			},
			&cli.IntFlag{
				Name:  "buffer-events",
				Usage: "Max buffered events (buffered policy)",
				Value: 0,
			},
			&cli.Int64Flag{
				Name:  "buffer-bytes",
				Usage: "Max buffer size in bytes (buffered policy)",
				Value: 0,
			},
			&cli.IntFlag{
				Name:  "flush-count",
				Usage: "Flush after N events accumulate (streaming policy)",
				Value: 0,
			},
			&cli.DurationFlag{
				Name:  "flush-interval",
				Usage: "Flush every duration, e.g. 5s, 30s (streaming policy)",
				Value: 0,
			},
			// Proxy flags
			&cli.StringFlag{
				Name:  "proxy-config",
				Usage: "Path to proxy pools config file (JSON)",
			},
			&cli.StringFlag{
				Name:  "proxy-pool",
				Usage: "Pool name to select proxy from",
			},
			&cli.StringFlag{
				Name:  "proxy-strategy",
				Usage: "Strategy override: round_robin, random, or sticky",
			},
			&cli.StringFlag{
				Name:  "proxy-sticky-key",
				Usage: "Sticky key override for proxy selection",
			},
			&cli.StringFlag{
				Name:  "proxy-domain",
				Usage: "Domain for sticky scope derivation (when scope=domain)",
			},
			&cli.StringFlag{
				Name:  "proxy-origin",
				Usage: "Origin for sticky scope derivation (when scope=origin, format: scheme://host:port)",
			},
			// Storage flags
			&cli.StringFlag{
				Name:  "storage-dataset",
				Usage: "Lode dataset ID (overrides default \"quarry\")",
				Value: lode.DefaultDataset,
			},
			&cli.StringFlag{
				Name:  "storage-backend",
				Usage: "Storage backend: fs (filesystem) or s3 (Amazon S3)",
			},
			&cli.StringFlag{
				Name:  "storage-path",
				Usage: "Storage path (fs: writable directory, s3: bucket/prefix)",
			},
			&cli.StringFlag{
				Name:  "storage-region",
				Usage: "AWS region for S3 backend (uses default credential chain if omitted)",
			},
			&cli.StringFlag{
				Name:  "storage-endpoint",
				Usage: "Custom S3 endpoint URL for S3-compatible providers (e.g. Cloudflare R2, MinIO)",
			},
			&cli.BoolFlag{
				Name:  "storage-s3-path-style",
				Usage: "Force path-style addressing for S3 (required by R2, MinIO)",
			},
			// Browser reuse flags
			&cli.BoolFlag{
				Name:  "no-browser-reuse",
				Usage: "Disable transparent browser reuse across runs",
			},
			// Module resolution flags
			&cli.StringFlag{
				Name:  "resolve-from",
				Usage: "Path to node_modules directory for bare-specifier ESM resolution fallback (monorepo/container support)",
			},
			// Fan-out flags
			&cli.IntFlag{
				Name:  "depth",
				Usage: "Maximum fan-out recursion depth (0 = disabled)",
				Value: 0,
			},
			&cli.IntFlag{
				Name:  "max-runs",
				Usage: "Maximum total child runs (required when --depth > 0)",
			},
			&cli.IntFlag{
				Name:  "parallel",
				Usage: "Maximum concurrent child runs",
				Value: 1,
			},
			// Adapter flags (event-bus notification)
			&cli.StringFlag{
				Name:  "adapter",
				Usage: "Event-bus adapter type (webhook, redis)",
			},
			&cli.StringFlag{
				Name:  "adapter-url",
				Usage: "Adapter endpoint URL (required when --adapter is set)",
			},
			&cli.StringSliceFlag{
				Name:  "adapter-header",
				Usage: "Custom HTTP header as key=value (repeatable)",
			},
			&cli.DurationFlag{
				Name:  "adapter-timeout",
				Usage: "Adapter notification timeout",
				Value: webhook.DefaultTimeout,
			},
			&cli.IntFlag{
				Name:  "adapter-retries",
				Usage: "Adapter retry attempts",
				Value: webhook.DefaultRetries,
			},
			&cli.StringFlag{
				Name:  "adapter-channel",
				Usage: "Pub/sub channel name for Redis adapter (default: quarry:run_completed)",
			},
			// Event sink flags
			&cli.StringSliceFlag{
				Name:  "event-sink",
				Usage: "Event sink type (repeatable: lode, redis). Default: lode only",
			},
			&cli.StringFlag{
				Name:  "event-sink-redis-url",
				Usage: "Redis URL for event sink (required when --event-sink includes redis)",
			},
			&cli.StringFlag{
				Name:  "event-sink-redis-stream-key",
				Usage: "Redis Stream key for events",
				Value: redisstream.DefaultStreamKey,
			},
			&cli.Int64Flag{
				Name:  "event-sink-redis-max-len",
				Usage: "Approximate MAXLEN for Redis Stream trimming",
				Value: redisstream.DefaultMaxLen,
			},
			&cli.DurationFlag{
				Name:  "event-sink-redis-ttl",
				Usage: "Redis Stream key expiry",
				Value: redisstream.DefaultTTL,
			},
			&cli.DurationFlag{
				Name:  "event-sink-redis-timeout",
				Usage: "Per-write timeout for Redis event sink",
				Value: redisstream.DefaultTimeout,
			},
			&cli.IntFlag{
				Name:  "event-sink-redis-retries",
				Usage: "Retry attempts for Redis event sink",
				Value: redisstream.DefaultRetries,
			},
			&cli.StringFlag{
				Name:  "event-sink-redis-delivery",
				Usage: "Delivery mode for Redis event sink (mandatory, best_effort)",
				Value: "mandatory",
			},
			&cli.StringFlag{
				Name:  "event-sink-lode-delivery",
				Usage: "Delivery mode for Lode event sink (mandatory, best_effort)",
				Value: "mandatory",
			},
		},
		Action: runAction,
	}
}

// policyChoice holds parsed policy configuration.
type policyChoice struct {
	name          string
	flushMode     string
	maxEvents     int
	maxBytes      int64
	flushCount    int
	flushInterval time.Duration
}

// proxyChoice holds parsed proxy configuration.
type proxyChoice struct {
	configPath string
	poolName   string
	strategy   string
	stickyKey  string
	domain     string
	origin     string
}

// storageChoice holds parsed storage configuration.
type storageChoice struct {
	backend      string // "fs" or "s3"
	path         string // fs: directory, s3: bucket/prefix
	region       string // AWS region for S3 (optional)
	endpoint     string // custom S3 endpoint for S3-compatible providers (optional)
	usePathStyle bool   // force path-style addressing for S3 (optional)
}

// adapterChoice holds parsed adapter configuration.
type adapterChoice struct {
	adapterType string
	url         string
	channel     string
	headers     map[string]string
	timeout     time.Duration
	retries     int
}

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

// fanOutChoice holds parsed fan-out configuration.
type fanOutChoice struct {
	depth    int
	maxRuns  int
	parallel int
}

func validateFanOutConfig(choice fanOutChoice) error {
	if choice.depth < 0 {
		return fmt.Errorf("--depth must be >= 0, got %d", choice.depth)
	}
	if choice.depth > 0 && choice.maxRuns == 0 {
		return errors.New("--max-runs is required when --depth > 0 (safety rail to prevent unbounded fan-out)")
	}
	if choice.maxRuns < 0 {
		return fmt.Errorf("--max-runs must be >= 0, got %d", choice.maxRuns)
	}
	if choice.parallel < 1 {
		return fmt.Errorf("--parallel must be >= 1, got %d", choice.parallel)
	}
	return nil
}

// childFactory holds the shared configuration needed to construct and execute
// child runs during fan-out. Each invocation of Run builds an independent
// policy, sink, and metrics collector for the child.
type childFactory struct {
	policyChoice      policyChoice
	executorPath      string
	storage           storageChoice
	storageDataset    string
	source            string
	category          string
	proxy             *types.ProxyEndpoint
	browserWSEndpoint string
	resolveFrom       string
	eventSinks        []eventSinkChoice
}

// Run constructs and executes a single child run for the fan-out operator.
func (cf *childFactory) Run(ctx context.Context, item runtime.WorkItem, observer runtime.EnqueueObserver) (*runtime.RunResult, error) {
	childMeta := &types.RunMeta{
		RunID:   item.RunID,
		Attempt: 1,
	}

	childSource := cf.source
	if item.Source != "" {
		childSource = item.Source
	}
	childCategory := cf.category
	if item.Category != "" {
		childCategory = item.Category
	}

	childCollector := metrics.NewCollector(
		cf.policyChoice.name,
		filepath.Base(cf.executorPath),
		cf.storage.backend,
		item.RunID,
		"",
	)

	if err := lode.ValidateRunKey(cf.storageDataset, childSource, childCategory, item.RunID); err != nil {
		return nil, fmt.Errorf("invalid child run key: %w", err)
	}

	childStartTime := time.Now()
	childPol, childLodeClient, childFileWriter, err := buildPolicy(ctx,
		cf.policyChoice, cf.storage, cf.storageDataset,
		childSource, childCategory, item.RunID,
		childStartTime, childCollector, cf.eventSinks,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create child policy: %w", err)
	}
	defer iox.DiscardClose(childPol)

	config := &runtime.RunConfig{
		ExecutorPath:      cf.executorPath,
		ScriptPath:        item.Target,
		Job:               item.Params,
		RunMeta:           childMeta,
		Policy:            childPol,
		Proxy:             cf.proxy,
		FileWriter:        childFileWriter,
		EnqueueObserver:   observer,
		BrowserWSEndpoint: cf.browserWSEndpoint,
		ResolveFrom:       cf.resolveFrom,
		Source:            childSource,
		Category:          childCategory,
		StorageDataset:    cf.storageDataset,
		StorageDay:        lode.DeriveDay(childStartTime),
		Collector:         childCollector,
		Storage:           childLodeClient,
		StartedAt:         childStartTime,
	}

	orchestrator, err := runtime.NewRunOrchestrator(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create child orchestrator: %w", err)
	}

	// Execute persists the child's metrics and run records itself
	// (CONTRACT_RUN.md §Run Finalization).
	result, err := orchestrator.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("child execution failed: %w", err)
	}
	return result, nil
}

// runFinalizer handles post-execution concerns: adapter notification, the
// exit report, and result printing. Metrics and run records are persisted by
// Execute itself (CONTRACT_RUN.md §Run Finalization). Built once per run
// invocation and shared across fan-out and single-run paths.
type runFinalizer struct {
	collector      *metrics.Collector
	adapter        *adapterChoice
	storage        storageChoice
	storageDataset string
	source         string
	category       string
	policyChoice   policyChoice
	startTime      time.Time
	quiet          bool
	reportPath     string
}

// Finalize notifies the adapter, writes the report, and prints results.
// duration is computed from startTime internally.
//
// Note: run_completed events reach all configured event sinks (including Redis Streams)
// through the normal policy path — no separate terminal publish is needed.
func (f *runFinalizer) Finalize(result *runtime.RunResult) {
	duration := time.Since(f.startTime)
	if result.RunRecordError != "" {
		fmt.Fprintf(os.Stderr, "Warning: %s\n", result.RunRecordError)
	}
	f.notifyAdapter(result, duration)
	f.writeReport(result)
	f.printResults(result, duration)
}

func (f *runFinalizer) notifyAdapter(result *runtime.RunResult, duration time.Duration) {
	if f.adapter == nil {
		return
	}
	adpt, err := buildAdapter(*f.adapter)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: adapter creation failed: %v\n", err)
		return
	}
	defer iox.DiscardClose(adpt)

	event := buildRunCompletedEvent(result, f.storage, f.storageDataset, f.source, f.category, lode.DeriveDay(f.startTime), duration)
	ctx, cancel := context.WithTimeout(context.Background(), f.adapter.timeout)
	defer cancel()
	if err := adpt.Publish(ctx, event); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: adapter notification failed: %v\n", err)
	}
}

func (f *runFinalizer) writeReport(result *runtime.RunResult) {
	if f.reportPath == "" {
		return
	}
	exitCode := outcomeToExitCode(result.Outcome.Status)
	report := runtime.BuildRunReport(result, f.collector.Snapshot(), f.policyChoice.name, exitCode)
	if err := runtime.WriteRunReport(report, f.reportPath); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write report: %v\n", err)
	}
}

func (f *runFinalizer) printResults(result *runtime.RunResult, duration time.Duration) {
	if f.quiet {
		return
	}
	printRunResult(result, f.policyChoice, duration)
	printMetrics(f.collector.Snapshot())
}

func runAction(c *cli.Context) error {
	// Load config file if --config is provided
	var cfg *quarryconfig.Config
	if configPath := c.String("config"); configPath != "" {
		loaded, err := quarryconfig.Load(configPath)
		if err != nil {
			return cli.Exit(fmt.Sprintf("failed to load config: %v", err), exitConfigError)
		}
		cfg = loaded
	}

	// Resolve values with precedence: CLI flag > config file > flag default
	source := resolveString(c, "source", configVal(cfg, func(c *quarryconfig.Config) string { return c.Source }))
	category := resolveString(c, "category", configVal(cfg, func(c *quarryconfig.Config) string { return c.Category }))
	executor := resolveString(c, "executor", configVal(cfg, func(c *quarryconfig.Config) string { return c.Executor }))
	browserWSEndpoint := resolveString(c, "browser-ws-endpoint", configVal(cfg, func(c *quarryconfig.Config) string { return c.BrowserWSEndpoint }))
	resolveFrom := resolveString(c, "resolve-from", configVal(cfg, func(c *quarryconfig.Config) string { return c.ResolveFrom }))

	dryRun := c.Bool("dry-run")

	// Manual validation for fields that were previously Required:true
	// In dry-run mode, --source is not required (script validation only)
	if source == "" && !dryRun {
		return cli.Exit("--source is required (provide via CLI flag or config file)", exitConfigError)
	}

	// Validate and absolutize --resolve-from
	if resolveFrom != "" {
		absResolveFrom, err := filepath.Abs(resolveFrom)
		if err != nil {
			return cli.Exit(fmt.Sprintf("--resolve-from: cannot resolve path %q: %v", resolveFrom, err), exitConfigError)
		}
		info, err := os.Stat(absResolveFrom)
		if err != nil {
			if os.IsNotExist(err) {
				return cli.Exit(fmt.Sprintf("--resolve-from: directory does not exist: %s", absResolveFrom), exitConfigError)
			}
			return cli.Exit(fmt.Sprintf("--resolve-from: cannot access %q: %v", absResolveFrom, err), exitConfigError)
		}
		if !info.IsDir() {
			return cli.Exit(fmt.Sprintf("--resolve-from: not a directory: %s", absResolveFrom), exitConfigError)
		}
		resolveFrom = absResolveFrom
	}

	// Dry-run mode: validate script loadability only.
	// Skip policy, storage, proxy, adapter, and fan-out config entirely.
	if dryRun {
		executorPath, err := resolveExecutor(executor)
		if err != nil {
			return cli.Exit(err.Error(), exitConfigError)
		}
		return runDryRun(c.Context, executorPath, c.String("script"), resolveFrom)
	}

	// Parse policy config with precedence
	choice := policyChoice{
		name:          resolveString(c, "policy", configVal(cfg, func(c *quarryconfig.Config) string { return c.Policy.Name })),
		flushMode:     resolveString(c, "flush-mode", configVal(cfg, func(c *quarryconfig.Config) string { return c.Policy.FlushMode })),
		maxEvents:     resolveInt(c, "buffer-events", configIntVal(cfg, func(c *quarryconfig.Config) int { return c.Policy.BufferEvents })),
		maxBytes:      resolveInt64(c, "buffer-bytes", configInt64Val(cfg, func(c *quarryconfig.Config) int64 { return c.Policy.BufferBytes })),
		flushCount:    resolveInt(c, "flush-count", configIntVal(cfg, func(c *quarryconfig.Config) int { return c.Policy.FlushCount })),
		flushInterval: resolveDuration(c, "flush-interval", configPolicyDurationVal(cfg)),
	}

	// Validate policy config
	if err := validatePolicyConfig(choice); err != nil {
		return cli.Exit(fmt.Sprintf("invalid policy config: %v", err), exitExecutorCrash)
	}

	// Parse job payload (--job or --job-json, not both)
	job, err := parseJobPayload(c.String("job"), c.String("job-json"))
	if err != nil {
		return cli.Exit(err.Error(), exitConfigError)
	}

	// Build run metadata
	runMeta := &types.RunMeta{
		RunID:   c.String("run-id"),
		Attempt: c.Int("attempt"),
	}
	if jobID := c.String("job-id"); jobID != "" {
		runMeta.JobID = &jobID
	}
	if parentRunID := c.String("parent-run-id"); parentRunID != "" {
		runMeta.ParentRunID = &parentRunID
	}

	// Parse and validate storage config with precedence
	storageBackend := resolveString(c, "storage-backend", configVal(cfg, func(c *quarryconfig.Config) string { return c.Storage.Backend }))
	storagePath := resolveString(c, "storage-path", configVal(cfg, func(c *quarryconfig.Config) string { return c.Storage.Path }))

	if storageBackend == "" {
		return cli.Exit("--storage-backend is required (provide via CLI flag or config file)", exitConfigError)
	}
	if storagePath == "" {
		return cli.Exit("--storage-path is required (provide via CLI flag or config file)", exitConfigError)
	}

	storageConfig := storageChoice{
		backend:      storageBackend,
		path:         storagePath,
		region:       resolveString(c, "storage-region", configVal(cfg, func(c *quarryconfig.Config) string { return c.Storage.Region })),
		endpoint:     resolveString(c, "storage-endpoint", configVal(cfg, func(c *quarryconfig.Config) string { return c.Storage.Endpoint })),
		usePathStyle: resolveBool(c, "storage-s3-path-style", configBoolVal(cfg, func(c *quarryconfig.Config) bool { return c.Storage.S3PathStyle })),
	}
	if err := validateStorageConfig(storageConfig); err != nil {
		return cli.Exit(err.Error(), exitConfigError)
	}

	storageDataset := resolveString(c, "storage-dataset", configVal(cfg, func(c *quarryconfig.Config) string { return c.Storage.Dataset }))
	if err := lode.ValidateRunKey(storageDataset, source, category, runMeta.RunID); err != nil {
		return cli.Exit(err.Error(), exitConfigError)
	}

	// Parse and validate adapter config (pre-execution: fail fast on bad config)
	var adptConfig *adapterChoice
	adapterType := resolveString(c, "adapter", configVal(cfg, func(c *quarryconfig.Config) string { return c.Adapter.Type }))
	if adapterType != "" {
		ac, err := parseAdapterConfigWithPrecedence(c, cfg, adapterType)
		if err != nil {
			return cli.Exit(fmt.Sprintf("invalid adapter config: %v", err), exitConfigError)
		}
		adptConfig = &ac
	}

	// Parse and validate event sink config
	eventSinks, err := parseEventSinkConfig(c, cfg)
	if err != nil {
		return cli.Exit(fmt.Sprintf("invalid event sink config: %v", err), exitConfigError)
	}

	// Parse and validate fan-out config
	fanOut := fanOutChoice{
		depth:    c.Int("depth"),
		maxRuns:  c.Int("max-runs"),
		parallel: c.Int("parallel"),
	}
	if err := validateFanOutConfig(fanOut); err != nil {
		return cli.Exit(fmt.Sprintf("invalid fan-out config: %v", err), exitConfigError)
	}
	if fanOut.depth == 0 && c.IsSet("parallel") && fanOut.parallel > 1 {
		fmt.Fprintf(os.Stderr, "Warning: --parallel > 1 has no effect without --depth > 0\n")
	}

	// Resolve executor path (needed for metrics dimension before policy build)
	executorPath, err := resolveExecutor(executor)
	if err != nil {
		return cli.Exit(err.Error(), exitConfigError)
	}

	// Create metrics collector per CONTRACT_METRICS.md
	var jobID string
	if runMeta.JobID != nil {
		jobID = *runMeta.JobID
	}
	// Use basename for stable executor identity (avoids high-cardinality from absolute paths)
	collector := metrics.NewCollector(choice.name, filepath.Base(executorPath), storageConfig.backend, runMeta.RunID, jobID)

	// Build policy with storage sink and optional event sinks
	// Start time is "now" - used to derive partition day
	startTime := time.Now()
	pol, lodeClient, fileWriter, err := buildPolicy(c.Context, choice, storageConfig, storageDataset, source, category, runMeta.RunID, startTime, collector, eventSinks)
	if err != nil {
		return fmt.Errorf("failed to create policy: %w", err)
	}
	defer iox.DiscardClose(pol)

	// Resolve proxy pools from config file (inline proxies: key)
	var configPools []types.ProxyPool
	if cfg != nil {
		configPools = cfg.ProxyPools()
	}

	// Conflict check: --proxy-config and config proxies: cannot both be present
	cliProxyConfig := c.String("proxy-config")
	if cliProxyConfig != "" && len(configPools) > 0 {
		return cli.Exit("cannot use --proxy-config and config file proxies: together (use one source for proxy pools)", exitConfigError)
	}

	// Deprecation warning for --proxy-config
	if cliProxyConfig != "" {
		fmt.Fprintf(os.Stderr, "Warning: --proxy-config is deprecated; define proxy pools in quarry.yaml under the proxies: key instead\n")
	}

	// Parse proxy config with precedence
	proxyConfig := proxyChoice{
		configPath: cliProxyConfig,
		poolName:   resolveString(c, "proxy-pool", configVal(cfg, func(c *quarryconfig.Config) string { return c.Proxy.Pool })),
		strategy:   resolveString(c, "proxy-strategy", configVal(cfg, func(c *quarryconfig.Config) string { return c.Proxy.Strategy })),
		stickyKey:  c.String("proxy-sticky-key"),
		domain:     c.String("proxy-domain"),
		origin:     c.String("proxy-origin"),
	}

	// Select proxy if configured
	var resolvedProxy *types.ProxyEndpoint
	if proxyConfig.poolName != "" {
		endpoint, err := selectProxy(proxyConfig, runMeta, configPools)
		if err != nil {
			return cli.Exit(fmt.Sprintf("proxy selection failed: %v", err), exitExecutorCrash)
		}
		resolvedProxy = endpoint
	}

	// Warn if proxy and browser-ws-endpoint both set (launch args ignored; page.authenticate still applies)
	if resolvedProxy != nil && browserWSEndpoint != "" {
		fmt.Fprintf(os.Stderr, "Warning: --proxy-* launch args are ignored with --browser-ws-endpoint; only page.authenticate() credentials apply\n")
	}

	// Set up context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Resolve browser reuse:
	// Priority: explicit --browser-ws-endpoint > browser reuse > per-run launch
	noBrowserReuse := resolveBool(c, "no-browser-reuse", configBoolVal(cfg, func(c *quarryconfig.Config) bool { return c.NoBrowserReuse }))
	if browserWSEndpoint == "" && !noBrowserReuse {
		idleTimeout := runtime.IdleTimeoutFromEnv()
		reuseCfg := runtime.ReusableBrowserConfig{
			ExecutorPath: executorPath,
			ScriptPath:   c.String("script"),
			Proxy:        resolvedProxy,
			IdleTimeout:  idleTimeout,
		}
		if ws, err := runtime.AcquireReusableBrowser(ctx, reuseCfg); err == nil {
			browserWSEndpoint = ws
		} else {
			fmt.Fprintf(os.Stderr, "Browser reuse unavailable: %v\n", err)
		}
	}

	// Build finalizer (shared post-run concerns for both execution paths)
	finalizer := &runFinalizer{
		collector:      collector,
		adapter:        adptConfig,
		storage:        storageConfig,
		storageDataset: storageDataset,
		source:         source,
		category:       category,
		policyChoice:   choice,
		startTime:      startTime,
		quiet:          c.Bool("quiet"),
		reportPath:     c.String("report"),
	}

	// Build root run config
	rootConfig := &runtime.RunConfig{
		ExecutorPath:      executorPath,
		ScriptPath:        c.String("script"),
		Job:               job,
		RunMeta:           runMeta,
		Policy:            pol,
		Proxy:             resolvedProxy,
		FileWriter:        fileWriter,
		BrowserWSEndpoint: browserWSEndpoint,
		ResolveFrom:       resolveFrom,
		Source:            source,
		Category:          category,
		StorageDataset:    storageDataset,
		StorageDay:        lode.DeriveDay(startTime),
		Collector:         collector,
		Storage:           lodeClient,
		StartedAt:         startTime,
	}

	// Branch: fan-out or single run
	if fanOut.depth > 0 {
		// Use reusable browser if already acquired; otherwise launch a managed
		// browser for fan-out to avoid N cold startups (one per child run).
		if browserWSEndpoint == "" {
			managedBrowser, err := runtime.LaunchManagedBrowser(ctx, executorPath, c.String("script"))
			if err != nil {
				return cli.Exit(fmt.Sprintf("failed to launch shared browser: %v", err), exitExecutorCrash)
			}
			defer iox.DiscardClose(managedBrowser)
			browserWSEndpoint = managedBrowser.WSEndpoint
			rootConfig.BrowserWSEndpoint = browserWSEndpoint
		}

		factory := &childFactory{
			policyChoice:      choice,
			executorPath:      executorPath,
			storage:           storageConfig,
			storageDataset:    storageDataset,
			source:            source,
			category:          category,
			proxy:             resolvedProxy,
			browserWSEndpoint: browserWSEndpoint,
			resolveFrom:       resolveFrom,
			eventSinks:        eventSinks,
		}
		return runWithFanOut(ctx, fanOut, rootConfig, factory, finalizer)
	}

	// Create orchestrator
	orchestrator, err := runtime.NewRunOrchestrator(rootConfig)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}

	// Execute run (startTime was set earlier for Lode day derivation)
	result, err := orchestrator.Execute(ctx)
	if err != nil {
		return fmt.Errorf("execution failed: %w", err)
	}

	finalizer.Finalize(result)
	return cli.Exit("", outcomeToExitCode(result.Outcome.Status))
}

// runWithFanOut executes the root run with fan-out scheduling enabled.
// It creates an Operator, wires the root run's EnqueueObserver, and
// runs the root orchestrator and operator concurrently.
func runWithFanOut(
	ctx context.Context,
	fanOut fanOutChoice,
	rootConfig *runtime.RunConfig,
	factory *childFactory,
	finalizer *runFinalizer,
) error {
	// Create operator
	operator := runtime.NewOperator(runtime.FanOutConfig{
		MaxDepth: fanOut.depth,
		MaxRuns:  fanOut.maxRuns,
		Parallel: fanOut.parallel,
	}, factory.Run)

	// Wire root run's enqueue observer into the operator
	rootConfig.EnqueueObserver = operator.NewObserver(0)

	rootOrchestrator, err := runtime.NewRunOrchestrator(rootConfig)
	if err != nil {
		return fmt.Errorf("failed to create orchestrator: %w", err)
	}

	// Run root orchestrator and operator concurrently.
	// rootDone signals to the operator that the root run has finished
	// and no more enqueue events will arrive from it.
	rootDone := make(chan struct{})
	var rootResult *runtime.RunResult
	var rootErr error

	go func() {
		rootResult, rootErr = rootOrchestrator.Execute(ctx)
		close(rootDone)
	}()

	// Operator blocks until root is done + queue drained + workers idle.
	operator.Run(ctx, rootDone)

	if rootErr != nil {
		return fmt.Errorf("execution failed: %w", rootErr)
	}

	finalizer.Finalize(rootResult)

	// Print fan-out summary
	if !finalizer.quiet {
		fanOutResult := operator.Results()
		runtime.PrintFanOutSummary(fanOutResult)
	}

	// Exit code is determined by root run outcome only
	return cli.Exit("", outcomeToExitCode(rootResult.Outcome.Status))
}

// runDryRun validates script loadability via the executor's --validate mode.
// It prints a human-readable summary to stderr and exits 0 (valid) or 1 (invalid).
func runDryRun(ctx context.Context, executorPath, scriptPath, resolveFrom string) error {
	fmt.Fprintf(os.Stderr, "Dry-run validation:\n")
	fmt.Fprintf(os.Stderr, "  script:   %s\n", scriptPath)
	fmt.Fprintf(os.Stderr, "  executor: %s\n", executorPath)
	if resolveFrom != "" {
		fmt.Fprintf(os.Stderr, "  resolve-from: %s\n", resolveFrom)
	}
	fmt.Fprintln(os.Stderr)

	result, err := runtime.ValidateScript(ctx, executorPath, scriptPath, resolveFrom)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ Executor failed: %v\n\nValidation failed.\n", err)
		return cli.Exit("", exitExecutorCrash)
	}

	if !result.Valid {
		fmt.Fprintf(os.Stderr, "  ✗ Script load failed: %s\n\nValidation failed.\n", result.Error)
		return cli.Exit("", exitScriptError)
	}

	fmt.Fprintf(os.Stderr, "  ✓ Script loaded successfully\n")
	if result.Exports != nil {
		fmt.Fprintf(os.Stderr, "  ✓ Default export: function\n")
		if len(result.Exports.Hooks) > 0 {
			fmt.Fprintf(os.Stderr, "  ✓ Hooks: %s\n", strings.Join(result.Exports.Hooks, ", "))
		}
	}
	fmt.Fprintf(os.Stderr, "\nValidation passed.\n")

	return cli.Exit("", exitSuccess)
}

// resolveString returns the CLI flag value if explicitly set, else the config
// value if non-empty, else the urfave default.
func resolveString(c *cli.Context, flag string, configVal string) string {
	if c.IsSet(flag) {
		return c.String(flag)
	}
	if configVal != "" {
		return configVal
	}
	return c.String(flag)
}

// resolveInt returns the CLI flag value if explicitly set, else the config
// value if non-zero, else the urfave default.
func resolveInt(c *cli.Context, flag string, configVal int) int {
	if c.IsSet(flag) {
		return c.Int(flag)
	}
	if configVal != 0 {
		return configVal
	}
	return c.Int(flag)
}

// resolveInt64 returns the CLI flag value if explicitly set, else the config
// value if non-zero, else the urfave default.
func resolveInt64(c *cli.Context, flag string, configVal int64) int64 {
	if c.IsSet(flag) {
		return c.Int64(flag)
	}
	if configVal != 0 {
		return configVal
	}
	return c.Int64(flag)
}

// resolveBool returns the CLI flag value if explicitly set, else the config
// value if true, else the urfave default.
func resolveBool(c *cli.Context, flag string, configVal bool) bool {
	if c.IsSet(flag) {
		return c.Bool(flag)
	}
	if configVal {
		return configVal
	}
	return c.Bool(flag)
}

// resolveDuration returns the CLI flag value if explicitly set, else the config
// value if non-zero, else the urfave default.
func resolveDuration(c *cli.Context, flag string, configVal time.Duration) time.Duration {
	if c.IsSet(flag) {
		return c.Duration(flag)
	}
	if configVal != 0 {
		return configVal
	}
	return c.Duration(flag)
}

// configVal safely extracts a string value from an optional config.
func configVal(cfg *quarryconfig.Config, fn func(*quarryconfig.Config) string) string {
	if cfg == nil {
		return ""
	}
	return fn(cfg)
}

// configIntVal safely extracts an int value from an optional config.
func configIntVal(cfg *quarryconfig.Config, fn func(*quarryconfig.Config) int) int {
	if cfg == nil {
		return 0
	}
	return fn(cfg)
}

// configInt64Val safely extracts an int64 value from an optional config.
func configInt64Val(cfg *quarryconfig.Config, fn func(*quarryconfig.Config) int64) int64 {
	if cfg == nil {
		return 0
	}
	return fn(cfg)
}

// configBoolVal safely extracts a bool value from an optional config.
func configBoolVal(cfg *quarryconfig.Config, fn func(*quarryconfig.Config) bool) bool {
	if cfg == nil {
		return false
	}
	return fn(cfg)
}

// parseAdapterConfigWithPrecedence builds adapter config using CLI > config > defaults.
func parseAdapterConfigWithPrecedence(c *cli.Context, cfg *quarryconfig.Config, adapterType string) (adapterChoice, error) {
	ac := adapterChoice{
		adapterType: adapterType,
		url:         resolveString(c, "adapter-url", configVal(cfg, func(c *quarryconfig.Config) string { return c.Adapter.URL })),
		timeout:     resolveDuration(c, "adapter-timeout", configDurationVal(cfg)),
		headers:     make(map[string]string),
	}

	// Retries: CLI > config > urfave default
	if c.IsSet("adapter-retries") {
		ac.retries = c.Int("adapter-retries")
	} else if cfg != nil && cfg.Adapter.Retries != nil {
		ac.retries = *cfg.Adapter.Retries
	} else {
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

// configDurationVal extracts the adapter timeout duration from config.
func configDurationVal(cfg *quarryconfig.Config) time.Duration {
	if cfg == nil {
		return 0
	}
	return cfg.Adapter.Timeout.Duration
}

// configPolicyDurationVal extracts the policy flush interval from config.
func configPolicyDurationVal(cfg *quarryconfig.Config) time.Duration {
	if cfg == nil {
		return 0
	}
	return cfg.Policy.FlushInterval.Duration
}

func validatePolicyConfig(choice policyChoice) error {
	switch choice.name {
	case "strict":
		if choice.maxEvents > 0 || choice.maxBytes > 0 || choice.flushMode != "at_least_once" {
			fmt.Fprintf(os.Stderr, "Warning: buffer/flush flags ignored for strict policy\n")
		}
		return nil

	case "buffered":
		if choice.maxEvents <= 0 && choice.maxBytes <= 0 {
			return fmt.Errorf(`buffered policy requires buffer limits

Add one or both of:
  --buffer-events <n>   Maximum events to buffer (e.g., --buffer-events 1000)
  --buffer-bytes <n>    Maximum bytes to buffer (e.g., --buffer-bytes 1048576)`)
		}
		switch policy.FlushMode(choice.flushMode) {
		case policy.FlushAtLeastOnce, policy.FlushChunksFirst, policy.FlushTwoPhase:
			return nil
		default:
			return fmt.Errorf(`invalid --flush-mode: %q

Valid options:
  at_least_once   Flush all buffered data at least once (default)
  chunks_first    Flush artifact chunks before events
  two_phase       Two-phase commit for transactional semantics`, choice.flushMode)
		}

	case "streaming":
		if choice.flushCount <= 0 && choice.flushInterval <= 0 {
			return fmt.Errorf(`streaming policy requires at least one flush trigger

Add one or both of:
  --flush-count <n>       Flush after N events (e.g., --flush-count 100)
  --flush-interval <d>    Flush every duration (e.g., --flush-interval 5s)`)
		}
		// Warn about irrelevant buffered flags
		if choice.maxEvents > 0 || choice.maxBytes > 0 || choice.flushMode != "at_least_once" {
			fmt.Fprintf(os.Stderr, "Warning: buffer/flush-mode flags ignored for streaming policy\n")
		}
		return nil

	default:
		return fmt.Errorf(`invalid --policy: %q

Valid options:
  strict      Write events immediately, fail on any error (default)
  buffered    Buffer events in memory, flush periodically
  streaming   Continuous batched writes with flush triggers`, choice.name)
	}
}

func validateStorageConfig(config storageChoice) error {
	switch config.backend {
	case "fs":
		if config.endpoint != "" || config.usePathStyle {
			fmt.Fprintf(os.Stderr, "Warning: --storage-endpoint and --storage-s3-path-style are ignored for fs backend\n")
		}
		// Validate path exists and is a directory
		info, err := os.Stat(config.path)
		if os.IsNotExist(err) {
			return fmt.Errorf(`storage path does not exist: %s

Create the directory first:
  mkdir -p %s`, config.path, config.path)
		}
		if err != nil {
			return fmt.Errorf("cannot access storage path %q: %v (ensure the path exists and is readable)", config.path, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("storage path is not a directory: %s (--storage-path for fs backend must be a directory, not a file)", config.path)
		}
		return nil

	case "s3":
		// Basic validation for S3 path format
		if config.path == "" {
			return fmt.Errorf(`--storage-path required for s3 backend

Format: bucket-name/optional-prefix
Example: --storage-path my-bucket/quarry-data`)
		}
		// S3 credentials are validated at runtime by AWS SDK
		return nil

	default:
		return fmt.Errorf(`invalid --storage-backend: %q

Valid options:
  fs   Filesystem storage (requires writable directory)
  s3   Amazon S3 storage (requires AWS credentials)`, config.backend)
	}
}

// parseJobPayload parses job payload from --job (inline) or --job-json (file).
// Using both flags is an explicit error. If neither is specified, returns empty object.
// The payload must be a top-level JSON object. Arrays, primitives, and null are
// rejected with actionable error messages.
func parseJobPayload(jobInline, jobFile string) (map[string]any, error) {
	hasInline := jobInline != ""
	hasFile := jobFile != ""

	// Conflict: both specified
	if hasInline && hasFile {
		return nil, fmt.Errorf(`cannot use both --job and --job-json

Provide job payload via ONE of:
  --job '{"key": "value"}'     (inline JSON)
  --job-json ./payload.json    (path to JSON file)`)
	}

	// Load from file
	if hasFile {
		data, err := os.ReadFile(jobFile)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf(`job file not found: %s

Ensure the file exists:
  ls -la %s`, jobFile, jobFile)
			}
			return nil, fmt.Errorf("cannot read job file %q: %v", jobFile, err)
		}

		var raw any
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf(`malformed JSON in job file %s: %v

The file must contain valid JSON. Example:
  {"url": "https://example.com", "page": 1}`, jobFile, err)
		}

		job, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf(`job file %s must contain a JSON object

The payload must be a top-level JSON object (not an array, string, number, or null).

Valid:
  {}
  {"url": "https://example.com"}

Invalid:
  []            (array)
  "string"      (primitive)
  123           (primitive)
  null          (null)

Received: %s`, jobFile, describeJSONType(raw))
		}
		return job, nil
	}

	// Parse inline JSON
	if hasInline {
		var raw any
		if err := json.Unmarshal([]byte(jobInline), &raw); err != nil {
			return nil, fmt.Errorf(`malformed --job JSON: %v

The --job flag must contain valid JSON. Examples:
  --job '{}'
  --job '{"key": "value"}'
  --job '{"url": "https://example.com", "page": 1}'

Received: %s`, err, jobInline)
		}

		job, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf(`--job must be a JSON object

The payload must be a top-level JSON object (not an array, string, number, or null).

Valid:
  --job '{}'
  --job '{"url": "https://example.com"}'

Invalid:
  --job '[]'            (array)
  --job '"string"'      (primitive)
  --job '123'           (primitive)
  --job 'null'          (null)

Received: %s`, describeJSONType(raw))
		}
		return job, nil
	}

	// Neither specified: empty object
	return map[string]any{}, nil
}

// describeJSONType returns a human-readable description of a JSON value's type.
func describeJSONType(v any) string {
	switch v := v.(type) {
	case nil:
		return "null"
	case map[string]any:
		return "object"
	case []any:
		return fmt.Sprintf("array (length %d)", len(v))
	case string:
		return fmt.Sprintf("string (%q)", v)
	case float64:
		return fmt.Sprintf("number (%v)", v)
	case bool:
		return fmt.Sprintf("boolean (%v)", v)
	default:
		return fmt.Sprintf("unknown (%T)", v)
	}
}

// resolveExecutor finds the executor binary path.
// Resolution order:
//  1. Explicit --executor flag (if provided)
//  2. Embedded executor (extracted to temp dir)
//  3. Bundled path relative to quarry binary (development layout)
//  4. "quarry-executor" in PATH
func resolveExecutor(explicit string) (string, error) {
	// 1. Explicit override takes priority
	if explicit != "" {
		if _, err := os.Stat(explicit); err != nil {
			return "", fmt.Errorf("executor not found at %s (check the path and try again)", explicit)
		}
		return explicit, nil
	}

	// 2. Try embedded executor (primary method for distributed binary)
	if executor.IsEmbedded() {
		path, err := executor.ExtractedPath()
		if err == nil {
			return path, nil
		}
		// Log extraction failure but continue to fallbacks
		fmt.Fprintf(os.Stderr, "Warning: failed to extract embedded executor: %v\n", err)
	}

	// 3. Try bundled path relative to this binary (development layout)
	execPath, err := os.Executable()
	if err == nil {
		execDir := filepath.Dir(execPath)
		// Try common bundled locations
		bundledPaths := []string{
			filepath.Join(execDir, "..", "executor-node", "dist", "bin", "executor.js"),
			filepath.Join(execDir, "executor-node", "dist", "bin", "executor.js"),
			filepath.Join(execDir, "executor.js"),
		}
		for _, p := range bundledPaths {
			absPath, err := filepath.Abs(p)
			if err != nil {
				continue
			}
			if _, err := os.Stat(absPath); err == nil {
				return absPath, nil
			}
		}
	}

	// 4. Try PATH lookup
	if path, err := exec.LookPath("quarry-executor"); err == nil {
		return path, nil
	}

	// Not found - provide actionable error
	return "", fmt.Errorf(`executor not found

Quarry could not locate the executor. To fix:

  1. Build the executor:
     pnpm -C executor-node run build

  2. Or specify the path manually:
     --executor ./executor-node/dist/bin/executor.js

  3. Or add quarry-executor to your PATH`)
}

func buildPolicy(ctx context.Context, choice policyChoice, storageConfig storageChoice, dataset, source, category, runID string, startTime time.Time, collector *metrics.Collector, eventSinkConfigs []eventSinkChoice) (policy.Policy, *lode.LodeClient, lode.FileWriter, error) {
	lodeSink, client, fw, err := buildStorageSink(ctx, storageConfig, dataset, source, category, runID, choice.name, startTime, collector)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create storage sink: %w", err)
	}

	sink, err := buildEffectiveSink(eventSinkConfigs, lodeSink, source, category)
	if err != nil {
		return nil, nil, nil, err
	}

	switch choice.name {
	case "strict":
		return policy.NewStrictPolicy(sink), client, fw, nil

	case "buffered":
		config := policy.BufferedConfig{
			MaxBufferEvents: choice.maxEvents,
			MaxBufferBytes:  choice.maxBytes,
			FlushMode:       policy.FlushMode(choice.flushMode),
		}
		p, err := policy.NewBufferedPolicy(sink, config)
		return p, client, fw, err

	case "streaming":
		config := policy.StreamingConfig{
			FlushCount:    choice.flushCount,
			FlushInterval: choice.flushInterval,
		}
		p, err := policy.NewStreamingPolicy(sink, config)
		return p, client, fw, err

	default:
		return nil, nil, nil, fmt.Errorf("unknown policy: %s", choice.name)
	}
}

// buildStorageSink creates a Lode storage sink based on CLI configuration.
// Storage backend and path are required - no silent fallback to stub.
// If collector is non-nil, wraps the sink with metrics instrumentation.
// Returns the sink, the underlying client (for run finalization),
// a FileWriter for sidecar file uploads, and any error.
func buildStorageSink(ctx context.Context, storageConfig storageChoice, dataset, source, category, runID, policy string, startTime time.Time, collector *metrics.Collector) (policy.Sink, *lode.LodeClient, lode.FileWriter, error) {
	// Build Lode config with partition keys
	cfg := lode.Config{
		Dataset:  dataset,
		Source:   source,
		Category: category,
		Day:      lode.DeriveDay(startTime),
		RunID:    runID,
		Policy:   policy,
		// Collector increments are nil-receiver safe, so this is harmless
		// when metrics are disabled.
		RetryObserver: func(int) { collector.IncLodeWriteRetry() },
	}

	// LodeClient implements both lode.Client and lode.FileWriter.
	// Capture as concrete type so we can return both interfaces.
	var lc *lode.LodeClient
	var err error

	switch storageConfig.backend {
	case "fs":
		lc, err = lode.NewLodeClient(ctx, cfg, storageConfig.path)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("filesystem storage initialization failed: %w (ensure directory %s exists and is writable)", err, storageConfig.path)
		}
	case "s3":
		bucket, prefix := lode.ParseS3Path(storageConfig.path)
		s3cfg := lode.S3Config{
			Bucket:       bucket,
			Prefix:       prefix,
			Region:       storageConfig.region,
			Endpoint:     storageConfig.endpoint,
			UsePathStyle: storageConfig.usePathStyle,
		}
		lc, err = lode.NewLodeS3Client(ctx, cfg, s3cfg)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("S3 storage initialization failed: %w (check AWS credentials and bucket permissions)", err)
		}
	default:
		// Should not reach here due to validateStorageConfig
		return nil, nil, nil, fmt.Errorf("unknown storage-backend: %s", storageConfig.backend)
	}

	sink := lode.NewSink(cfg, lc)
	if collector != nil {
		return lode.NewInstrumentedSink(sink, collector), lc, lc, nil
	}
	return sink, lc, lc, nil
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
	}
	if result.RunMeta.JobID != nil {
		event.JobID = *result.RunMeta.JobID
	}
	return event
}

// buildStoragePath constructs a human-readable storage path for the event payload.
func buildStoragePath(storageConfig storageChoice, dataset, source, category, day, runID string) string {
	partitions := fmt.Sprintf("datasets/%s/partitions/source=%s/category=%s/day=%s/run_id=%s",
		dataset, source, category, day, runID)

	switch storageConfig.backend {
	case "fs":
		absPath, err := filepath.Abs(storageConfig.path)
		if err != nil {
			absPath = storageConfig.path
		}
		return fmt.Sprintf("file://%s/%s", absPath, partitions)
	case "s3":
		bucket, prefix := lode.ParseS3Path(storageConfig.path)
		if prefix != "" {
			return fmt.Sprintf("s3://%s/%s/%s", bucket, prefix, partitions)
		}
		return fmt.Sprintf("s3://%s/%s", bucket, partitions)
	default:
		return partitions
	}
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

// buildEffectiveSink wraps the Lode sink with fan-out and composite routing
// when event sinks are configured. Returns lodeSink unchanged when no event
// sink config is present (backward compatible default).
func buildEffectiveSink(configs []eventSinkChoice, lodeSink policy.Sink, source, category string) (policy.Sink, error) {
	if len(configs) == 0 {
		return lodeSink, nil
	}
	entries, err := buildEventSinkEntries(configs, lodeSink, source, category)
	if err != nil {
		return nil, fmt.Errorf("failed to create event sinks: %w", err)
	}
	fanout := policy.NewFanoutEventSink(entries)
	return policy.NewCompositeSink(fanout, lodeSink), nil
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

func outcomeToExitCode(status types.OutcomeStatus) int {
	switch status {
	case types.OutcomeSuccess:
		return exitSuccess
	case types.OutcomeScriptError:
		return exitScriptError
	case types.OutcomeExecutorCrash:
		return exitExecutorCrash
	case types.OutcomePolicyFailure:
		return exitPolicyFailure
	case types.OutcomeVersionMismatch:
		return exitPolicyFailure // non-retryable configuration error, same as policy_failure
	default:
		return exitScriptError
	}
}

// selectProxy loads proxy pools and selects an endpoint.
// Note: The selector is created fresh per invocation (CLI is one-shot).
// Round-robin counters and sticky maps do not persist across runs.
// This is intentional - each run is independent.
//
// configPools are pools defined inline in a quarry.yaml config file.
// They take priority over --proxy-config when present.
func selectProxy(config proxyChoice, runMeta *types.RunMeta, configPools []types.ProxyPool) (*types.ProxyEndpoint, error) {
	var selector *proxy.Selector
	var pools []types.ProxyPool

	if len(configPools) > 0 {
		// Use inline pools from config file
		pools = configPools
		selector = proxy.NewSelector()
		for _, pool := range pools {
			p := pool
			if err := selector.RegisterPool(&p); err != nil {
				return nil, fmt.Errorf("failed to register pool %q: %w", p.Name, err)
			}
		}
	} else if config.configPath != "" {
		// Use legacy --proxy-config JSON file
		var err error
		selector, err = loadAndRegisterPools(config.configPath)
		if err != nil {
			return nil, err
		}
		pools, _ = loadProxyPools(config.configPath)
	} else {
		return nil, errors.New("--proxy-config or config file proxies: required when --proxy-pool is specified")
	}

	// Warn about domain/origin sticky scopes without the required input.
	for _, pool := range pools {
		if pool.Name == config.poolName && pool.Sticky != nil {
			scope := pool.Sticky.Scope
			// Check if required input is missing for the scope
			switch scope {
			case types.ProxyStickyDomain:
				if config.stickyKey == "" && config.domain == "" {
					fmt.Fprintf(os.Stderr, "Warning: pool %q uses domain sticky scope but no --proxy-sticky-key or --proxy-domain provided\n", pool.Name)
				}
			case types.ProxyStickyOrigin:
				if config.stickyKey == "" && config.origin == "" {
					fmt.Fprintf(os.Stderr, "Warning: pool %q uses origin sticky scope but no --proxy-sticky-key or --proxy-origin provided\n", pool.Name)
				}
			}
		}
	}

	// Build selection request (commit for actual runs)
	req := proxy.SelectRequest{
		Pool:      config.poolName,
		StickyKey: config.stickyKey,
		Domain:    config.domain,
		Origin:    config.origin,
		Commit:    true,
	}

	// Set job ID for sticky scope derivation
	if runMeta.JobID != nil {
		req.JobID = *runMeta.JobID
	}

	// Set strategy override if specified
	if config.strategy != "" {
		strategy := types.ProxyStrategy(config.strategy)
		req.StrategyOverride = &strategy
	}

	// Select endpoint
	endpoint, err := selector.Select(req)
	if err != nil {
		return nil, fmt.Errorf("selection failed: %w", err)
	}

	return endpoint, nil
}

// loadAndRegisterPools loads proxy pools from a config file and returns a ready selector.
func loadAndRegisterPools(configPath string) (*proxy.Selector, error) {
	pools, err := loadProxyPools(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load proxy pools: %w", err)
	}
	selector := proxy.NewSelector()
	for _, pool := range pools {
		if err := selector.RegisterPool(&pool); err != nil {
			return nil, fmt.Errorf("failed to register pool %q: %w", pool.Name, err)
		}
	}
	return selector, nil
}

// loadProxyPools loads proxy pools from a JSON config file.
func loadProxyPools(path string) ([]types.ProxyPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var pools []types.ProxyPool
	if err := json.Unmarshal(data, &pools); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	return pools, nil
}

func printRunResult(result *runtime.RunResult, choice policyChoice, duration time.Duration) {
	fmt.Printf("\nrun_id=%s, attempt=%d, outcome=%s, duration=%s\n",
		result.RunMeta.RunID,
		result.RunMeta.Attempt,
		result.Outcome.Status,
		duration.Round(time.Millisecond),
	)

	switch choice.name {
	case "buffered":
		fmt.Printf("policy=%s, flush_mode=%s, drops=%d, buffer_bytes=%d\n",
			choice.name,
			choice.flushMode,
			result.PolicyStats.EventsDropped,
			result.PolicyStats.BufferSize,
		)
	case "streaming":
		fmt.Printf("policy=%s, flush_count=%d, flush_interval=%s, flushes=%d\n",
			choice.name,
			choice.flushCount,
			choice.flushInterval,
			result.PolicyStats.FlushCount,
		)
	default:
		fmt.Printf("policy=%s\n", choice.name)
	}

	fmt.Printf("\n=== Run Result ===\n")
	fmt.Printf("Run ID:       %s\n", result.RunMeta.RunID)
	if result.RunMeta.JobID != nil {
		fmt.Printf("Job ID:       %s\n", *result.RunMeta.JobID)
	}
	if result.RunMeta.ParentRunID != nil {
		fmt.Printf("Parent Run:   %s\n", *result.RunMeta.ParentRunID)
	}
	fmt.Printf("Attempt:      %d\n", result.RunMeta.Attempt)
	fmt.Printf("Outcome:      %s\n", result.Outcome.Status)
	fmt.Printf("Message:      %s\n", result.Outcome.Message)
	fmt.Printf("Duration:     %s\n", result.Duration)
	fmt.Printf("Events:       %d\n", result.EventCount)

	if result.ProxyUsed != nil {
		fmt.Printf("\n=== Proxy ===\n")
		fmt.Printf("Protocol:     %s\n", result.ProxyUsed.Protocol)
		fmt.Printf("Host:         %s\n", result.ProxyUsed.Host)
		fmt.Printf("Port:         %d\n", result.ProxyUsed.Port)
		if result.ProxyUsed.Username != nil {
			fmt.Printf("Username:     %s\n", *result.ProxyUsed.Username)
		}
	}

	fmt.Printf("\n=== Policy Stats ===\n")
	fmt.Printf("Events Total:     %d\n", result.PolicyStats.TotalEvents)
	fmt.Printf("Events Persisted: %d\n", result.PolicyStats.EventsPersisted)
	fmt.Printf("Events Dropped:   %d\n", result.PolicyStats.EventsDropped)
	fmt.Printf("Chunks Total:     %d\n", result.PolicyStats.TotalChunks)
	fmt.Printf("Flushes:          %d\n", result.PolicyStats.FlushCount)

	if result.ArtifactStats.TotalArtifacts > 0 {
		fmt.Printf("\n=== Artifact Stats ===\n")
		fmt.Printf("Total Artifacts:   %d\n", result.ArtifactStats.TotalArtifacts)
		fmt.Printf("Committed:         %d\n", result.ArtifactStats.CommittedArtifacts)
		fmt.Printf("Orphaned:          %d\n", result.ArtifactStats.OrphanedArtifacts)
		fmt.Printf("Total Chunks:      %d\n", result.ArtifactStats.TotalChunks)
		fmt.Printf("Total Bytes:       %d\n", result.ArtifactStats.TotalBytes)
	}

	if len(result.OrphanIDs) > 0 {
		fmt.Printf("\n=== Orphan Artifacts ===\n")
		for _, id := range result.OrphanIDs {
			fmt.Printf("  - %s\n", id)
		}
	}

	if result.StderrOutput != "" {
		fmt.Printf("\n=== Executor Stderr ===\n")
		fmt.Printf("%s", result.StderrOutput)
	}
}

// printMetrics prints the CONTRACT_METRICS.md metrics surface via CLI.
// Uses contract metric names for stable, machine-parseable output.
func printMetrics(snap metrics.Snapshot) {
	fmt.Printf("\n=== Metrics (CONTRACT_METRICS) ===\n")

	// Run lifecycle
	fmt.Printf("runs_started_total:              %d\n", snap.RunsStarted)
	fmt.Printf("runs_completed_total:            %d\n", snap.RunsCompleted)
	fmt.Printf("runs_failed_total:               %d\n", snap.RunsFailed)
	fmt.Printf("runs_crashed_total:              %d\n", snap.RunsCrashed)

	// Ingestion policy
	fmt.Printf("events_received_total:           %d\n", snap.EventsReceived)
	fmt.Printf("events_persisted_total:          %d\n", snap.EventsPersisted)
	fmt.Printf("events_dropped_total:            %d\n", snap.EventsDropped)
	// Deterministic output order for dropped-by-type breakdown
	droppedTypes := sortedKeys(snap.DroppedByType)
	for _, eventType := range droppedTypes {
		fmt.Printf("  events_dropped{type=%s}:      %d\n", eventType, snap.DroppedByType[eventType])
	}

	// Executor
	fmt.Printf("executor_launch_success_total:   %d\n", snap.ExecutorLaunchSuccess)
	fmt.Printf("executor_launch_failure_total:   %d\n", snap.ExecutorLaunchFailure)
	fmt.Printf("executor_crash_total:            %d\n", snap.ExecutorCrash)
	fmt.Printf("ipc_decode_errors_total:         %d\n", snap.IPCDecodeErrors)

	// Lode / Storage (per-call granularity)
	fmt.Printf("lode_write_success_total:        %d\n", snap.LodeWriteSuccess)
	fmt.Printf("lode_write_failure_total:        %d\n", snap.LodeWriteFailure)
	fmt.Printf("lode_write_retry_total:          %d\n", snap.LodeWriteRetry)

	// Dimensions
	fmt.Printf("\n  policy=%s executor=%s storage_backend=%s\n", snap.Policy, snap.Executor, snap.StorageBackend)
}

// sortedKeys returns map keys in sorted order for deterministic output.
func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
