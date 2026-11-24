package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/adapter/redisstream"
	"github.com/pithecene/quarry/adapter/webhook"
	quarryconfig "github.com/pithecene/quarry/cli/config"
	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/log"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/runtime"
	"github.com/pithecene/quarry/types"
)

// Exit codes per CONTRACT_RUN.md.
const (
	exitSuccess       = 0
	exitScriptError   = 1
	exitExecutorCrash = 2
	exitPolicyFailure = 3
	exitStorageError  = 4
	exitDeadline      = 5
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

  # Run with an S3-compatible provider
  quarry run --script ./script.ts --run-id run-005 --source my-source \
    --storage-backend s3 --storage-path my-bucket/prefix \
    --storage-endpoint https://s3.example.com \
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
				Name:  "no-browser",
				Usage: "Run without a browser: the executor launches none, and ctx.page, ctx.browser and ctx.browserContext throw NoBrowserError",
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
				Usage: "Custom S3 endpoint URL for S3-compatible providers",
			},
			&cli.BoolFlag{
				Name:  "storage-s3-path-style",
				Usage: "Force path-style addressing for S3 (required by many S3-compatible providers)",
			},
			&cli.DurationFlag{
				Name:  "storage-attempt-timeout",
				Usage: "Longest stall of an S3 request before its attempt fails and the SDK retries it (default 60s)",
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
			// Deadline flag
			&cli.DurationFlag{
				Name:  "max-duration",
				Usage: "Stop the run and end it as deadline_exceeded once it has run this long (0 = no deadline)",
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
			&cli.BoolFlag{
				Name:  "adapter-publish-endpoint-path",
				Usage: "Publish the path of --storage-endpoint in notifications",
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
	maxEvents     int
	maxBytes      int64
	flushCount    int
	flushInterval time.Duration
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

func validatePolicyConfig(choice policyChoice) error {
	switch choice.name {
	case "strict":
		if choice.maxEvents > 0 || choice.maxBytes > 0 {
			fmt.Fprintf(os.Stderr, "Warning: buffer flags ignored for strict policy\n")
		}
		return nil

	case "buffered":
		if choice.maxEvents <= 0 && choice.maxBytes <= 0 {
			return fmt.Errorf(`buffered policy requires buffer limits

Add one or both of:
  --buffer-events <n>   Maximum events to buffer (e.g., --buffer-events 1000)
  --buffer-bytes <n>    Maximum bytes to buffer (e.g., --buffer-bytes 1048576)`)
		}
		return nil

	case "streaming":
		if choice.flushCount <= 0 && choice.flushInterval <= 0 {
			return fmt.Errorf(`streaming policy requires at least one flush trigger

Add one or both of:
  --flush-count <n>       Flush after N events (e.g., --flush-count 100)
  --flush-interval <d>    Flush every duration (e.g., --flush-interval 5s)`)
		}
		// Warn about irrelevant buffered flags
		if choice.maxEvents > 0 || choice.maxBytes > 0 {
			fmt.Fprintf(os.Stderr, "Warning: buffer flags ignored for streaming policy\n")
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

func runAction(c *cli.Context) error {
	// Load config file if --config is provided
	cfg := &quarryconfig.Config{}
	if configPath := c.String("config"); configPath != "" {
		loaded, err := quarryconfig.Load(configPath)
		if err != nil {
			return cli.Exit(fmt.Sprintf("failed to load config: %v", err), exitConfigError)
		}
		cfg = loaded
	}

	// Resolve values with precedence: CLI flag > config file > flag default
	source := resolve(c, "source", cfg.Source, (*cli.Context).String)
	category := resolve(c, "category", cfg.Category, (*cli.Context).String)
	executor := resolve(c, "executor", cfg.Executor, (*cli.Context).String)
	browserWSEndpoint := resolve(c, "browser-ws-endpoint", cfg.BrowserWSEndpoint, (*cli.Context).String)
	noBrowser := c.Bool("no-browser")
	if noBrowser && browserWSEndpoint != "" {
		return cli.Exit("--no-browser cannot be combined with --browser-ws-endpoint", exitConfigError)
	}
	resolveFrom := resolve(c, "resolve-from", cfg.ResolveFrom, (*cli.Context).String)

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
		name:          resolve(c, "policy", cfg.Policy.Name, (*cli.Context).String),
		maxEvents:     resolve(c, "buffer-events", cfg.Policy.BufferEvents, (*cli.Context).Int),
		maxBytes:      resolve(c, "buffer-bytes", cfg.Policy.BufferBytes, (*cli.Context).Int64),
		flushCount:    resolve(c, "flush-count", cfg.Policy.FlushCount, (*cli.Context).Int),
		flushInterval: resolve(c, "flush-interval", cfg.Policy.FlushInterval.Duration, (*cli.Context).Duration),
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
	storageBackend := resolve(c, "storage-backend", cfg.Storage.Backend, (*cli.Context).String)
	storagePath := resolve(c, "storage-path", cfg.Storage.Path, (*cli.Context).String)

	if storageBackend == "" {
		return cli.Exit("--storage-backend is required (provide via CLI flag or config file)", exitConfigError)
	}
	if storagePath == "" {
		return cli.Exit("--storage-path is required (provide via CLI flag or config file)", exitConfigError)
	}

	storageConfig := storageChoice{
		backend:        storageBackend,
		path:           storagePath,
		region:         resolve(c, "storage-region", cfg.Storage.Region, (*cli.Context).String),
		endpoint:       resolve(c, "storage-endpoint", cfg.Storage.Endpoint, (*cli.Context).String),
		usePathStyle:   resolve(c, "storage-s3-path-style", cfg.Storage.S3PathStyle, (*cli.Context).Bool),
		attemptTimeout: resolve(c, "storage-attempt-timeout", cfg.Storage.AttemptTimeout.Duration, (*cli.Context).Duration),
	}
	if err := validateStorageConfig(storageConfig); err != nil {
		return cli.Exit(err.Error(), exitConfigError)
	}

	storageDataset := resolve(c, "storage-dataset", cfg.Storage.Dataset, (*cli.Context).String)
	if err := lode.ValidateRunKey(storageDataset, source, category, runMeta.RunID); err != nil {
		return cli.Exit(err.Error(), exitConfigError)
	}

	// Parse and validate adapter config (pre-execution: fail fast on bad config)
	var adptConfig *adapterChoice
	adapterType := resolve(c, "adapter", cfg.Adapter.Type, (*cli.Context).String)
	if adapterType != "" {
		ac, err := parseAdapterConfigWithPrecedence(c, cfg, adapterType)
		if err != nil {
			return cli.Exit(fmt.Sprintf("invalid adapter config: %v", err), exitConfigError)
		}
		if err := validateLocatorEndpoint(storageConfig, ac.publishEndpointPath); err != nil {
			return cli.Exit(err.Error(), exitConfigError)
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
	pol, lodeClient, err := buildPolicy(c.Context, choice, storageConfig, storageDataset, source, category, runMeta, startTime, collector, eventSinks)
	if err != nil {
		return fmt.Errorf("failed to create policy: %w", err)
	}
	defer iox.DiscardClose(pol)

	// Resolve proxy pools from config file (inline proxies: key)
	configPools := cfg.ProxyPools()

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
		poolName:   resolve(c, "proxy-pool", cfg.Proxy.Pool, (*cli.Context).String),
		strategy:   resolve(c, "proxy-strategy", cfg.Proxy.Strategy, (*cli.Context).String),
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
	noBrowserReuse := resolve(c, "no-browser-reuse", cfg.NoBrowserReuse, (*cli.Context).Bool)
	if browserWSEndpoint == "" && !noBrowserReuse && !noBrowser {
		idleTimeout := runtime.IdleTimeoutFromEnv()
		reuseCfg := runtime.ReusableBrowserConfig{
			ExecutorPath: executorPath,
			ScriptPath:   c.String("script"),
			Proxy:        resolvedProxy,
			ResolveFrom:  resolveFrom,
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
		FileWriter:        lodeClient,
		ArtifactStore:     lodeClient,
		BrowserWSEndpoint: browserWSEndpoint,
		NoBrowser:         noBrowser,
		ResolveFrom:       resolveFrom,
		Source:            source,
		Category:          category,
		StorageDataset:    storageDataset,
		StorageDay:        lode.DeriveDay(startTime),
		Collector:         collector,
		Storage:           lodeClient,
		StartedAt:         startTime,
		MaxDuration:       c.Duration("max-duration"),
	}

	// Branch: fan-out or single run
	if fanOut.depth > 0 {
		// Use reusable browser if already acquired; otherwise launch a managed
		// browser for fan-out to avoid N cold startups (one per child run).
		if browserWSEndpoint == "" && !noBrowser {
			managedBrowser, err := runtime.LaunchManagedBrowser(ctx, executorPath, c.String("script"), resolveFrom, resolvedProxy)
			if err != nil {
				return cli.Exit(fmt.Sprintf("failed to launch shared browser: %v", err), exitExecutorCrash)
			}
			defer iox.DiscardClose(managedBrowser)
			browserWSEndpoint = managedBrowser.WSEndpoint
			rootConfig.BrowserWSEndpoint = browserWSEndpoint
		}

		factory := &childFactory{
			maxDuration:       rootConfig.MaxDuration,
			policyChoice:      choice,
			executorPath:      executorPath,
			storage:           storageConfig,
			storageDataset:    storageDataset,
			source:            source,
			category:          category,
			proxy:             resolvedProxy,
			browserWSEndpoint: browserWSEndpoint,
			noBrowser:         noBrowser,
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

	finalizer.Finalize(ctx, result)
	return cli.Exit("", runExitCode(result))
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

	finalizer.Finalize(ctx, rootResult)

	// Print fan-out summary
	if !finalizer.quiet {
		fanOutResult := operator.Results()
		runtime.PrintFanOutSummary(fanOutResult)
	}

	// The root run alone determines the exit code.
	return cli.Exit("", runExitCode(rootResult))
}

func buildPolicy(ctx context.Context, choice policyChoice, storageConfig storageChoice, dataset, source, category string, runMeta *types.RunMeta, startTime time.Time, collector *metrics.Collector, eventSinkConfigs []eventSinkChoice) (policy.Policy, *lode.LodeClient, error) {
	lodeSink, client, err := buildStorageSink(ctx, storageConfig, dataset, source, category, runMeta, choice.name, startTime, collector)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create storage sink: %w", err)
	}

	sink, err := buildEffectiveSink(eventSinkConfigs, lodeSink, source, category)
	if err != nil {
		return nil, nil, err
	}

	switch choice.name {
	case "strict":
		return policy.NewStrictPolicy(sink), client, nil

	case "buffered":
		config := policy.BufferedConfig{
			MaxBufferEvents: choice.maxEvents,
			MaxBufferBytes:  choice.maxBytes,
			Logger:          log.NewLogger(runMeta),
		}
		p, err := policy.NewBufferedPolicy(sink, config)
		return p, client, err

	case "streaming":
		config := policy.StreamingConfig{
			FlushCount:    choice.flushCount,
			FlushInterval: choice.flushInterval,
			Logger:        log.NewLogger(runMeta),
		}
		p, err := policy.NewStreamingPolicy(sink, config)
		return p, client, err

	default:
		return nil, nil, fmt.Errorf("unknown policy: %s", choice.name)
	}
}
