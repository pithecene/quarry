package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/log"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// storageChoice holds parsed storage configuration.
type storageChoice struct {
	backend      string // "fs" or "s3"
	path         string // fs: directory, s3: bucket/prefix
	region       string // AWS region for S3 (optional)
	endpoint     string // custom S3 endpoint for S3-compatible providers (optional)
	usePathStyle bool   // force path-style addressing for S3 (optional)
	// The longest stall of an S3 request before its attempt fails. Zero
	// means the default.
	attemptTimeout time.Duration
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
			return fmt.Errorf("cannot access storage path %q: %w (ensure the path exists and is readable)", config.path, err)
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

// buildStorageSink creates a Lode storage sink based on CLI configuration.
// Storage backend and path are required - no silent fallback to stub.
// It returns the sink, instrumented by the collector, and the client, which also
// writes the run's sidecar files, artifacts, and records.
func buildStorageSink(ctx context.Context, storageConfig storageChoice, dataset, source, category string, runMeta *types.RunMeta, policy string, startTime time.Time, collector *metrics.Collector) (policy.Sink, *lode.LodeClient, error) {
	logger := log.NewLogger(runMeta)
	// Build Lode config with partition keys
	cfg := lode.Config{
		Dataset:              dataset,
		Source:               source,
		Category:             category,
		Day:                  lode.DeriveDay(startTime),
		RunID:                runMeta.RunID,
		Policy:               policy,
		RetryObserver:        func(int) { collector.IncLodeWriteRetry() },
		IndexFailureObserver: func(string, error) { collector.IncArtifactIndexWriteFailure() },
		ReconcileObserver: func(key string, err error) {
			logger.Warn("storage write reported an error, and the stored bytes match", map[string]any{
				"key":   key,
				"error": err.Error(),
			})
		},
	}

	var lc *lode.LodeClient
	var err error

	switch storageConfig.backend {
	case "fs":
		lc, err = lode.NewLodeClient(ctx, cfg, storageConfig.path)
		if err != nil {
			return nil, nil, fmt.Errorf("filesystem storage initialization failed: %w (ensure directory %s exists and is writable)", err, storageConfig.path)
		}
	case "s3":
		bucket, prefix := lode.ParseS3Path(storageConfig.path)
		s3cfg := lode.S3Config{
			Bucket:         bucket,
			Prefix:         prefix,
			Region:         storageConfig.region,
			Endpoint:       storageConfig.endpoint,
			UsePathStyle:   storageConfig.usePathStyle,
			AttemptTimeout: storageConfig.attemptTimeout,
		}
		lc, err = lode.NewLodeS3Client(ctx, cfg, s3cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("S3 storage initialization failed: %w (check AWS credentials and bucket permissions)", err)
		}
	default:
		// Should not reach here due to validateStorageConfig
		return nil, nil, fmt.Errorf("unknown storage-backend: %s", storageConfig.backend)
	}

	return lode.NewInstrumentedSink(lode.NewSink(cfg, lc), collector), lc, nil
}

// storageRoot is the absolute directory that Lode's filesystem store opens for
// path. The store cleans path, and the operating system resolves the result
// against the physical working directory, which filepath.Abs does not use.
func storageRoot(path string) (string, error) {
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	if wd, err = filepath.EvalSymlinks(wd); err != nil {
		return "", err
	}
	return filepath.Join(wd, path), nil
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
