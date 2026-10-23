package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	lodelibrary "github.com/pithecene/lode/lode"
	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/reader"
	"github.com/pithecene/quarry/cli/render"
	"github.com/pithecene/quarry/lode"
)

// StatsCommand returns the stats command with subcommands.
// Stats returns aggregated, derived facts per CONTRACT_CLI.md.
func StatsCommand() *cli.Command {
	return &cli.Command{
		Name:  "stats",
		Usage: "Show aggregated statistics (runs, jobs, tasks, proxies, executors)",
		Subcommands: []*cli.Command{
			statsRunsCommand(),
			statsJobsCommand(),
			statsTasksCommand(),
			statsProxiesCommand(),
			statsExecutorsCommand(),
			statsMetricsCommand(),
		},
	}
}

func statsRunsCommand() *cli.Command {
	return &cli.Command{
		Name:   "runs",
		Usage:  "Show run statistics",
		Flags:  TUIReadOnlyFlags(),
		Action: statsRunsAction,
	}
}

func statsRunsAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return r.RenderTUI("stats_runs", reader.StatsRuns())
	}

	return r.Render(reader.StatsRuns())
}

func statsJobsCommand() *cli.Command {
	return &cli.Command{
		Name:   "jobs",
		Usage:  "Show job statistics",
		Flags:  TUIReadOnlyFlags(),
		Action: statsJobsAction,
	}
}

func statsJobsAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return r.RenderTUI("stats_jobs", reader.StatsJobs())
	}

	return r.Render(reader.StatsJobs())
}

func statsTasksCommand() *cli.Command {
	return &cli.Command{
		Name:   "tasks",
		Usage:  "Show task statistics",
		Flags:  TUIReadOnlyFlags(),
		Action: statsTasksAction,
	}
}

func statsTasksAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return r.RenderTUI("stats_tasks", reader.StatsTasks())
	}

	return r.Render(reader.StatsTasks())
}

func statsProxiesCommand() *cli.Command {
	return &cli.Command{
		Name:   "proxies",
		Usage:  "Show proxy statistics",
		Flags:  TUIReadOnlyFlags(),
		Action: statsProxiesAction,
	}
}

func statsProxiesAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return r.RenderTUI("stats_proxies", reader.StatsProxies())
	}

	return r.Render(reader.StatsProxies())
}

func statsExecutorsCommand() *cli.Command {
	return &cli.Command{
		Name:   "executors",
		Usage:  "Show executor statistics",
		Flags:  TUIReadOnlyFlags(),
		Action: statsExecutorsAction,
	}
}

func statsExecutorsAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return r.RenderTUI("stats_executors", reader.StatsExecutors())
	}

	return r.Render(reader.StatsExecutors())
}

func statsMetricsCommand() *cli.Command {
	return &cli.Command{
		Name:  "metrics",
		Usage: "Show contract metrics (run lifecycle, ingestion, executor, storage)",
		Flags: append(append(TUIReadOnlyFlags(), storageReadFlags()...),
			&cli.StringFlag{Name: "run-id", Usage: "Read metrics for specific run ID"},
			&cli.StringFlag{Name: "source", Usage: "Filter by source partition"},
		),
		Action: statsMetricsAction,
	}
}

func statsMetricsAction(c *cli.Context) error {
	backend := c.String("storage-backend")
	path := c.String("storage-path")

	var snapshot *reader.MetricsSnapshot

	withStorage, err := storageRequested(c)
	if err != nil {
		return err
	}
	if withStorage {
		// Build Lode dataset for reading
		ds, err := buildReadDataset(c, backend, path)
		if err != nil {
			return fmt.Errorf("failed to initialize storage reader: %w", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		record, err := lode.QueryLatestMetrics(ctx, ds, c.String("run-id"), c.String("source"))
		if err != nil {
			return fmt.Errorf("failed to read metrics from Lode: %w", err)
		}

		parsed, err := reader.ParseMetricsRecord(record)
		if err != nil {
			return fmt.Errorf("failed to parse metrics record: %w", err)
		}
		snapshot = parsed
	} else {
		snapshot = reader.StatsMetrics()
	}

	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return r.RenderTUI("stats_metrics", snapshot)
	}

	return r.Render(snapshot)
}

// buildReadDataset creates a Lode Dataset for reading based on CLI flags.
// S3 options mirror `quarry run`, so data written to an S3-compatible
// provider can be read back from it.
func buildReadDataset(c *cli.Context, backend, path string) (lodelibrary.Dataset, error) {
	dataset := c.String("storage-dataset")
	endpoint := c.String("storage-endpoint")
	usePathStyle := c.Bool("storage-s3-path-style")
	switch backend {
	case "fs":
		if endpoint != "" || usePathStyle {
			fmt.Fprintf(os.Stderr, "Warning: --storage-endpoint and --storage-s3-path-style are ignored for fs backend\n")
		}
		return lode.NewReadDatasetFS(dataset, path)
	case "s3":
		bucket, prefix := lode.ParseS3Path(path)
		return lode.NewReadDatasetS3(dataset, lode.S3Config{
			Bucket:       bucket,
			Prefix:       prefix,
			Region:       c.String("storage-region"),
			Endpoint:     endpoint,
			UsePathStyle: usePathStyle,
		})
	default:
		return nil, fmt.Errorf("unsupported storage-backend: %s (must be fs or s3)", backend)
	}
}
