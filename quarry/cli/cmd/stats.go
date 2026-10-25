package cmd

import (
	"context"
	"fmt"
	"time"

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
		Usage: "Show aggregated statistics (runs, proxies, metrics)",
		Subcommands: []*cli.Command{
			statsRunsCommand(),
			statsProxiesCommand(),
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
	var snapshot *reader.MetricsSnapshot

	withStorage, err := storageRequested(c)
	if err != nil {
		return err
	}
	if withStorage {
		// The store is built exactly as `quarry run` builds it, so data written
		// to an S3-compatible provider can be read back from it.
		factory, err := storeFactoryFrom(c)
		if err != nil {
			return fmt.Errorf("failed to initialize storage reader: %w", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		record, err := lode.QueryLatestMetrics(ctx, factory, c.String("storage-dataset"), c.String("run-id"), c.String("source"))
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
