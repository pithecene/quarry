package cmd

import (
	"fmt"

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
		Usage: "Show aggregated statistics (metrics)",
		Subcommands: []*cli.Command{
			statsMetricsCommand(),
		},
	}
}

func statsMetricsCommand() *cli.Command {
	return &cli.Command{
		Name:  "metrics",
		Usage: "Show contract metrics (run lifecycle, ingestion, executor, storage) from storage",
		Flags: append(append(ReadOnlyFlags(), storageReadFlags()...),
			&cli.StringFlag{Name: "run-id", Usage: "Read metrics for specific run ID"},
			&cli.StringFlag{Name: "source", Usage: "Filter by source partition"},
			readTimeoutFlag(),
		),
		Action: statsMetricsAction,
	}
}

func statsMetricsAction(c *cli.Context) error {
	if err := requireStorage(c, "stats metrics", readsStorage); err != nil {
		return err
	}
	// The store is built exactly as `quarry run` builds it, so data written
	// to an S3-compatible provider can be read back from it.
	factory, err := storeFactoryFrom(c)
	if err != nil {
		return fmt.Errorf("failed to initialize storage reader: %w", err)
	}

	// Without --run-id the read takes each end record that matches, so its
	// length follows the dataset's size and the caller sets the bound.
	ctx, cancel := readContext(c)
	defer cancel()

	record, err := lode.QueryLatestMetrics(ctx, factory, c.String("storage-dataset"), c.String("run-id"), c.String("source"))
	if err != nil {
		return fmt.Errorf("failed to read metrics from Lode: %w", err)
	}

	snapshot, err := reader.ParseMetricsRecord(record)
	if err != nil {
		return fmt.Errorf("failed to parse metrics record: %w", err)
	}

	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	return r.Render(snapshot)
}
