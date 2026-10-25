package cmd

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/render"
	"github.com/pithecene/quarry/lode"
)

// IndexCommand returns the index command, which maintains Quarry's run
// records (CONTRACT_CLI.md §The `index` Command).
func IndexCommand() *cli.Command {
	return &cli.Command{
		Name:  "index",
		Usage: "Maintain the run records that list runs reads",
		Subcommands: []*cli.Command{
			{
				Name:  "rebuild",
				Usage: "Backfill run records for runs written before v0.14.0",
				Flags: append([]cli.Flag{FormatFlag, NoColorFlag,
					&cli.BoolFlag{Name: "dry-run", Usage: "Report the records that would be written, without writing them"},
				}, storageReadFlags()...),
				Action: indexRebuildAction,
			},
		},
	}
}

func indexRebuildAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}
	ok, err := storageRequested(c)
	if err != nil {
		return cli.Exit(err.Error(), 1)
	}
	if !ok {
		return cli.Exit("index rebuild writes storage: --storage-backend and --storage-path are required", 1)
	}
	factory, err := storeFactoryFrom(c)
	if err != nil {
		return cli.Exit(fmt.Sprintf("failed to initialize storage: %v", err), 1)
	}

	// No deadline: a rebuild scans the whole dataset. It is interruptible,
	// and a rerun resumes where it stopped.
	report, rebuildErr := lode.RebuildRunRecords(c.Context, c.String("storage-dataset"), factory, lode.RebuildOptions{DryRun: c.Bool("dry-run")})
	if err := renderRebuild(r, report); err != nil {
		return err
	}
	if rebuildErr != nil {
		return cli.Exit(fmt.Sprintf("index rebuild failed (rerun it to resume): %v", rebuildErr), 1)
	}
	return nil
}

// renderRebuild renders the report; the table shows the listed runs and
// prints the totals to stderr.
func renderRebuild(r *render.Renderer, report lode.RebuildReport) error {
	if r.Format() != render.FormatTable {
		return r.Render(report)
	}
	verb := "wrote"
	if report.DryRun {
		verb = "would write"
	}
	fmt.Fprintf(os.Stderr, "Scanned %d runs: %d already indexed, %s %d start and %d end records, %d skipped\n",
		report.RunsScanned, report.AlreadyIndexed, verb, report.StartsWritten, report.EndsWritten, report.Skipped)
	return r.Render(report.Runs)
}
