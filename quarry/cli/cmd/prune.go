package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/render"
	"github.com/pithecene/quarry/reader"
	"github.com/pithecene/quarry/retention"
)

// pruneAgeFlags give, per run state, the age in days at which a run in that
// state is pruned. An unset flag keeps every run in that state.
var pruneAgeFlags = []struct{ name, usage string }{
	{"succeeded-days", "Prune succeeded runs that ended at least this many days ago (0 prunes them all)"},
	{"failed-days", "Prune failed runs that ended at least this many days ago (0 prunes them all)"},
	{"unterminated-days", "Prune unterminated runs that started at least this many days ago (at least 1; must exceed the longest run)"},
}

// PruneCommand returns the prune command, which removes whole runs by state
// and age (CONTRACT_CLI.md §prune).
func PruneCommand() *cli.Command {
	flags := []cli.Flag{FormatFlag, NoColorFlag,
		&cli.BoolFlag{Name: "dry-run", Usage: "Report the runs that would be removed, without removing anything"},
		&cli.StringFlag{Name: "run-id", Usage: "Prune only this run (requires --source, --category, and --day)"},
		&cli.DurationFlag{Name: "sweep-grace", Usage: "Then sweep the whole dataset, reclaiming uncommitted data older than this (e.g. 24h); 0 skips the sweep"},
	}
	for _, f := range pruneAgeFlags {
		flags = append(flags, &cli.IntFlag{Name: f.name, Usage: f.usage, DefaultText: "unset: keep them"})
	}
	flags = append(append(flags, storageReadFlags()...), runSelectorFlags()...)
	return &cli.Command{
		Name:   "prune",
		Usage:  "Remove whole runs by state and age (writes storage)",
		Flags:  flags,
		Action: pruneAction,
	}
}

func pruneAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}
	ok, err := storageRequested(c)
	if err != nil {
		return cli.Exit(err.Error(), 1)
	}
	if !ok {
		return cli.Exit("prune writes storage: --storage-backend and --storage-path are required", 1)
	}
	factory, err := storeFactoryFrom(c)
	if err != nil {
		return cli.Exit(fmt.Sprintf("failed to initialize storage: %v", err), 1)
	}
	opts := retention.Options{
		Selector:   reader.Selector{Source: c.String("source"), Category: c.String("category"), Day: c.String("day")},
		RunID:      c.String("run-id"),
		Policy:     policyFrom(c),
		SweepGrace: c.Duration("sweep-grace"),
		DryRun:     c.Bool("dry-run"),
		Now:        time.Now(),
	}

	// No deadline, as for index rebuild: a prune may cover the whole
	// dataset. It is interruptible, and a rerun resumes it.
	report, pruneErr := retention.Prune(c.Context, c.String("storage-dataset"), factory, opts)
	if err := renderPrune(r, report); err != nil {
		return err
	}
	if pruneErr != nil {
		return cli.Exit(fmt.Sprintf("prune failed (rerun it to resume): %v", pruneErr), 1)
	}
	return nil
}

// policyFrom reads the age flags; a flag that is not set selects nothing.
func policyFrom(c *cli.Context) retention.Policy {
	age := func(name string) *time.Duration {
		if !c.IsSet(name) {
			return nil
		}
		d := time.Duration(c.Int(name)) * 24 * time.Hour
		return &d
	}
	return retention.Policy{Succeeded: age("succeeded-days"), Failed: age("failed-days"), Unterminated: age("unterminated-days")}
}

// renderPrune renders the report; the table shows the selected runs and
// prints the totals to stderr.
func renderPrune(r *render.Renderer, report retention.Report) error {
	if r.Format() != render.FormatTable {
		return r.Render(report)
	}
	verb := "removed"
	if report.DryRun {
		verb = "would remove"
	}
	fmt.Fprintf(os.Stderr, "Scanned %d runs: %d not due, %s %d (%d snapshots, %d objects, %d bytes, %d sidecar objects), kept %d\n",
		report.RunsScanned, report.NotDue, verb, report.RunsPruned, report.Snapshots, report.Objects, report.Bytes, report.SidecarObjects, report.RunsKept)
	for _, key := range report.Unparseable {
		fmt.Fprintf(os.Stderr, "Not addressed (malformed start-record key): %s\n", key)
	}
	if s := report.Sweep; s != nil {
		fmt.Fprintf(os.Stderr, "Sweep: %s %d objects (%d bytes recorded, %d orphans), settled %d expiries, %d unparseable\n",
			verb, s.Objects, s.Bytes, s.Orphans, s.Settled, len(s.Unparseable))
	}
	return r.Render(report.Runs)
}
