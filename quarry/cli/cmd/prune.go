package cmd

import (
	"errors"
	"fmt"
	"math"
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
	{"unterminated-days", "Prune unterminated runs that started and last wrote at least this many days ago (at least 1; must exceed the longest run)"},
}

// PruneCommand returns the prune command, which removes whole runs by state
// and age (CONTRACT_CLI.md §The `prune` Command).
func PruneCommand() *cli.Command {
	flags := []cli.Flag{FormatFlag, NoColorFlag,
		&cli.BoolFlag{Name: "dry-run", Usage: "Report the runs that would be removed, without removing anything"},
		&cli.StringFlag{Name: "run-id", Usage: "Prune only this run (requires --source, --category, and --day)"},
		&cli.DurationFlag{Name: "sweep-grace", Usage: "Then sweep the whole dataset, reclaiming uncommitted data older than this, such as 24h; 0 skips the sweep"},
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
	if err := rejectEmptySelectors(c); err != nil {
		return cli.Exit(err.Error(), 1)
	}
	policy, err := policyFrom(c)
	if err != nil {
		return cli.Exit(err.Error(), 1)
	}
	factory, err := storeFactoryFrom(c)
	if err != nil {
		return cli.Exit(fmt.Sprintf("failed to initialize storage: %v", err), 1)
	}
	opts := retention.Options{
		Selector:   reader.Selector{Source: c.String("source"), Category: c.String("category"), Day: c.String("day")},
		RunID:      c.String("run-id"),
		Policy:     policy,
		SweepGrace: c.Duration("sweep-grace"),
		DryRun:     c.Bool("dry-run"),
		Now:        time.Now(),
	}

	// No deadline, as for index rebuild: a prune may cover the whole
	// dataset. It is interruptible, and a rerun resumes it.
	report, pruneErr := retention.Prune(c.Context, c.String("storage-dataset"), factory, opts)
	if errors.Is(pruneErr, retention.ErrInvalidOptions) {
		// Refused before anything is read: there is no report to render,
		// and a rerun with the same options is refused again.
		return cli.Exit(pruneErr.Error(), 1)
	}
	if err := renderPrune(r, report); err != nil {
		return err
	}
	if pruneErr != nil {
		return cli.Exit(fmt.Sprintf("prune failed (rerun it to resume): %v", pruneErr), 1)
	}
	return nil
}

// rejectEmptySelectors refuses a selector given with an empty value, such as
// an unset shell variable: for a deletion, an empty selector would widen the
// prune to every run instead of narrowing it.
func rejectEmptySelectors(c *cli.Context) error {
	for _, name := range []string{"source", "category", "day", "run-id"} {
		if c.IsSet(name) && c.String(name) == "" {
			return fmt.Errorf("--%s must not be empty", name)
		}
	}
	return nil
}

// maxDays is the largest day count a time.Duration holds.
const maxDays = math.MaxInt64 / int64(24*time.Hour)

// policyFrom reads the age flags; a flag that is not set selects nothing.
// A count outside [0, maxDays] is refused before conversion, since an
// overflowing duration could wrap to a small or zero age.
func policyFrom(c *cli.Context) (retention.Policy, error) {
	var policy retention.Policy
	for _, f := range []struct {
		name string
		dst  **time.Duration
	}{{"succeeded-days", &policy.Succeeded}, {"failed-days", &policy.Failed}, {"unterminated-days", &policy.Unterminated}} {
		if !c.IsSet(f.name) {
			continue
		}
		n := int64(c.Int(f.name))
		if n < 0 || n > maxDays {
			return policy, fmt.Errorf("--%s must be between 0 and %d", f.name, maxDays)
		}
		d := time.Duration(n) * 24 * time.Hour
		*f.dst = &d
	}
	return policy, nil
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
	fmt.Fprintf(os.Stderr, "Scanned %d runs: %d not due, %s %d (%d snapshots, %d objects, %d bytes, %d sidecar objects), kept %d; %s %d orphaned end records\n",
		report.RunsScanned, report.NotDue, verb, report.RunsPruned, report.Snapshots, report.Objects, report.Bytes, report.SidecarObjects, report.RunsKept, verb, report.OrphanEndRecords)
	for _, key := range report.Unparseable {
		fmt.Fprintf(os.Stderr, "Not addressed (malformed start-record key): %s\n", key)
	}
	if s := report.Sweep; s != nil {
		fmt.Fprintf(os.Stderr, "Sweep: %s %d objects (%d bytes recorded, %d orphans), settled %d expiries, %d unparseable\n",
			verb, s.Objects, s.Bytes, s.Orphans, s.Settled, len(s.Unparseable))
	}
	return r.Render(report.Runs)
}
