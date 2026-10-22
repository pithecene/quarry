package cmd

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/reader"
	"github.com/pithecene/quarry/cli/render"
)

// listWarningThreshold is the number of items above which we warn about using --limit.
const listWarningThreshold = 100

// isStderrTTY returns true if stderr is a TTY.
func isStderrTTY() bool {
	info, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

// ListCommand returns the list command with subcommands.
// List returns thin slices (not inspect-level detail) per CONTRACT_CLI.md.
func ListCommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List entities (runs, jobs, pools, executors)",
		Subcommands: []*cli.Command{
			listRunsCommand(),
			listEventsCommand(),
			listJobsCommand(),
			listPoolsCommand(),
			listExecutorsCommand(),
		},
	}
}

func listRunsCommand() *cli.Command {
	return &cli.Command{
		Name:  "runs",
		Usage: "List runs",
		Flags: append(append(append(ReadOnlyFlags(), storageReadFlags()...), runSelectorFlags()...),
			&cli.StringFlag{
				Name:  "state",
				Usage: "Filter by state: succeeded, failed, unterminated (with storage); running, failed, succeeded (stub)",
			},
			&cli.IntFlag{
				Name:  "limit",
				Usage: "Maximum number of runs to return (0 = no limit)",
				Value: 0,
			},
			&cli.StringFlag{
				Name:  "after",
				Usage: "Resume after the cursor a previous page returned (with storage)",
			},
		),
		Action: listRunsAction,
	}
}

func listRunsAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	// TUI not supported for list commands
	if c.Bool("tui") {
		return cli.Exit("--tui is not supported for list commands", 1)
	}

	// With storage, runs come from run records; without, from the stub.
	withStorage, err := storageRequested(c)
	if err != nil {
		return cli.Exit(err.Error(), 1)
	}
	if withStorage {
		return listRunsFromRecords(c, r)
	}
	// Selectors and cursors have meaning only against storage; never ignore
	// them silently on the stub path.
	for _, name := range []string{"after", "source", "category", "day"} {
		if c.IsSet(name) {
			return cli.Exit(fmt.Sprintf("--%s requires --storage-backend and --storage-path", name), 1)
		}
	}
	switch st := c.String("state"); st {
	case "", "running", "failed", "succeeded":
	default:
		return cli.Exit(fmt.Sprintf("--state %q: stub data has states running, failed, succeeded (succeeded, failed, unterminated need storage)", st), 1)
	}

	opts := reader.ListRunsOptions{
		State: c.String("state"),
		Limit: c.Int("limit"),
	}

	results := reader.ListRuns(opts)

	// Warn if output is large and --limit was not specified (TTY only to avoid noise in pipelines)
	if len(results) > listWarningThreshold && opts.Limit == 0 && isStderrTTY() {
		fmt.Fprintf(os.Stderr, "Warning: returning %d results. Consider using --limit to reduce output.\n\n", len(results))
	}

	return r.Render(results)
}

func listJobsCommand() *cli.Command {
	return &cli.Command{
		Name:   "jobs",
		Usage:  "List jobs",
		Flags:  ReadOnlyFlags(),
		Action: listJobsAction,
	}
}

func listJobsAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return cli.Exit("--tui is not supported for list commands", 1)
	}

	return r.Render(reader.ListJobs())
}

func listPoolsCommand() *cli.Command {
	return &cli.Command{
		Name:   "pools",
		Usage:  "List proxy pools",
		Flags:  ReadOnlyFlags(),
		Action: listPoolsAction,
	}
}

func listPoolsAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return cli.Exit("--tui is not supported for list commands", 1)
	}

	return r.Render(reader.ListPools())
}

func listExecutorsCommand() *cli.Command {
	return &cli.Command{
		Name:   "executors",
		Usage:  "List executors",
		Flags:  ReadOnlyFlags(),
		Action: listExecutorsAction,
	}
}

func listExecutorsAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return cli.Exit("--tui is not supported for list commands", 1)
	}

	return r.Render(reader.ListExecutors())
}
