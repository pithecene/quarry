package cmd

import (
	"context"
	"fmt"

	lodelib "github.com/pithecene/lode/lode"
	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/reader"
	"github.com/pithecene/quarry/cli/render"
	"github.com/pithecene/quarry/lode"
	runreader "github.com/pithecene/quarry/reader"
)

// InspectCommand returns the inspect command with subcommands.
// Inspect returns a deep view of a single entity per CONTRACT_CLI.md.
func InspectCommand() *cli.Command {
	return &cli.Command{
		Name:  "inspect",
		Usage: "Inspect a single entity (run, proxy)",
		Subcommands: []*cli.Command{
			inspectRunCommand(),
			inspectProxyCommand(),
		},
	}
}

func inspectRunCommand() *cli.Command {
	flags := append(append(TUIReadOnlyFlags(), storageReadFlags()...), runSelectorFlags()...)
	return &cli.Command{
		Name:      "run",
		Usage:     "Inspect a run by ID (reads storage)",
		ArgsUsage: "<run-id>",
		Flags:     append(flags, readTimeoutFlag()),
		Action:    inspectRunAction,
	}
}

// inspectRunAction reads the run that the selector and run ID locate, the
// policy that its metrics record names, and its sidecar files
// (CONTRACT_CLI.md §inspect run).
func inspectRunAction(c *cli.Context) error {
	if c.NArg() < 1 {
		return cli.Exit("run-id required", 1)
	}
	runID := c.Args().First()

	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}
	ctx, cancel := readContext(c)
	defer cancel()
	_, run, factory, err := locateRun(ctx, c, "inspect run", runID)
	if err != nil {
		return err
	}
	resp, err := inspectRunDetails(ctx, c, factory, runID, run)
	if err != nil {
		return cli.Exit(fmt.Sprintf("inspect run: %v", err), 1)
	}

	if c.Bool("tui") {
		return r.RenderTUI("inspect_run", resp)
	}
	return r.Render(resp)
}

// inspectRunDetails adds to run the policy of its latest metrics record, or
// none when the run persisted no metrics, and its sidecar files.
func inspectRunDetails(ctx context.Context, c *cli.Context, factory lodelib.Factory[lodelib.RangeStore], runID string, run runreader.Run) (*reader.InspectRunResponse, error) {
	sel := selectorFrom(c)
	key := lode.Config{
		Dataset: c.String("storage-dataset"), Source: sel.Source,
		Category: sel.Category, Day: sel.Day, RunID: runID,
	}
	if run.Dataset != key.Dataset || run.RunID != key.RunID ||
		run.Source != key.Source || run.Category != key.Category || run.Day != key.Day ||
		run.Partition != lode.PartitionPrefix(key) {
		return nil, fmt.Errorf("start record identity does not match the selected storage key for run %q", runID)
	}

	details, err := lode.QueryRunDetails(ctx, factory, key)
	if err != nil {
		return nil, err
	}
	resp := &reader.InspectRunResponse{Run: run, Policy: details.Policy}
	for _, f := range details.SidecarFiles {
		resp.SidecarFiles = append(resp.SidecarFiles, reader.SidecarFileSummary{
			Filename: f.Filename, ContentType: f.ContentType, Size: f.Size, Path: f.Path,
		})
	}
	return resp, nil
}

func inspectProxyCommand() *cli.Command {
	return &cli.Command{
		Name:      "proxy",
		Usage:     "Inspect a proxy pool by name",
		ArgsUsage: "<pool-name>",
		Flags:     TUIReadOnlyFlags(),
		Action:    inspectProxyAction,
	}
}

func inspectProxyAction(c *cli.Context) error {
	if c.NArg() < 1 {
		return cli.Exit("pool-name required", 1)
	}
	poolName := c.Args().First()

	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	if c.Bool("tui") {
		return r.RenderTUI("inspect_proxy", reader.InspectProxy(poolName))
	}

	return r.Render(reader.InspectProxy(poolName))
}
