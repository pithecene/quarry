package cmd

import (
	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/reader"
	"github.com/pithecene/quarry/cli/render"
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
	return &cli.Command{
		Name:      "run",
		Usage:     "Inspect a run by ID",
		ArgsUsage: "<run-id>",
		Flags:     TUIReadOnlyFlags(),
		Action:    inspectRunAction,
	}
}

func inspectRunAction(c *cli.Context) error {
	if c.NArg() < 1 {
		return cli.Exit("run-id required", 1)
	}
	runID := c.Args().First()

	// Get renderer
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	// Handle TUI mode
	if c.Bool("tui") {
		return r.RenderTUI("inspect_run", reader.InspectRun(runID))
	}

	// Standard render
	resp := reader.InspectRun(runID)
	return r.Render(resp)
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
