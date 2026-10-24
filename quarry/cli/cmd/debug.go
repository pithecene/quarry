package cmd

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/reader"
	"github.com/pithecene/quarry/cli/render"
	"github.com/pithecene/quarry/proxy"
	"github.com/pithecene/quarry/types"
)

// DebugCommand returns the debug command with subcommands.
// Debug commands are opt-in diagnostic tools per CONTRACT_CLI.md.
// They are read-only by default; any mutation must be explicitly requested.
func DebugCommand() *cli.Command {
	return &cli.Command{
		Name:  "debug",
		Usage: "Diagnostic tools (resolve proxy, ipc)",
		Subcommands: []*cli.Command{
			debugResolveCommand(),
			debugIPCCommand(),
		},
	}
}

func debugResolveCommand() *cli.Command {
	return &cli.Command{
		Name:  "resolve",
		Usage: "Resolve entities for debugging",
		Subcommands: []*cli.Command{
			debugResolveProxyCommand(),
		},
	}
}

func debugResolveProxyCommand() *cli.Command {
	return &cli.Command{
		Name:      "proxy",
		Usage:     "Resolve a proxy endpoint from a pool",
		ArgsUsage: "<pool>",
		Flags: append(ReadOnlyFlags(),
			&cli.BoolFlag{
				Name:  "commit",
				Usage: "Commit the resolution (advance rotation counters)",
			},
			&cli.StringFlag{
				Name:     "proxy-config",
				Usage:    "Path to proxy pools config file (JSON)",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "strategy",
				Usage: "Strategy override: round_robin, random, or sticky",
			},
			&cli.StringFlag{
				Name:  "sticky-key",
				Usage: "Sticky key for proxy selection",
			},
			&cli.StringFlag{
				Name:  "job-id",
				Usage: "Job ID for sticky scope derivation",
			},
		),
		Action: debugResolveProxyAction,
	}
}

func debugResolveProxyAction(c *cli.Context) error {
	if c.NArg() < 1 {
		return cli.Exit("pool name required", 1)
	}
	poolName := c.Args().First()
	commit := c.Bool("commit")
	configPath := c.String("proxy-config")

	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	// TUI not supported for debug commands
	if c.Bool("tui") {
		return cli.Exit("--tui is not supported for debug commands", 1)
	}

	// Load proxy pools and create selector
	selector, err := loadAndRegisterPools(configPath)
	if err != nil {
		return cli.Exit(fmt.Sprintf("proxy setup failed: %v", err), 1)
	}

	// Build selection request
	req := proxy.SelectRequest{
		Pool:      poolName,
		StickyKey: c.String("sticky-key"),
		JobID:     c.String("job-id"),
		Commit:    commit,
	}

	// Set strategy override if specified
	if strategy := c.String("strategy"); strategy != "" {
		s := types.ProxyStrategy(strategy)
		req.StrategyOverride = &s
	}

	// Select endpoint
	endpoint, err := selector.Select(req)
	if err != nil {
		return cli.Exit(fmt.Sprintf("proxy selection failed: %v", err), 1)
	}

	// Build response
	resp := &reader.ResolveProxyResponse{
		Endpoint: reader.ProxyEndpoint{
			Host:     endpoint.Host,
			Port:     endpoint.Port,
			Protocol: string(endpoint.Protocol),
		},
		Committed: commit,
	}
	if endpoint.Username != nil {
		resp.Endpoint.Username = endpoint.Username
	}

	return r.Render(resp)
}

func debugIPCCommand() *cli.Command {
	return &cli.Command{
		Name:  "ipc",
		Usage: "Show IPC debug information",
		Flags: append(ReadOnlyFlags(),
			&cli.BoolFlag{
				Name:  "verbose",
				Usage: "Include payload details",
			},
		),
		Action: debugIPCAction,
	}
}

func debugIPCAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}

	// TUI not supported for debug commands
	if c.Bool("tui") {
		return cli.Exit("--tui is not supported for debug commands", 1)
	}

	verbose := c.Bool("verbose")
	return r.Render(reader.DebugIPC(verbose))
}
