package cmd

import (
	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/render"
)

// ListCommand returns the list command with subcommands.
// List returns thin slices (not inspect-level detail) per CONTRACT_CLI.md.
func ListCommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "List entities (runs, events, artifacts)",
		Subcommands: []*cli.Command{
			listRunsCommand(),
			listEventsCommand(),
			listArtifactsCommand(),
		},
	}
}

func listRunsCommand() *cli.Command {
	return &cli.Command{
		Name:  "runs",
		Usage: "List runs",
		Flags: append(append(append(ReadOnlyFlags(), storageReadFlags()...), runSelectorFlags()...),
			readTimeoutFlag(),
			&cli.StringFlag{
				Name:  "state",
				Usage: "Filter by state: succeeded, failed, unterminated",
			},
			&cli.IntFlag{
				Name:  "limit",
				Usage: "Maximum number of runs to return (0 = no limit)",
				Value: 0,
			},
			&cli.StringFlag{
				Name:  "after",
				Usage: "Resume after the cursor a previous page returned",
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

	if err := requireStorage(c, "list runs", "reads"); err != nil {
		return err
	}
	return listRunsFromRecords(c, r)
}
