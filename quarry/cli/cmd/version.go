package cmd

import (
	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/render"
	"github.com/pithecene/quarry/types"
)

// VersionResponse is the response for the version command.
// Reports the canonical project version (lockstep across all components).
type VersionResponse struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
}

// VersionCommand returns the version command.
// Version reports the canonical project version per CONTRACT_CLI.md.
// All components share a single version (lockstep versioning).
// It must not contact the executor.
func VersionCommand(_, commit string) *cli.Command {
	return &cli.Command{
		Name:   "version",
		Usage:  "Show version information",
		Flags:  ReadOnlyFlags(),
		Action: versionAction(commit),
	}
}

func versionAction(commit string) cli.ActionFunc {
	return func(c *cli.Context) error {
		r, err := render.NewRenderer(c)
		if err != nil {
			return err
		}

		// TUI not supported for version command
		if c.Bool("tui") {
			return cli.Exit("--tui is not supported for version command", 1)
		}

		resp := VersionResponse{
			Version: types.Version,
			Commit:  commit,
		}

		return r.Render(resp)
	}
}
