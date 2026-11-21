// Package cmd provides CLI commands for the quarry binary.
package cmd

import "github.com/urfave/cli/v2"

// Commands returns the quarry binary's top-level commands, in help order. The
// binary registers exactly these, and the parity test checks the artifact
// against them, so a command cannot leave one without the other.
func Commands(commit string) []*cli.Command {
	return []*cli.Command{
		RunCommand(),
		InitCommand(),
		InspectCommand(),
		StatsCommand(),
		ListCommand(),
		ReadCommand(),
		IndexCommand(),
		PruneCommand(),
		DebugCommand(),
		VersionCommand("", commit),
	}
}

// Shared flags for read-only commands per CONTRACT_CLI.md.
var (
	// FormatFlag selects output format: json, table, yaml.
	FormatFlag = &cli.StringFlag{
		Name:    "format",
		Aliases: []string{"f"},
		Usage:   "Output format: json, table, yaml",
	}

	// NoColorFlag disables colored output in table format.
	// Note: Currently a placeholder - table output doesn't emit ANSI colors yet.
	// The flag is defined for forward compatibility when table coloring is added.
	NoColorFlag = &cli.BoolFlag{
		Name:  "no-color",
		Usage: "Disable colored output (table format only)",
	}
)

// ReadOnlyFlags returns the shared output flags of the read-only commands.
func ReadOnlyFlags() []cli.Flag {
	return []cli.Flag{
		FormatFlag,
		NoColorFlag,
	}
}
