// Package cmd provides CLI commands for the quarry binary.
package cmd

import "github.com/urfave/cli/v2"

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
