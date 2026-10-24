// Package main provides the quarry CLI entrypoint.
//
// The CLI is the only execution entrypoint per CONTRACT_CLI.md.
// All commands except `run` are read-only.
//
// Usage:
//
//	quarry <command> [subcommand] [options]
//
// Exit codes for `run` per CONTRACT_RUN.md:
//   - 0: success (run_complete)
//   - 1: script error (run_error)
//   - 2: executor crash
//   - 3: policy failure
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/cmd"
	"github.com/pithecene/quarry/types"
)

// Commit is set via ldflags at build time.
var commit = "unknown"

func main() {
	app := &cli.App{
		Name:           "quarry",
		Usage:          "Quarry extraction runtime CLI",
		Version:        fmt.Sprintf("%s (commit: %s)", types.Version, commit),
		ExitErrHandler: exitErrHandler,
		Commands: []*cli.Command{
			cmd.RunCommand(),
			cmd.InspectCommand(),
			cmd.StatsCommand(),
			cmd.ListCommand(),
			cmd.IndexCommand(),
			cmd.PruneCommand(),
			cmd.DebugCommand(),
			cmd.VersionCommand("", commit),
		},
	}

	if err := app.Run(os.Args); err != nil {
		// ExitErrHandler already handled the exit for cli.ExitCoder errors.
		// This branch handles unexpected errors that weren't wrapped.
		os.Exit(1)
	}
}

// exitErrHandler handles errors from the CLI, preserving exit codes from cli.Exit().
// This ensures that `run` command exit codes per CONTRACT_RUN.md are propagated.
func exitErrHandler(_ *cli.Context, err error) {
	if err == nil {
		return
	}

	// Check for ExitCoder (from cli.Exit), handles wrapped errors
	if exitCoder, ok := errors.AsType[cli.ExitCoder](err); ok {
		code := exitCoder.ExitCode()
		msg := exitCoder.Error()

		// Only print if there's a real message (not just "exit status N")
		// cli.Exit("", N).Error() returns "exit status N", so skip those
		if msg != "" && msg != fmt.Sprintf("exit status %d", code) {
			fmt.Fprintln(os.Stderr, msg)
		}
		os.Exit(code)
	}

	// Unexpected error - print and exit with code 1
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}
