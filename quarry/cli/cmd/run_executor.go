package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/executor"
	"github.com/pithecene/quarry/runtime"
)

// runDryRun validates script loadability via the executor's --validate mode.
// It prints a human-readable summary to stderr and exits 0 (valid) or 1 (invalid).
func runDryRun(ctx context.Context, executorPath, scriptPath, resolveFrom string) error {
	fmt.Fprintf(os.Stderr, "Dry-run validation:\n")
	fmt.Fprintf(os.Stderr, "  script:   %s\n", scriptPath)
	fmt.Fprintf(os.Stderr, "  executor: %s\n", executorPath)
	if resolveFrom != "" {
		fmt.Fprintf(os.Stderr, "  resolve-from: %s\n", resolveFrom)
	}
	fmt.Fprintln(os.Stderr)

	result, err := runtime.ValidateScript(ctx, executorPath, scriptPath, resolveFrom)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ Executor failed: %v\n\nValidation failed.\n", err)
		return cli.Exit("", exitExecutorCrash)
	}

	if !result.Valid {
		fmt.Fprintf(os.Stderr, "  ✗ Script load failed: %s\n\nValidation failed.\n", result.Error)
		return cli.Exit("", exitScriptError)
	}

	fmt.Fprintf(os.Stderr, "  ✓ Script loaded successfully\n")
	if result.Exports != nil {
		fmt.Fprintf(os.Stderr, "  ✓ Default export: function\n")
		if len(result.Exports.Hooks) > 0 {
			fmt.Fprintf(os.Stderr, "  ✓ Hooks: %s\n", strings.Join(result.Exports.Hooks, ", "))
		}
	}
	fmt.Fprintf(os.Stderr, "\nValidation passed.\n")

	return cli.Exit("", exitSuccess)
}

// resolveExecutor finds the executor binary path.
// Resolution order:
//  1. Explicit --executor flag (if provided)
//  2. Embedded executor (run from memory)
//  3. Bundled path relative to quarry binary (development layout)
//  4. "quarry-executor" in PATH
func resolveExecutor(explicit string) (string, error) {
	// 1. Explicit override takes priority
	if explicit != "" {
		if _, err := os.Stat(explicit); err != nil {
			return "", fmt.Errorf("executor not found at %s (check the path and try again)", explicit)
		}
		// The prefix keeps a bare name out of PATH lookup, and a file named as
		// the embedded executor distinct from it. Cleaning the path instead
		// could change which file a symlink in it selects.
		if explicit == executor.Embedded || filepath.Base(explicit) == explicit {
			return "." + string(filepath.Separator) + explicit, nil
		}
		return explicit, nil
	}

	// 2. Try embedded executor (primary method for distributed binary)
	if executor.IsEmbedded() {
		return executor.Embedded, nil
	}

	// 3. Try bundled path relative to this binary (development layout)
	execPath, err := os.Executable()
	if err == nil {
		execDir := filepath.Dir(execPath)
		// Try common bundled locations
		bundledPaths := []string{
			filepath.Join(execDir, "..", "executor-node", "dist", "bin", "executor.js"),
			filepath.Join(execDir, "executor-node", "dist", "bin", "executor.js"),
			filepath.Join(execDir, "executor.js"),
		}
		for _, p := range bundledPaths {
			absPath, err := filepath.Abs(p)
			if err != nil {
				continue
			}
			if _, err := os.Stat(absPath); err == nil {
				return absPath, nil
			}
		}
	}

	// 4. Try PATH lookup
	if path, err := exec.LookPath("quarry-executor"); err == nil {
		return path, nil
	}

	// Not found - provide actionable error
	return "", fmt.Errorf(`executor not found

Quarry could not locate the executor. To fix:

  1. Build the executor:
     pnpm -C executor-node run build

  2. Or specify the path manually:
     --executor ./executor-node/dist/bin/executor.js

  3. Or add quarry-executor to your PATH`)
}
