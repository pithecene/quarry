package tui

import (
	"fmt"
	"strings"
)

// Run starts the appropriate TUI based on the view type.
// Returns an error if the view type doesn't support TUI.
func Run(viewType string, data any) error {
	// Validate TUI is only used for supported commands
	if !IsTUISupported(viewType) {
		return fmt.Errorf("TUI mode is not supported for %s", viewType)
	}

	// Route to appropriate TUI
	if strings.HasPrefix(viewType, "inspect_") {
		return RunInspectTUI(viewType, data)
	}
	if strings.HasPrefix(viewType, "stats_") {
		return RunStatsTUI(viewType, data)
	}

	return fmt.Errorf("unknown view type: %s", viewType)
}

// IsTUISupported returns true if the view type supports TUI mode.
// Per CONTRACT_CLI.md, only inspect and stats commands support TUI.
func IsTUISupported(viewType string) bool {
	supportedPrefixes := []string{
		"inspect_",
		"stats_",
	}

	for _, prefix := range supportedPrefixes {
		if strings.HasPrefix(viewType, prefix) {
			return true
		}
	}

	return false
}

// SupportedTUIViews returns a list of view types that support TUI.
func SupportedTUIViews() []string {
	return []string{
		"inspect_run",
		"inspect_job",
		"inspect_task",
		"inspect_proxy",
		"inspect_executor",
		"stats_runs",
		"stats_jobs",
		"stats_tasks",
		"stats_proxies",
		"stats_executors",
		"stats_metrics",
	}
}
