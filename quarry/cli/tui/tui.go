package tui

import (
	"fmt"
	"slices"
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
// Per CONTRACT_CLI.md, only the listed inspect and stats commands support TUI.
func IsTUISupported(viewType string) bool {
	return slices.Contains(SupportedTUIViews(), viewType)
}

// SupportedTUIViews returns a list of view types that support TUI.
func SupportedTUIViews() []string {
	return []string{
		"inspect_run",
		"inspect_proxy",
		"stats_runs",
		"stats_proxies",
		"stats_metrics",
	}
}
