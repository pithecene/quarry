package tui

import (
	"strings"
	"testing"

	"github.com/pithecene/quarry/cli/reader"
)

func TestIsTUISupported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		viewType string
		want     bool
	}{
		// Supported: inspect commands
		{"inspect_run", true},
		{"inspect_proxy", true},

		// Supported: stats commands
		{"stats_runs", true},
		{"stats_proxies", true},
		{"stats_metrics", true},

		// Not supported: removed views
		{"inspect_job", false},
		{"inspect_task", false},
		{"inspect_executor", false},
		{"stats_jobs", false},
		{"stats_tasks", false},
		{"stats_executors", false},

		// Not supported: list commands
		{"list_runs", false},
		{"list_pools", false},

		// Not supported: debug commands
		{"debug_ipc", false},
		{"debug_resolve_proxy", false},

		// Not supported: version
		{"version", false},

		// Not supported: run
		{"run", false},

		// Not supported: unknown
		{"unknown", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.viewType, func(t *testing.T) {
			got := IsTUISupported(tt.viewType)
			if got != tt.want {
				t.Errorf("IsTUISupported(%q) = %v, want %v", tt.viewType, got, tt.want)
			}
		})
	}
}

func TestSupportedTUIViews(t *testing.T) {
	t.Parallel()

	views := SupportedTUIViews()

	// Should have exactly 5 supported views (2 inspect + 3 stats)
	if len(views) != 5 {
		t.Errorf("SupportedTUIViews() returned %d views, expected 5", len(views))
	}

	// All returned views should be supported
	for _, v := range views {
		if !IsTUISupported(v) {
			t.Errorf("SupportedTUIViews() returned %q but IsTUISupported returns false", v)
		}
	}
}

func TestRun_UnsupportedViewType(t *testing.T) {
	t.Parallel()

	err := Run("list_runs", nil)
	if err == nil {
		t.Error("Expected error for unsupported view type")
	}
}

func TestRenderStatsStatic_Metrics(t *testing.T) {
	t.Parallel()

	out := RenderStatsStatic("stats_metrics", &reader.MetricsSnapshot{RunID: "run-001", Executor: "executor.js", RunsStarted: 3})
	if strings.Contains(out, "Unknown view type") || strings.Contains(out, "Invalid data type") {
		t.Fatalf("stats_metrics did not render: %q", out)
	}
	for _, want := range []string{"Contract Metrics", "Runs:", "Storage:", "run-001", "executor.js"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered stats_metrics lacks %q", want)
		}
	}
}

func TestRenderStatsStatic_MetricsFitsTerminal(t *testing.T) {
	t.Parallel()

	view := NewStatsModel("stats_metrics", &reader.MetricsSnapshot{RunID: "run-001"}).View()
	if n := strings.Count(view, "\n") + 1; n > 24 {
		t.Errorf("stats_metrics view is %d lines, more than a 24-row terminal", n)
	}
}
