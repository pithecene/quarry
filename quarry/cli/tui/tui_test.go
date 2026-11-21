package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/pithecene/quarry/cli/reader"
	runreader "github.com/pithecene/quarry/reader"
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

// TestInspectRunView covers the inspect run TUI view, which shows the same
// data as the JSON, YAML, and table outputs (CONTRACT_CLI.md § TUI Mode).
func TestInspectRunView(t *testing.T) {
	t.Parallel()
	// The TUI shows every run field that the JSON, YAML, and table outputs show
	// (CONTRACT_CLI.md § TUI Mode: same data payloads).
	t.Run("every run field appears", func(t *testing.T) {
		str := func(s string) *string { return &s }
		count := int64(7)
		resp := &reader.InspectRunResponse{Run: runreader.Run{
			RunID: "run-a", Dataset: "quarry", Source: "demo", Category: "default", Day: "2026-10-22",
			Partition: "source=demo/category=default/day=2026-10-22/run_id=run-a/", DeadlineAt: str("2026-10-22T02:00:00Z"),
			Terminal: str("run_error"), EventCount: &count, Message: str("selector not found"), Backfilled: true,
		}}
		out := RenderInspectStatic("inspect_run", resp)
		for _, want := range []string{"quarry", "demo", "default", "2026-10-22", "run_id=run-a/", "2026-10-22T02:00:00Z", "run_error", "7", "selector not found", "true"} {
			if !strings.Contains(out, want) {
				t.Errorf("static inspect run lacks %q", want)
			}
		}
	})

	t.Run("the sidecar section scrolls into view", func(t *testing.T) {

		const path = "datasets/quarry/partitions/source=demo/category=default/day=2026-10-22/run_id=run-a/files/page.png"
		resp := &reader.InspectRunResponse{}
		if out := RenderInspectStatic("inspect_run", resp); strings.Contains(out, "Sidecar Files") {
			t.Fatalf("empty sidecar section in %q", out)
		}
		resp.SidecarFiles = []reader.SidecarFileSummary{
			{Filename: "page.png", ContentType: "image/png", Size: 3, Path: path},
			{Filename: "index.html", ContentType: "text/html", Size: 12, Path: "files/index.html"},
		}
		out := RenderInspectStatic("inspect_run", resp)
		for _, want := range []string{"Sidecar Files", "page.png (image/png, 3 bytes)", path, "index.html (text/html, 12 bytes)", "files/index.html"} {
			if !strings.Contains(out, want) {
				t.Errorf("static inspect run lacks %q", want)
			}
		}

		const width, height = 70, 16
		model, _ := NewInspectModel("inspect_run", resp).Update(tea.WindowSizeMsg{Width: width, Height: height})
		if out := model.View(); !strings.Contains(out, "Run Details") || strings.Contains(out, "index.html") {
			t.Fatalf("unexpected initial viewport: %q", out)
		}
		for range 2 {
			model, _ = model.Update(tea.KeyMsg{Type: tea.KeyPgDown})
		}
		out = model.View()
		if !strings.Contains(out, "page.png (image/png, 3 bytes)") || !strings.Contains(out, "index.html (text/html, 12 bytes)") {
			t.Errorf("scrolled viewport lacks sidecar entries: %q", out)
		}
		for range 6 {
			model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRight})
		}
		out = model.View()
		if !strings.Contains(out, "run_id=run-a/files/page.png") {
			t.Errorf("scrolled viewport lacks the path suffix: %q", out)
		}
		if w, h := lipgloss.Size(out); w > width || h > height {
			t.Errorf("viewport fits %dx%d terminal, got %dx%d", width, height, w, h)
		}
	})
}
