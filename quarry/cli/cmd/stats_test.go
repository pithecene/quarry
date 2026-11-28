package cmd

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	lodelib "github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/internal/legacydata"
	"github.com/pithecene/quarry/internal/testutil"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
)

// stats metrics reads the latest metrics record from storage, filtered by
// --run-id.
func TestStatsMetrics_ReadsStorage(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	for _, runID := range []string{"run-a", "run-b"} {
		cfg := lode.Config{Dataset: "quarry", Source: "demo", Category: "default", Day: "2026-10-22", RunID: runID}
		snap := metrics.Snapshot{RunID: runID, RunsStarted: 1, Policy: "strict", Executor: "executor.mjs", StorageBackend: "fs"}
		if err := legacydata.WriteMetrics(t.Context(), lodelib.FS(root), cfg, snap, time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)); err != nil {
			t.Fatalf("metrics: %v", err)
		}
	}
	out, err := runCLI(t, "stats", "metrics", "--format", "json", "--storage-backend", "fs", "--storage-path", root, "--run-id", "run-a")
	if err != nil {
		t.Fatalf("stats metrics: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if got["run_id"] != "run-a" || got["policy"] != "strict" || got["runs_started_total"] != float64(1) {
		t.Errorf("snapshot = %v, want run-a's strict metrics", got)
	}
	out, err = runCLI(t, "stats", "metrics", "--format", "yaml", "--storage-backend", "fs", "--storage-path", root, "--run-id", "run-a")
	if err != nil || !strings.Contains(out, "runs_started_total: 1\n") || strings.Contains(out, "job_id") || strings.Contains(out, "dropped_by_type") {
		t.Errorf("YAML = %s, %v, want the documented names without empty fields", out, err)
	}
}

// stats metrics bounds its whole read by --read-timeout, as the listing
// commands do: a dataset too large for the default needs a longer bound.
func TestStatsMetrics_ReadTimeout(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	cfg := lode.Config{Dataset: "quarry", Source: "demo", Category: "default", Day: "2026-10-22", RunID: "run-a"}
	snap := metrics.Snapshot{RunID: "run-a", RunsStarted: 1, Policy: "strict", Executor: "executor.mjs", StorageBackend: "fs"}
	if err := legacydata.WriteMetrics(t.Context(), lodelib.FS(root), cfg, snap, time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("metrics: %v", err)
	}
	cases := []struct {
		name    string
		timeout string
		wantErr string
	}{
		{name: "a bound the read fits in succeeds", timeout: "1m"},
		{name: "an expired bound fails the read", timeout: "1ns", wantErr: "deadline exceeded"},
		{name: "a value that is not a duration is refused", timeout: "soon", wantErr: "read-timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runCLI(t, "stats", "metrics", "--format", "json", "--storage-backend", "fs", "--storage-path", root, "--read-timeout", tc.timeout)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("stats metrics: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want one that names %q", err, tc.wantErr)
			}
		})
	}
}
