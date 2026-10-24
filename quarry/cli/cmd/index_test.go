package cmd

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/pithecene/quarry/internal/testutil"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/types"
)

// writeLegacyFSRun writes a finished run the way pre-record versions did:
// partitions only, no run records.
func writeLegacyFSRun(t *testing.T, root, runID string) {
	t.Helper()
	cfg := lode.Config{Dataset: "quarry", Source: "demo", Category: "default", Day: "2026-10-22", RunID: runID}
	c, err := lode.NewLodeClient(t.Context(), cfg, root)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	for i, typ := range []types.EventType{types.EventTypeItem, types.EventTypeRunComplete} {
		ev := &types.EventEnvelope{ContractVersion: types.ContractVersion, EventID: "e", RunID: runID, Seq: int64(i + 1), Type: typ, Ts: "2026-10-22T00:00:00Z", Payload: map[string]any{}, Attempt: 1}
		if err := c.WriteEvents(t.Context(), "quarry", runID, []*types.EventEnvelope{ev}); err != nil {
			t.Fatalf("events: %v", err)
		}
	}
}

// Stdout capture swaps os.Stdout: these tests stay serial.
func TestIndexRebuild_DryRunThenWrite(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	writeLegacyFSRun(t, root, "legacy-1")
	writeFSRun(t, root, "native-1")
	storage := []string{"--format", "json", "--storage-backend", "fs", "--storage-path", root}

	rebuild := func(extra ...string) lode.RebuildReport {
		t.Helper()
		out, err := runCLI(t, append(append([]string{"index", "rebuild"}, storage...), extra...)...)
		if err != nil {
			t.Fatalf("index rebuild %v: %v", extra, err)
		}
		var report lode.RebuildReport
		if err := json.Unmarshal([]byte(out), &report); err != nil {
			t.Fatalf("decode %q: %v", out, err)
		}
		return report
	}

	dry := rebuild("--dry-run")
	if !dry.DryRun || dry.RunsScanned != 2 || dry.AlreadyIndexed != 1 || dry.StartsWritten != 1 || dry.EndsWritten != 1 {
		t.Fatalf("dry run = %+v", dry)
	}
	out, err := runListCLI(t, append([]string{"runs", "--state", "succeeded", "--source", "demo"}, storage...)...)
	if err != nil || strings.Contains(out, "legacy-1") {
		t.Fatalf("dry run indexed the run: %v %s", err, out)
	}

	if got := rebuild(); got.StartsWritten != 1 || got.EndsWritten != 1 {
		t.Fatalf("rebuild = %+v", got)
	}
	out, err = runListCLI(t, append([]string{"runs", "--state", "succeeded", "--source", "demo"}, storage...)...)
	if err != nil || !strings.Contains(out, `"legacy-1"`) || !strings.Contains(out, `"backfilled": true`) {
		t.Fatalf("list runs after rebuild: %v %s", err, out)
	}
	if again := rebuild(); again.AlreadyIndexed != 2 || len(again.Runs) != 0 {
		t.Fatalf("rerun = %+v", again)
	}
}

func TestIndexRebuild_RequiresStorage(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	if _, err := runCLI(t, "index", "rebuild"); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("err = %v, want storage required", err)
	}
}

// YAML uses the documented field names.
func TestIndexRebuild_YAMLFieldNames(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	writeLegacyFSRun(t, root, "legacy-1")
	out, err := runCLI(t, "index", "rebuild", "--dry-run", "--format", "yaml", "--storage-backend", "fs", "--storage-path", root)
	if err != nil {
		t.Fatalf("index rebuild: %v", err)
	}
	for _, want := range []string{"runs_scanned: 1", "starts_written: 1", "run_id: legacy-1", "wrote_start: true"} {
		if !strings.Contains(out, want) {
			t.Errorf("yaml lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "no_end:") || strings.Contains(out, "skipped: \"\"") {
		t.Errorf("yaml keeps omitted fields:\n%s", out)
	}
}
