package cmd

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/pithecene/quarry/internal/testutil"
	"github.com/pithecene/quarry/retention"
)

func runPrune(t *testing.T, root string, args ...string) (retention.Report, error) {
	t.Helper()
	out, err := runCLI(t, append([]string{"prune", "--format", "json", "--storage-backend", "fs", "--storage-path", root}, args...)...)
	var report retention.Report
	if out != "" {
		if jerr := json.Unmarshal([]byte(out), &report); jerr != nil {
			t.Fatalf("decode %q: %v", out, jerr)
		}
	}
	return report, err
}

func TestPrune_DryRunThenRemove(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	writeFSRun(t, root, "run-a")
	writeFSRun(t, root, "run-b") // commits last and holds the head

	dry, err := runPrune(t, root, "--succeeded-days", "0", "--dry-run")
	if err != nil || !dry.DryRun || dry.RunsPruned != 1 || dry.RunsKept != 1 {
		t.Fatalf("dry run = %+v, err = %v", dry, err)
	}
	listed, err := runListCLI(t, "runs", "--format", "json", "--storage-backend", "fs", "--storage-path", root)
	if err != nil || !strings.Contains(listed, `"run-a"`) {
		t.Fatalf("dry run removed a run: %v %s", err, listed)
	}

	got, err := runPrune(t, root, "--succeeded-days", "0")
	if err != nil || got.RunsPruned != 1 || got.Runs[0].RunID != "run-a" || got.Runs[1].Kept == "" {
		t.Fatalf("prune = %+v, err = %v", got, err)
	}
	listed, err = runListCLI(t, "runs", "--format", "json", "--storage-backend", "fs", "--storage-path", root)
	if err != nil || strings.Contains(listed, `"run-a"`) || !strings.Contains(listed, `"run-b"`) {
		t.Fatalf("list runs after prune: %v %s", err, listed)
	}
}

func TestPrune_UnsetAgeKeepsRuns(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	writeFSRun(t, root, "run-a")
	got, err := runPrune(t, root, "--failed-days", "0", "--dry-run")
	if err != nil || got.RunsScanned != 1 || got.NotDue != 1 || len(got.Runs) != 0 {
		t.Fatalf("prune = %+v, err = %v", got, err)
	}
}

func TestPrune_Errors(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	cases := map[string][]string{
		"no storage":             {"prune", "--succeeded-days", "1"},
		"no state":               {"prune", "--storage-backend", "fs", "--storage-path", root},
		"unterminated too young": {"prune", "--storage-backend", "fs", "--storage-path", root, "--unterminated-days", "0"},
		"run without day":        {"prune", "--storage-backend", "fs", "--storage-path", root, "--failed-days", "1", "--run-id", "r"},
		"empty run id":           {"prune", "--storage-backend", "fs", "--storage-path", root, "--failed-days", "1", "--source", "demo", "--category", "default", "--day", "2026-10-22", "--run-id", ""},
		"negative days":          {"prune", "--storage-backend", "fs", "--storage-path", root, "--succeeded-days", "-9223372036854775808"},
		"overflowing days":       {"prune", "--storage-backend", "fs", "--storage-path", root, "--failed-days", "9223372036854775807"},
		"empty source":           {"prune", "--storage-backend", "fs", "--storage-path", root, "--failed-days", "1", "--source", ""},
	}
	for name, args := range cases {
		if _, err := runCLI(t, args...); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	// An empty run ID is refused before anything is read, so the run remains.
	writeFSRun(t, root, "run-a")
	writeFSRun(t, root, "run-b")
	if _, err := runCLI(t, "prune", "--storage-backend", "fs", "--storage-path", root, "--succeeded-days", "0", "--source", "demo", "--category", "default", "--day", "2026-10-22", "--run-id", ""); err == nil || !strings.Contains(err.Error(), "--run-id must not be empty") {
		t.Fatalf("err = %v", err)
	}
	if listed, err := runListCLI(t, "runs", "--format", "json", "--storage-backend", "fs", "--storage-path", root); err != nil || !strings.Contains(listed, `"run-a"`) {
		t.Fatalf("run removed: %v %s", err, listed)
	}
}
