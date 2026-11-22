package cmd

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	lodelib "github.com/pithecene/lode/lode"
	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/internal/testutil"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/types"
)

// runListCLI runs `quarry list ...` and returns stdout.
func runListCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	return runCLI(t, append([]string{"list"}, args...)...)
}

// runCLI runs a quarry command and captures its stdout.
func runCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	app := cli.NewApp()
	app.Commands = []*cli.Command{ListCommand(), ReadCommand(), IndexCommand(), PruneCommand(), InspectCommand(), StatsCommand(), DebugCommand()}
	app.ExitErrHandler = func(*cli.Context, error) {}

	orig := os.Stdout
	rd, wr, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = wr
	runErr := app.Run(append([]string{"quarry"}, args...))
	_ = wr.Close()
	os.Stdout = orig
	out, _ := io.ReadAll(rd)
	return string(out), runErr
}

// writeFSRun writes one finished run with two events to an fs dataset.
func writeFSRun(t *testing.T, root, runID string) {
	t.Helper()
	cfg := lode.Config{Dataset: "quarry", Source: "demo", Category: "default", Day: "2026-10-22", RunID: runID}
	c, err := lode.NewLodeClient(t.Context(), cfg, root)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx := t.Context()
	if err := c.WriteStartRecord(ctx, &types.RunMeta{RunID: runID, Attempt: 1}, time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC), nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	for i, typ := range []types.EventType{types.EventTypeItem, types.EventTypeRunComplete} {
		ev := &types.EventEnvelope{ContractVersion: types.ContractVersion, EventID: "e", RunID: runID, Seq: int64(i + 1), Type: typ, Ts: "2026-10-22T00:00:00Z", Payload: map[string]any{}, Attempt: 1}
		if err := c.WriteEvents(ctx, "quarry", runID, []*types.EventEnvelope{ev}); err != nil {
			t.Fatalf("events: %v", err)
		}
	}
	if err := c.WriteEndRecord(ctx, lode.RunEnd{EndedAt: time.Now(), Outcome: types.OutcomeSuccess, Terminal: types.EventTypeRunComplete, EventCount: 2}); err != nil {
		t.Fatalf("end: %v", err)
	}
}

func TestListRuns_FromRecords(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	writeFSRun(t, root, "run-a")
	writeFSRun(t, root, "run-b")

	out, err := runListCLI(t, "runs", "--format", "json", "--storage-backend", "fs", "--storage-path", root, "--source", "demo", "--limit", "1")
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	var page struct {
		Runs []struct {
			RunID string `json:"run_id"`
			State string `json:"state"`
		} `json:"runs"`
		Next string `json:"next"`
	}
	if err := json.Unmarshal([]byte(out), &page); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if len(page.Runs) != 1 || page.Runs[0].RunID != "run-a" || page.Runs[0].State != "succeeded" || page.Next == "" {
		t.Fatalf("page = %+v", page)
	}

	out, err = runListCLI(t, "runs", "--format", "json", "--storage-backend", "fs", "--storage-path", root, "--source", "demo", "--after", page.Next)
	if err != nil {
		t.Fatalf("list runs page 2: %v", err)
	}
	if !strings.Contains(out, `"run-b"`) || strings.Contains(out, `"run-a"`) {
		t.Fatalf("page 2 = %s", out)
	}
}

func TestListEvents_FromStorage(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	writeFSRun(t, root, "run-a")
	out, err := runListCLI(t, "events", "--format", "json", "--storage-backend", "fs", "--storage-path", root,
		"--source", "demo", "--category", "default", "--day", "2026-10-22", "--run-id", "run-a")
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	var resp struct {
		Run struct {
			RunID string `json:"run_id"`
		} `json:"run"`
		Events []map[string]any `json:"events"`
	}
	if err := json.Unmarshal([]byte(out), &resp); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if resp.Run.RunID != "run-a" || len(resp.Events) != 2 || resp.Events[0]["type"] != "item" || resp.Events[1]["type"] != "run_complete" {
		t.Fatalf("resp = %+v", resp)
	}

	_, err = runListCLI(t, "events", "--storage-backend", "fs", "--storage-path", root,
		"--source", "demo", "--category", "default", "--day", "2026-10-22", "--run-id", "missing")
	if err == nil || !strings.Contains(err.Error(), "run not found") {
		t.Fatalf("missing run err = %v", err)
	}
}

func TestListEvents_ReadTimeoutBoundsTheRead(t *testing.T) {
	t.Parallel()
	set := flag.NewFlagSet("list events", flag.ContinueOnError)
	for _, f := range []cli.Flag{readTimeoutFlag()} {
		if err := f.Apply(set); err != nil {
			t.Fatal(err)
		}
	}
	if err := set.Parse([]string{"--read-timeout", "42s"}); err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	ctx, cancel := readContext(cli.NewContext(nil, set, nil))
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("readContext set no deadline")
	}
	if got := deadline.Sub(before); got < 41*time.Second || got > 43*time.Second {
		t.Fatalf("deadline %v after the call, want about 42s", got)
	}

	unset := flag.NewFlagSet("list events", flag.ContinueOnError)
	if err := readTimeoutFlag().Apply(unset); err != nil {
		t.Fatal(err)
	}
	if err := unset.Parse(nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = readContext(cli.NewContext(nil, unset, nil))
	defer cancel()
	deadline, _ = ctx.Deadline()
	if got := deadline.Sub(before); got < defaultReadTimeout-time.Second || got > defaultReadTimeout+time.Second {
		t.Fatalf("default deadline %v after the call, want about %v", got, defaultReadTimeout)
	}
}

func TestListEvents_ReadTimeoutRejectsANonDuration(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	_, err := runListCLI(t, "events", "--storage-backend", "fs", "--storage-path", root,
		"--source", "demo", "--category", "default", "--day", "2026-10-22", "--run-id", "run-a",
		"--read-timeout", "soon")
	if err == nil || !strings.Contains(err.Error(), "read-timeout") {
		t.Fatalf("a read timeout that is not a duration should be refused, got %v", err)
	}
}

func TestListEvents_TableSummaryRows(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	writeFSRun(t, root, "run-a")
	out, err := runListCLI(t, "events", "--format", "table", "--no-color", "--storage-backend", "fs", "--storage-path", root,
		"--source", "demo", "--category", "default", "--day", "2026-10-22", "--run-id", "run-a")
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for _, want := range []string{"seq", "record_kind", "event_id", "run_complete"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output lacks %q:\n%s", want, out)
		}
	}
}

// YAML uses the documented field names, and omits what JSON omits.
func TestListRuns_YAMLFieldNames(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	writeFSRun(t, root, "run-a")
	out, err := runListCLI(t, "runs", "--format", "yaml", "--storage-backend", "fs", "--storage-path", root, "--source", "demo")
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	for _, want := range []string{"run_id: run-a", "event_count: 2", "started_at:"} {
		if !strings.Contains(out, want) {
			t.Errorf("yaml lacks %q:\n%s", want, out)
		}
	}
	for _, unwanted := range []string{"runid", "next:", "job_id"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("yaml has %q:\n%s", unwanted, out)
		}
	}
}

func TestEventSummaries_ArtifactCommitType(t *testing.T) {
	t.Parallel()

	got := eventSummaries([]map[string]any{
		{"seq": 1, "type": "item", "record_kind": "event"},
		{"seq": 2, "event_type": "artifact", "record_kind": "artifact_event"},
	})
	if got[0].Type != "item" || got[1].Type != "artifact" {
		t.Fatalf("types = %v, %v", got[0].Type, got[1].Type)
	}
}

// Every command that reads or writes storage needs --storage-backend and
// --storage-path together, and without them fails before writing anything:
// the read commands never return invented data.
func TestStorageCommands_StorageFlags(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	const missing, unpaired = "--storage-backend and --storage-path are required", "both --storage-backend and --storage-path"
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"list runs without storage", []string{"list", "runs"}, missing},
		{"list runs with a selector only", []string{"list", "runs", "--source", "x"}, missing},
		{"stats metrics without storage", []string{"stats", "metrics"}, missing},
		{"list events without storage", []string{"list", "events", "--run-id", "r"}, missing},
		{"list artifacts without storage", []string{"list", "artifacts", "--run-id", "r"}, missing},
		{"read artifact without storage", []string{"read", "artifact", "--run-id", "r", "--artifact-id", "a"}, missing},
		{"inspect run without storage", []string{"inspect", "run", "r"}, missing},
		{"index rebuild without storage", []string{"index", "rebuild"}, "index rebuild writes storage: --storage-backend and --storage-path are required"},
		{"prune without storage", []string{"prune", "--succeeded-days", "1"}, "prune writes storage: --storage-backend and --storage-path are required"},
		{"list events names its read", []string{"list", "events", "--run-id", "r"}, "list events reads storage: --storage-backend and --storage-path are required"},
		{"list runs with the backend alone", []string{"list", "runs", "--storage-backend", "fs"}, unpaired},
		{"stats metrics with the backend alone", []string{"stats", "metrics", "--storage-backend", "fs"}, unpaired},
		{"list runs with the dataset alone", []string{"list", "runs", "--storage-dataset", "other"}, unpaired},
		{"list runs with the region alone", []string{"list", "runs", "--storage-region", "us-east-1"}, unpaired},
		{"list runs with path style alone", []string{"list", "runs", "--storage-s3-path-style"}, unpaired},
		{"index rebuild with the backend alone", []string{"index", "rebuild", "--storage-backend", "fs"}, "index rebuild writes storage: both --storage-backend and --storage-path are required"},
		{"prune with the backend alone", []string{"prune", "--succeeded-days", "1", "--storage-backend", "fs"}, "prune writes storage: both --storage-backend and --storage-path are required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := runCLI(t, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want one that contains %q", err, tc.want)
			}
			if out != "" {
				t.Errorf("wrote output without storage: %q", out)
			}
		})
	}
}

// sharedStore builds the store once and returns that store, or the error of
// that one build, on every call.
func TestSharedStore_BuildsOnce(t *testing.T) {
	t.Parallel()
	buildErr := errors.New("no bucket")
	cases := []struct {
		name string
		err  error
	}{
		{"a store", nil},
		{"an error", buildErr},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builds := 0
			store := lodelib.NewMemory()
			f := sharedStore(func() (lodelib.RangeStore, error) {
				builds++
				return store, tc.err
			})
			for range 3 {
				got, err := f()
				if !errors.Is(err, tc.err) || got != store {
					t.Fatalf("got %v, %v; want the one build's store and error", got, err)
				}
			}
			if builds != 1 {
				t.Errorf("factory built %d stores, want 1", builds)
			}
		})
	}
}
