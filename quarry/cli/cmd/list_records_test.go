package cmd

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/urfave/cli/v2"

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
	app.Commands = []*cli.Command{ListCommand(), IndexCommand()}
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
	c, err := lode.NewLodeClient(cfg, root)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx := t.Context()
	if err := c.WriteStartRecord(ctx, &types.RunMeta{RunID: runID, Attempt: 1}, time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)); err != nil {
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

func TestListRuns_StorageFlagPairRequired(t *testing.T) {
	_, err := runListCLI(t, "runs", "--storage-backend", "fs")
	if err == nil || !strings.Contains(err.Error(), "both --storage-backend and --storage-path") {
		t.Fatalf("err = %v", err)
	}
}

func TestListEvents_FromStorage(t *testing.T) {
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

func TestListRuns_StorageOnlyFlagsRejectedOnStub(t *testing.T) {
	for _, flag := range []string{"--after", "--source", "--category", "--day"} {
		_, err := runListCLI(t, "runs", flag, "x")
		if err == nil || !strings.Contains(err.Error(), "requires --storage-backend") {
			t.Errorf("%s on the stub path: err = %v", flag, err)
		}
	}
}

func TestListRuns_StubRejectsStorageOnlyState(t *testing.T) {
	if _, err := runListCLI(t, "runs", "--state", "unterminated"); err == nil || !strings.Contains(err.Error(), "need storage") {
		t.Fatalf("err = %v", err)
	}
}

func TestListEvents_TableSummaryRows(t *testing.T) {
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

// An optional storage flag alone is an error, never stub data.
func TestListRuns_OptionalStorageFlagWithoutPairFails(t *testing.T) {
	for _, args := range [][]string{
		{"runs", "--storage-dataset", "other"},
		{"runs", "--storage-region", "us-east-1"},
		{"runs", "--storage-s3-path-style"},
	} {
		if _, err := runListCLI(t, args...); err == nil {
			t.Errorf("%v: want error, got stub output", args)
		}
	}
}

// YAML uses the documented field names, and omits what JSON omits.
func TestListRuns_YAMLFieldNames(t *testing.T) {
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
	got := eventSummaries([]map[string]any{
		{"seq": 1, "type": "item", "record_kind": "event"},
		{"seq": 2, "event_type": "artifact", "record_kind": "artifact_event"},
	})
	if got[0].Type != "item" || got[1].Type != "artifact" {
		t.Fatalf("types = %v, %v", got[0].Type, got[1].Type)
	}
}
