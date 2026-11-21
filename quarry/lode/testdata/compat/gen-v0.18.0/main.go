// Command gen-v0.18.0 writes the dataset under ../v0.18.0 with the storage
// code of Quarry v0.18.0 (Lode v0.17.1, format 1.1.0), so that later releases
// are tested against data the last release on Lode v0.17.1 wrote: run
// records, commit index entries, artifact Volumes, sidecar files, and
// metrics. Every input is fixed. Snapshot IDs and manifest creation times
// vary between generations.
//
// The program builds against a copy of the v0.18.0 tag. From an empty
// directory holding a copy of this file, with <tree> a directory into which
// `git archive v0.18.0 quarry | tar -x -C <tree>` extracted the module:
//
//	go mod init gen
//	go mod edit -require=github.com/pithecene/quarry@v0.0.0 -replace=github.com/pithecene/quarry=<tree>/quarry
//	go mod tidy
//	go run . <empty directory>
//
// The result replaces the fixture's contents.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

// run is one run as Quarry v0.18.0 stores it. A run without a terminal event
// stands for one whose process was killed: it has a start record and no end
// record.
type run struct {
	cfg      lode.Config
	jobID    string
	parent   string
	attempt  int
	items    int
	artifact bool
	sidecar  bool
	terminal types.EventType
	outcome  types.OutcomeStatus
}

var runs = []run{
	{cfg: cfg("shop", "products", "2026-11-01", "run-ok-1"), jobID: "job-1", attempt: 1, items: 3, artifact: true, sidecar: true, terminal: types.EventTypeRunComplete, outcome: types.OutcomeSuccess},
	{cfg: cfg("shop", "products", "2026-11-01", "run-fail-1"), attempt: 1, items: 1, terminal: types.EventTypeRunError, outcome: types.OutcomeScriptError},
	{cfg: cfg("shop", "products", "2026-11-01", "run-retry-1"), jobID: "job-1", parent: "run-fail-1", attempt: 2, items: 2, terminal: types.EventTypeRunComplete, outcome: types.OutcomeSuccess},
	{cfg: cfg("news", "default", "2026-11-02", "run-killed-1"), attempt: 1, items: 1},
}

func cfg(source, category, day, runID string) lode.Config {
	return lode.Config{Dataset: "quarry", Source: source, Category: category, Day: day, RunID: runID, Policy: "strict"}
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: gen-v0.18.0 <empty directory>")
	}
	for i, r := range runs {
		if err := write(context.Background(), os.Args[1], i, r); err != nil {
			log.Fatalf("%s: %v", r.cfg.RunID, err)
		}
	}
}

func write(ctx context.Context, root string, i int, r run) error {
	c, err := lode.NewLodeClient(ctx, r.cfg, root)
	if err != nil {
		return err
	}
	start, err := time.Parse(time.DateOnly, r.cfg.Day)
	if err != nil {
		return err
	}
	start = start.Add(time.Duration(i+1) * time.Hour)
	meta := &types.RunMeta{RunID: r.cfg.RunID, Attempt: r.attempt}
	if r.jobID != "" {
		meta.JobID = &r.jobID
	}
	if r.parent != "" {
		meta.ParentRunID = &r.parent
	}
	if err := c.WriteStartRecord(ctx, meta, start, nil); err != nil {
		return err
	}
	var seq int64
	env := func(t types.EventType, payload map[string]any) *types.EventEnvelope {
		seq++
		return &types.EventEnvelope{
			ContractVersion: types.ContractVersion, EventID: fmt.Sprintf("%s-e%d", r.cfg.RunID, seq), RunID: r.cfg.RunID,
			Seq: seq, Type: t, Ts: start.Add(time.Duration(seq) * time.Second).Format(time.RFC3339), Payload: payload,
			JobID: meta.JobID, ParentRunID: meta.ParentRunID, Attempt: r.attempt,
		}
	}
	// Items are written two to a commit, as a buffered policy flushes them.
	var batch []*types.EventEnvelope
	for n := range r.items {
		batch = append(batch, env(types.EventTypeItem, map[string]any{"item_type": "product", "n": n, "title": fmt.Sprintf("%s item %d", r.cfg.RunID, n)}))
		if len(batch) == 2 || n == r.items-1 {
			if err := c.WriteEvents(ctx, r.cfg.Dataset, r.cfg.RunID, batch); err != nil {
				return err
			}
			batch = nil
		}
	}
	if r.sidecar {
		if err := c.PutFile(ctx, "page.png", "image/png", []byte("png bytes of "+r.cfg.RunID)); err != nil {
			return err
		}
	}
	if r.artifact {
		if err := writeArtifact(ctx, c, r, env); err != nil {
			return err
		}
	}
	if r.terminal == "" {
		return nil
	}
	if err := c.WriteEvents(ctx, r.cfg.Dataset, r.cfg.RunID, []*types.EventEnvelope{env(r.terminal, map[string]any{"summary": "done"})}); err != nil {
		return err
	}
	ended := start.Add(time.Duration(seq+1) * time.Second)
	snap := metrics.Snapshot{RunsStarted: 1, EventsReceived: seq, EventsPersisted: seq, RunID: r.cfg.RunID, JobID: r.jobID, Policy: "strict", Executor: "gen", StorageBackend: "fs"}
	if err := c.WriteMetrics(ctx, snap, ended); err != nil {
		return err
	}
	return c.WriteEndRecord(ctx, lode.RunEnd{EndedAt: ended, Outcome: r.outcome, Terminal: r.terminal, EventCount: seq, MetricsPersisted: true})
}

// writeArtifact stores one artifact in two ranges and commits it, as the
// runtime does: the commit event carries the storage that Finish returns,
// and WriteEvents writes the artifact's commit index entry.
func writeArtifact(ctx context.Context, c *lode.LodeClient, r run, env func(types.EventType, map[string]any) *types.EventEnvelope) error {
	data := []byte("report of " + r.cfg.RunID)
	w, err := c.OpenArtifact(ctx, "art-1", int64(len(data)), "")
	if err != nil {
		return err
	}
	half := int64(len(data) / 2)
	if err := w.WriteRange(ctx, 0, data[:half]); err != nil {
		return err
	}
	if err := w.WriteRange(ctx, half, data[half:]); err != nil {
		return err
	}
	storage, err := w.Finish(ctx)
	if err != nil {
		return err
	}
	commit := env(types.EventTypeArtifact, map[string]any{"artifact_id": "art-1", "name": "report.txt", "content_type": "text/plain", "size_bytes": float64(len(data))})
	commit.Storage = &storage
	return c.WriteEvents(ctx, r.cfg.Dataset, r.cfg.RunID, []*types.EventEnvelope{commit})
}
