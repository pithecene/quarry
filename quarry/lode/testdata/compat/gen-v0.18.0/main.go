// Command gen-v0.18.0 writes the dataset for ../v0.18.0.zip using the
// storage code of Quarry v0.18.0 (Lode v0.17.1, format 1.1.0). The fixture
// includes run records, commit index entries, artifact Volumes, sidecars,
// and metrics. Every input is fixed. Snapshot IDs and manifest creation
// times vary between generations.
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
// From the output directory, create a fresh archive with
// `zip -q -r <absolute path to new v0.18.0.zip> artifacts datasets`.
// Replace the fixture archive with that file. Keep the canonical and
// partition copies of the dataset manifests as emitted by the released writer.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"time"

	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

const dataset = "quarry"

// run is one run as Quarry v0.18.0 stores it. A run without a terminal event
// stands for one whose process was killed: it has a start record and no end
// record.
type run struct {
	source   string
	category string
	day      string
	id       string
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
	{source: "shop", category: "products", day: "2026-11-01", id: "run-ok-1", jobID: "job-1", attempt: 1, items: 3, artifact: true, sidecar: true, terminal: types.EventTypeRunComplete, outcome: types.OutcomeSuccess},
	{source: "shop", category: "products", day: "2026-11-01", id: "run-fail-1", attempt: 1, items: 1, terminal: types.EventTypeRunError, outcome: types.OutcomeScriptError},
	{source: "shop", category: "products", day: "2026-11-01", id: "run-retry-1", jobID: "job-1", parent: "run-fail-1", attempt: 2, items: 2, terminal: types.EventTypeRunComplete, outcome: types.OutcomeSuccess},
	{source: "news", category: "default", day: "2026-11-02", id: "run-killed-1", attempt: 1, items: 1},
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: gen-v0.18.0 <empty directory>")
	}
	ctx := context.Background()
	for i, r := range runs {
		if err := write(ctx, os.Args[1], i, r); err != nil {
			log.Fatalf("%s: %v", r.id, err)
		}
	}
}

func write(ctx context.Context, root string, i int, r run) error {
	cfg := lode.Config{Dataset: dataset, Source: r.source, Category: r.category, Day: r.day, RunID: r.id, Policy: "strict"}
	c, err := lode.NewLodeClient(ctx, cfg, root)
	if err != nil {
		return err
	}
	start, err := time.Parse(time.DateOnly, r.day)
	if err != nil {
		return err
	}
	start = start.Add(time.Duration(i+1) * time.Hour)
	meta := &types.RunMeta{RunID: r.id, Attempt: r.attempt}
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
			ContractVersion: types.ContractVersion, EventID: fmt.Sprintf("%s-e%d", r.id, seq), RunID: r.id,
			Seq: seq, Type: t, Ts: start.Add(time.Duration(seq) * time.Second).Format(time.RFC3339), Payload: payload,
			JobID: meta.JobID, ParentRunID: meta.ParentRunID, Attempt: r.attempt,
		}
	}
	items := make([]*types.EventEnvelope, 0, r.items)
	for n := range r.items {
		items = append(items, env(types.EventTypeItem, map[string]any{
			"item_type": "product",
			"n":         n,
			"title":     fmt.Sprintf("%s item %d", r.id, n),
		}))
	}
	// Two items per commit simulate a buffered policy's flushes.
	for batch := range slices.Chunk(items, 2) {
		if err := c.WriteEvents(ctx, dataset, r.id, batch); err != nil {
			return err
		}
	}
	if r.sidecar {
		if err := c.PutFile(ctx, "page.png", "image/png", []byte("png bytes of "+r.id)); err != nil {
			return err
		}
	}
	if r.artifact {
		if err := writeArtifact(ctx, c, r.id, env); err != nil {
			return err
		}
	}
	if r.terminal == "" {
		return nil
	}
	if err := c.WriteEvents(ctx, dataset, r.id, []*types.EventEnvelope{env(r.terminal, map[string]any{"summary": "done"})}); err != nil {
		return err
	}
	ended := start.Add(time.Duration(seq+1) * time.Second)
	snap := metrics.Snapshot{RunsStarted: 1, EventsReceived: seq, EventsPersisted: seq, RunID: r.id, JobID: r.jobID, Policy: "strict", Executor: "gen", StorageBackend: "fs"}
	if err := c.WriteMetrics(ctx, snap, ended); err != nil {
		return err
	}
	return c.WriteEndRecord(ctx, lode.RunEnd{EndedAt: ended, Outcome: r.outcome, Terminal: r.terminal, EventCount: seq, MetricsPersisted: true})
}

// writeArtifact stores one artifact in two ranges and commits it, as the
// runtime does: the commit event carries the storage that Finish returns,
// and WriteEvents writes the artifact's commit index entry.
func writeArtifact(ctx context.Context, c *lode.LodeClient, runID string, env func(types.EventType, map[string]any) *types.EventEnvelope) error {
	data := []byte("report of " + runID)
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
	return c.WriteEvents(ctx, dataset, runID, []*types.EventEnvelope{commit})
}
