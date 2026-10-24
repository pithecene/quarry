// Command gen writes the dataset under ../v0.13.7 with the storage code of
// Quarry v0.13.7 (Lode v0.10.0), so that later releases are tested against
// data a released Quarry wrote. Every input is fixed; only snapshot IDs and
// manifest creation times vary between generations.
//
// The Go module of a released Quarry cannot be fetched by version, so this
// program builds against a checkout of the v0.13.7 tag. From an empty
// directory holding a copy of this file, with <checkout> a worktree at
// v0.13.7:
//
//	go mod init gen
//	go mod edit -require=github.com/pithecene-io/quarry@v0.0.0 -replace=github.com/pithecene-io/quarry=<checkout>/quarry
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

	"github.com/pithecene-io/quarry/lode"
	"github.com/pithecene-io/quarry/metrics"
	"github.com/pithecene-io/quarry/types"
)

// run is one run as Quarry v0.13.7 stores it. A run without a terminal event
// stands for one whose process was killed.
type run struct {
	cfg      lode.Config
	jobID    string
	parent   string
	attempt  int
	items    int
	artifact bool
	sidecar  bool
	terminal types.EventType
}

var runs = []run{
	{cfg: cfg("shop", "products", "2026-10-01", "run-ok-1"), jobID: "job-1", attempt: 1, items: 3, artifact: true, sidecar: true, terminal: types.EventTypeRunComplete},
	{cfg: cfg("shop", "products", "2026-10-01", "run-fail-1"), attempt: 1, items: 1, terminal: types.EventTypeRunError},
	{cfg: cfg("shop", "products", "2026-10-01", "run-retry-1"), jobID: "job-1", parent: "run-fail-1", attempt: 2, items: 2, terminal: types.EventTypeRunComplete},
	{cfg: cfg("news", "default", "2026-10-02", "run-killed-1"), attempt: 1, items: 1},
}

func cfg(source, category, day, runID string) lode.Config {
	return lode.Config{Dataset: "quarry", Source: source, Category: category, Day: day, RunID: runID, Policy: "strict"}
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: gen <empty directory>")
	}
	for i, r := range runs {
		if err := write(context.Background(), os.Args[1], i, r); err != nil {
			log.Fatalf("%s: %v", r.cfg.RunID, err)
		}
	}
}

func write(ctx context.Context, root string, i int, r run) error {
	c, err := lode.NewLodeClient(r.cfg, root)
	if err != nil {
		return err
	}
	start, err := time.Parse(time.DateOnly, r.cfg.Day)
	if err != nil {
		return err
	}
	start = start.Add(time.Duration(i+1) * time.Hour)
	var seq int64
	env := func(t types.EventType, payload map[string]any) *types.EventEnvelope {
		seq++
		e := &types.EventEnvelope{
			ContractVersion: types.ContractVersion, EventID: fmt.Sprintf("%s-e%d", r.cfg.RunID, seq), RunID: r.cfg.RunID,
			Seq: seq, Type: t, Ts: start.Add(time.Duration(seq) * time.Second).Format(time.RFC3339), Payload: payload, Attempt: r.attempt,
		}
		if r.jobID != "" {
			e.JobID = &r.jobID
		}
		if r.parent != "" {
			e.ParentRunID = &r.parent
		}
		return e
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
		data := []byte("report of " + r.cfg.RunID)
		chunk := &types.ArtifactChunk{ArtifactID: "art-1", Seq: 1, IsLast: true, Data: data}
		if err := c.WriteChunks(ctx, r.cfg.Dataset, r.cfg.RunID, []*types.ArtifactChunk{chunk}); err != nil {
			return err
		}
		commit := env(types.EventTypeArtifact, map[string]any{"artifact_id": "art-1", "name": "report.txt", "content_type": "text/plain", "size_bytes": float64(len(data))})
		if err := c.WriteEvents(ctx, r.cfg.Dataset, r.cfg.RunID, []*types.EventEnvelope{commit}); err != nil {
			return err
		}
	}
	if r.terminal == "" {
		return nil
	}
	if err := c.WriteEvents(ctx, r.cfg.Dataset, r.cfg.RunID, []*types.EventEnvelope{env(r.terminal, map[string]any{"summary": "done"})}); err != nil {
		return err
	}
	snap := metrics.Snapshot{RunsStarted: 1, EventsReceived: seq, EventsPersisted: seq, RunID: r.cfg.RunID, JobID: r.jobID, Policy: "strict", Executor: "gen", StorageBackend: "fs"}
	return c.WriteMetrics(ctx, snap, start.Add(time.Duration(seq+1)*time.Second))
}
