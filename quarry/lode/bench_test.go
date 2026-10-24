package lode_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/reader"
	"github.com/pithecene/quarry/types"
)

// These benchmarks measure Quarry's storage paths over Lode, so a Lode
// upgrade can be compared with benchstat on the same inputs:
//
//	go test -run '^$' -bench . -count 10 ./lode/ > new.txt

// benchBackends are the stores the write benchmarks run against.
var benchBackends = []struct {
	name    string
	factory func(b *testing.B) lode.Factory[lode.RangeStore]
}{
	{"memory", func(*testing.B) lode.Factory[lode.RangeStore] { return lode.Use(lode.NewMemory()) }},
	{"fs", func(b *testing.B) lode.Factory[lode.RangeStore] { return lode.FS(b.TempDir()) }},
}

func benchConfig(r int) qlode.Config {
	return qlode.Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-01", RunID: fmt.Sprintf("run-%04d", r), Policy: "strict"}
}

func benchEvents(cfg qlode.Config, first int64, n int) []*types.EventEnvelope {
	events := make([]*types.EventEnvelope, n)
	for i := range events {
		seq := first + int64(i)
		events[i] = &types.EventEnvelope{
			ContractVersion: types.ContractVersion, EventID: fmt.Sprintf("e%d", seq), RunID: cfg.RunID, Seq: seq,
			Type: types.EventTypeItem, Ts: "2026-10-01T01:00:00Z", Attempt: 1,
			Payload: map[string]any{"item_type": "product", "title": "Benchmark Page", "price": 9.25},
		}
	}
	return events
}

// BenchmarkLodeClient_WriteEvents measures one commit of a batch of events:
// a batch of one is what the strict policy commits per event.
func BenchmarkLodeClient_WriteEvents(b *testing.B) {
	for _, be := range benchBackends {
		for _, batch := range []int{1, 20} {
			b.Run(fmt.Sprintf("%s/batch=%d", be.name, batch), func(b *testing.B) {
				cfg := benchConfig(0)
				c, err := qlode.NewLodeClientWithFactory(b.Context(), cfg, be.factory(b))
				if err != nil {
					b.Fatal(err)
				}
				var seq int64 = 1
				for b.Loop() {
					if err := c.WriteEvents(b.Context(), cfg.Dataset, cfg.RunID, benchEvents(cfg, seq, batch)); err != nil {
						b.Fatal(err)
					}
					seq += int64(batch)
				}
			})
		}
	}
}

// benchDataset writes runs of events and a metrics record each, as the
// runtime does, and returns the store's factory.
func benchDataset(b *testing.B, runs, events int) lode.Factory[lode.RangeStore] {
	b.Helper()
	factory := lode.Use(lode.NewMemory())
	for r := range runs {
		cfg := benchConfig(r)
		c, err := qlode.NewLodeClientWithFactory(b.Context(), cfg, factory)
		if err != nil {
			b.Fatal(err)
		}
		started := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC)
		if err := c.WriteStartRecord(b.Context(), &types.RunMeta{Attempt: 1}, started); err != nil {
			b.Fatal(err)
		}
		for i := 0; i < events; i += 10 {
			if err := c.WriteEvents(b.Context(), cfg.Dataset, cfg.RunID, benchEvents(cfg, int64(i+1), 10)); err != nil {
				b.Fatal(err)
			}
		}
		if err := c.WriteMetrics(b.Context(), metrics.Snapshot{RunID: cfg.RunID}, started); err != nil {
			b.Fatal(err)
		}
	}
	return factory
}

// BenchmarkNewLodeClient measures opening a client, which reads the
// dataset's head.
func BenchmarkNewLodeClient(b *testing.B) {
	factory := benchDataset(b, 10, 50)
	cfg := benchConfig(10)
	for b.Loop() {
		if _, err := qlode.NewLodeClientWithFactory(b.Context(), cfg, factory); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReader_ReadEvents measures reading one run's events from a
// dataset of 10 runs.
func BenchmarkReader_ReadEvents(b *testing.B) {
	factory := benchDataset(b, 10, 50)
	rd, err := reader.New("quarry", factory)
	if err != nil {
		b.Fatal(err)
	}
	run, err := rd.FindRun(b.Context(), reader.Selector{Source: "src", Category: "cat", Day: "2026-10-01"}, "run-0003")
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, err := rd.ReadEvents(b.Context(), run); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueryLatestMetrics measures finding one run's metrics record in a
// dataset of 10 runs, which reads every snapshot's manifest.
func BenchmarkQueryLatestMetrics(b *testing.B) {
	factory := benchDataset(b, 10, 50)
	for b.Loop() {
		if _, err := qlode.QueryLatestMetrics(b.Context(), factory, "quarry", "run-0003", ""); err != nil {
			b.Fatal(err)
		}
	}
}
