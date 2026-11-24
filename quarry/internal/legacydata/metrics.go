// Package legacydata writes data in the forms that earlier releases stored, for
// the tests that read it. It is internal, so no consumer of the module depends
// on it.
package legacydata

import (
	"context"
	"time"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
)

// WriteMetrics writes the metrics record of the run cfg names, as releases
// before v0.20.0 did when a run ended.
func WriteMetrics(ctx context.Context, factory lode.Factory[lode.RangeStore], cfg qlode.Config, snap metrics.Snapshot, completedAt time.Time) error {
	store, err := factory()
	if err != nil {
		return err
	}
	ds, err := qlode.OpenDataset(ctx, cfg.Dataset, store)
	if err != nil {
		return err
	}
	// Those releases kept the fields that an end record keeps now.
	record := qlode.NewEndRecord(cfg, qlode.RunEnd{Metrics: &snap}).Metrics
	record["record_kind"] = qlode.RecordKindMetrics
	record["event_type"] = "metrics"
	record["ts"] = completedAt.UTC().Format(time.RFC3339)
	record["source"], record["category"], record["day"] = cfg.Source, cfg.Category, cfg.Day
	_, err = ds.Write(ctx, lode.R(record), map[string]any{})
	return err
}
