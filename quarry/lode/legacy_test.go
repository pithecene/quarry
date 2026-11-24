package lode

import (
	"context"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/metrics"
)

// writeLegacyMetrics writes c's metrics record and pending sidecar inventory,
// as releases before v0.20.0 did when a run ended. internal/legacydata does so
// for other packages, which this package's own tests cannot import.
func writeLegacyMetrics(ctx context.Context, c *LodeClient, snap metrics.Snapshot, completedAt time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.dataset.Write(ctx, lode.R(toMetricsRecordMap(snap, c.config, completedAt)), c.snapshotMetadata()); err != nil {
		return err
	}
	c.drainPendingFiles()
	return nil
}

// toMetricsRecordMap converts a metrics.Snapshot to a map for Lode storage.
// Written to event_type=metrics partition with record_kind=metrics.
// Key names use _total suffix to match CONTRACT_METRICS.md naming.
func toMetricsRecordMap(snap metrics.Snapshot, cfg Config, completedAt time.Time) map[string]any {
	m := metricsFields(snap)
	m["record_kind"] = RecordKindMetrics
	m["event_type"] = "metrics" // partition key
	m["ts"] = completedAt.UTC().Format(time.RFC3339)
	m["source"] = cfg.Source
	m["category"] = cfg.Category
	m["day"] = cfg.Day
	return m
}

// sidecarsOf returns the sidecar files of the run cfg names.
func sidecarsOf(t *testing.T, factory lode.Factory[lode.RangeStore], cfg Config) []SidecarFileRef {
	t.Helper()
	details, err := QueryRunDetails(t.Context(), factory, cfg)
	if err != nil {
		t.Fatalf("QueryRunDetails: %v", err)
	}
	return details.SidecarFiles
}
