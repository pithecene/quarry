// Package lode persists runs in Lode and reads them back per CONTRACT_LODE.md.
package lode

import (
	"context"
	"time"

	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// DeriveDay computes the partition day from run start time.
// Format: YYYY-MM-DD in UTC per CONTRACT_LODE.md.
func DeriveDay(startTime time.Time) string {
	return startTime.UTC().Format("2006-01-02")
}

// DefaultDataset is the default Lode dataset name.
const DefaultDataset = "quarry"

// Config holds Lode sink configuration.
// All partition keys are required per CONTRACT_LODE.md.
type Config struct {
	// Dataset is the Lode dataset ID (default: "quarry", overridable via --storage-dataset).
	Dataset string
	// Source is the partition key for origin system/provider.
	Source string
	// Category is the partition key for logical data type.
	Category string
	// Day is the partition key derived from run start time (YYYY-MM-DD UTC).
	Day string
	// RunID is the partition key for run identifier.
	RunID string
	// Policy is the ingestion policy name (e.g. "strict", "buffered").
	Policy string
	// RetryObserver, if set, is invoked once per Lode CAS retry with the
	// 1-indexed attempt number (see lode.WithRetryObserver). Optional.
	RetryObserver func(attempt int)
	// IndexFailureObserver, if set, is invoked once per commit index entry
	// that could not be written (CONTRACT_LODE.md §Commit Index). The
	// artifact's commit record is persisted, so the failure fails no write.
	// Optional.
	IndexFailureObserver func(artifactID string, err error)
	// ReconcileObserver, if set, is invoked once per write that reported err and
	// succeeds because the object at key holds its bytes. Optional.
	ReconcileObserver func(key string, err error)
}

// Sink is a Lode-backed implementation of policy.Sink.
// Writes events to Lode storage per CONTRACT_LODE.md.
type Sink struct {
	config Config
	client Client
}

// Client abstracts the Lode storage client.
// Real implementations connect to Lode; stubs are used for testing.
type Client interface {
	// WriteEvents writes a batch of events to Lode.
	// Must preserve ordering within the batch.
	WriteEvents(ctx context.Context, dataset, runID string, events []*types.EventEnvelope) error

	// Close releases client resources.
	Close() error
}

// NewSink creates a new Lode sink.
func NewSink(config Config, client Client) *Sink {
	return &Sink{
		config: config,
		client: client,
	}
}

// WriteEvents implements policy.Sink.
func (s *Sink) WriteEvents(ctx context.Context, events []*types.EventEnvelope) error {
	return s.client.WriteEvents(ctx, s.config.Dataset, s.config.RunID, events)
}

// Close implements policy.Sink.
func (s *Sink) Close() error {
	return s.client.Close()
}

// Verify Sink implements policy.Sink and policy.EventSink.
var (
	_ policy.Sink      = (*Sink)(nil)
	_ policy.EventSink = (*Sink)(nil)
)
