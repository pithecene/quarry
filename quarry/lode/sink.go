// Package lode provides Lode integration per CONTRACT_LODE.md.
//
// This package defines the interface and stub for Lode persistence.
// Real implementations will connect to the actual Lode storage system.
package lode

import (
	"context"
	"time"

	"github.com/pithecene/quarry/metrics"
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
}

// Sink is a Lode-backed implementation of policy.Sink.
// Writes events and chunks to Lode storage per CONTRACT_LODE.md.
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

	// WriteChunks writes a batch of artifact chunks to Lode.
	// Must preserve ordering within the batch.
	WriteChunks(ctx context.Context, dataset, runID string, chunks []*types.ArtifactChunk) error

	// WriteMetrics writes a metrics snapshot to Lode.
	// Written to event_type=metrics partition with record_kind=metrics.
	WriteMetrics(ctx context.Context, snap metrics.Snapshot, completedAt time.Time) error

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

// WriteChunks implements policy.Sink.
func (s *Sink) WriteChunks(ctx context.Context, chunks []*types.ArtifactChunk) error {
	return s.client.WriteChunks(ctx, s.config.Dataset, s.config.RunID, chunks)
}

// Close implements policy.Sink.
func (s *Sink) Close() error {
	return s.client.Close()
}

// Verify Sink implements policy.Sink and policy.EventSink.
var _ policy.Sink = (*Sink)(nil)
var _ policy.EventSink = (*Sink)(nil)

// StubClient is a test client that accepts writes without persisting.
// Use for integration testing before real Lode is available.
type StubClient struct {
	Events  []StubEventRecord
	Chunks  []StubChunkRecord
	Metrics []StubMetricsRecord
	Closed  bool
}

// StubEventRecord is a recorded event write for testing.
type StubEventRecord struct {
	Dataset string
	RunID   string
	Events  []*types.EventEnvelope
}

// StubChunkRecord is a recorded chunk write for testing.
type StubChunkRecord struct {
	Dataset string
	RunID   string
	Chunks  []*types.ArtifactChunk
}

// StubMetricsRecord is a recorded metrics write for testing.
type StubMetricsRecord struct {
	Snapshot    metrics.Snapshot
	CompletedAt time.Time
}

// NewStubClient creates a new stub client.
func NewStubClient() *StubClient {
	return &StubClient{}
}

// WriteEvents implements Client.
func (c *StubClient) WriteEvents(_ context.Context, dataset, runID string, events []*types.EventEnvelope) error {
	c.Events = append(c.Events, StubEventRecord{
		Dataset: dataset,
		RunID:   runID,
		Events:  events,
	})
	return nil
}

// WriteChunks implements Client.
func (c *StubClient) WriteChunks(_ context.Context, dataset, runID string, chunks []*types.ArtifactChunk) error {
	c.Chunks = append(c.Chunks, StubChunkRecord{
		Dataset: dataset,
		RunID:   runID,
		Chunks:  chunks,
	})
	return nil
}

// WriteMetrics implements Client.
func (c *StubClient) WriteMetrics(_ context.Context, snap metrics.Snapshot, completedAt time.Time) error {
	c.Metrics = append(c.Metrics, StubMetricsRecord{
		Snapshot:    snap,
		CompletedAt: completedAt,
	})
	return nil
}

// Close implements Client.
func (c *StubClient) Close() error {
	c.Closed = true
	return nil
}

// Verify StubClient implements Client.
var _ Client = (*StubClient)(nil)
