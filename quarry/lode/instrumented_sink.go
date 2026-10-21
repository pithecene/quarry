package lode

import (
	"context"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// InstrumentedSink wraps a policy.Sink and records write metrics
// per CONTRACT_METRICS.md. Each WriteEvents/WriteChunks call increments
// lode_write_success or lode_write_failure on the metrics collector.
type InstrumentedSink struct {
	inner     policy.Sink
	collector *metrics.Collector
}

// NewInstrumentedSink wraps a sink with metrics instrumentation.
func NewInstrumentedSink(inner policy.Sink, collector *metrics.Collector) *InstrumentedSink {
	return &InstrumentedSink{inner: inner, collector: collector}
}

// WriteEvents delegates to the inner sink and records success or failure.
func (s *InstrumentedSink) WriteEvents(ctx context.Context, events []*types.EventEnvelope) error {
	err := s.inner.WriteEvents(ctx, events)
	if err != nil {
		s.collector.IncLodeWriteFailure()
	} else {
		s.collector.IncLodeWriteSuccess()
	}
	return err
}

// WriteChunks delegates to the inner sink and records success or failure.
func (s *InstrumentedSink) WriteChunks(ctx context.Context, chunks []*types.ArtifactChunk) error {
	err := s.inner.WriteChunks(ctx, chunks)
	if err != nil {
		s.collector.IncLodeWriteFailure()
	} else {
		s.collector.IncLodeWriteSuccess()
	}
	return err
}

// Close delegates to the inner sink.
func (s *InstrumentedSink) Close() error {
	return s.inner.Close()
}

// Verify InstrumentedSink implements policy.Sink and policy.EventSink.
var _ policy.Sink = (*InstrumentedSink)(nil)
var _ policy.EventSink = (*InstrumentedSink)(nil)
