package policy

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

// --- Test Helpers ---

// benchEnvelope returns a realistic event envelope for benchmarks.
func benchEnvelope(seq int64) *types.EventEnvelope {
	return &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         fmt.Sprintf("evt-%d", seq),
		RunID:           "bench-run-001",
		Seq:             seq,
		Type:            types.EventTypeItem,
		Ts:              "2026-02-10T00:00:00Z",
		Payload: map[string]any{
			"url":    "https://example.com/page",
			"status": 200,
			"title":  "Benchmark Page",
		},
		Attempt: 1,
	}
}

// benchChunk returns a realistic artifact chunk for benchmarks.
func benchChunk(seq int64) *types.ArtifactChunk {
	return &types.ArtifactChunk{
		ArtifactID: "art-001",
		Seq:        seq,
		IsLast:     false,
		Data:       make([]byte, 4096),
	}
}

// noopSink is a zero-allocation sink for benchmarks.
// It does no locking and no recording — pure throughput measurement.
type noopSink struct{}

func (noopSink) WriteEvents(_ context.Context, _ []*types.EventEnvelope) error { return nil }
func (noopSink) WriteChunks(_ context.Context, _ []*types.ArtifactChunk) error { return nil }
func (noopSink) Close() error                                                  { return nil }

// slowSink adds a fixed delay per write to simulate storage latency.
type slowSink struct {
	delay time.Duration
}

func (s slowSink) WriteEvents(_ context.Context, _ []*types.EventEnvelope) error {
	time.Sleep(s.delay)
	return nil
}

func (s slowSink) WriteChunks(_ context.Context, _ []*types.ArtifactChunk) error {
	time.Sleep(s.delay)
	return nil
}

func (s slowSink) Close() error { return nil }

// ============================================
// Strict Policy Benchmarks
// ============================================

// BenchmarkStrictPolicy_IngestEvent measures per-event ingestion throughput
// for strict policy with a zero-cost sink.
func BenchmarkStrictPolicy_IngestEvent(b *testing.B) {
	pol := NewStrictPolicy(noopSink{})
	ctx := b.Context()
	env := benchEnvelope(1)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		if err := pol.IngestEvent(ctx, env); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStrictPolicy_IngestArtifactChunk measures per-chunk throughput.
func BenchmarkStrictPolicy_IngestArtifactChunk(b *testing.B) {
	pol := NewStrictPolicy(noopSink{})
	ctx := b.Context()
	chunk := benchChunk(1)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		if err := pol.IngestArtifactChunk(ctx, chunk); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStrictPolicy_ConcurrentIngest measures contention under concurrent writers
// with varying parallelism levels.
func BenchmarkStrictPolicy_ConcurrentIngest(b *testing.B) {
	for _, goroutines := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("goroutines=%d", goroutines), func(b *testing.B) {
			prev := runtime.GOMAXPROCS(goroutines)
			b.Cleanup(func() { runtime.GOMAXPROCS(prev) })

			pol := NewStrictPolicy(noopSink{})
			ctx := b.Context()
			env := benchEnvelope(1)

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := pol.IngestEvent(ctx, env); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkStrictPolicy_SlowSink measures backpressure with simulated storage latency.
func BenchmarkStrictPolicy_SlowSink(b *testing.B) {
	for _, delay := range []time.Duration{10 * time.Microsecond, 100 * time.Microsecond, time.Millisecond} {
		b.Run(fmt.Sprintf("delay=%s", delay), func(b *testing.B) {
			pol := NewStrictPolicy(slowSink{delay: delay})
			ctx := b.Context()
			env := benchEnvelope(1)

			b.ResetTimer()
			for b.Loop() {
				if err := pol.IngestEvent(ctx, env); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ============================================
// Buffered Policy Benchmarks
// ============================================

// BenchmarkBufferedPolicy_IngestEvent measures event buffering throughput.
func BenchmarkBufferedPolicy_IngestEvent(b *testing.B) {
	for _, mode := range []FlushMode{FlushAtLeastOnce, FlushChunksFirst, FlushTwoPhase} {
		b.Run(fmt.Sprintf("mode=%s", mode), func(b *testing.B) {
			pol, err := NewBufferedPolicy(noopSink{}, BufferedConfig{
				MaxBufferEvents: 0, // bytes-only limit
				MaxBufferBytes:  1 << 62,
				FlushMode:       mode,
			})
			if err != nil {
				b.Fatal(err)
			}

			ctx := b.Context()
			env := benchEnvelope(1)

			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				if err := pol.IngestEvent(ctx, env); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkBufferedPolicy_IngestThenFlush measures the cost of buffering N events + one flush.
func BenchmarkBufferedPolicy_IngestThenFlush(b *testing.B) {
	for _, batchSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			pol, err := NewBufferedPolicy(noopSink{}, BufferedConfig{
				MaxBufferEvents: batchSize + 1,
				MaxBufferBytes:  1 << 62,
				FlushMode:       FlushAtLeastOnce,
			})
			if err != nil {
				b.Fatal(err)
			}

			ctx := b.Context()

			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				for j := range batchSize {
					if err := pol.IngestEvent(ctx, benchEnvelope(int64(j))); err != nil {
						b.Fatal(err)
					}
				}
				if err := pol.Flush(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkBufferedPolicy_DropPressure measures drop-path cost when buffer is full
// and incoming events are droppable.
func BenchmarkBufferedPolicy_DropPressure(b *testing.B) {
	pol, err := NewBufferedPolicy(noopSink{}, BufferedConfig{
		MaxBufferEvents: 10,
		MaxBufferBytes:  1 << 62,
		FlushMode:       FlushAtLeastOnce,
	})
	if err != nil {
		b.Fatal(err)
	}

	ctx := b.Context()

	// Fill buffer with non-droppable events
	for i := range 10 {
		env := benchEnvelope(int64(i))
		env.Type = types.EventTypeItem
		if err := pol.IngestEvent(ctx, env); err != nil {
			b.Fatal(err)
		}
	}

	// Droppable event that will be dropped on each iteration
	droppable := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "drop-001",
		RunID:           "bench-run-001",
		Seq:             100,
		Type:            types.EventTypeLog,
		Ts:              "2026-02-10T00:00:00Z",
		Payload:         map[string]any{"level": "debug", "message": "benchmark log"},
		Attempt:         1,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		if err := pol.IngestEvent(ctx, droppable); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBufferedPolicy_ConcurrentIngest measures contention under concurrent writers.
func BenchmarkBufferedPolicy_ConcurrentIngest(b *testing.B) {
	pol, err := NewBufferedPolicy(noopSink{}, BufferedConfig{
		MaxBufferEvents: 0, // bytes-only limit
		MaxBufferBytes:  1 << 62,
		FlushMode:       FlushAtLeastOnce,
	})
	if err != nil {
		b.Fatal(err)
	}

	ctx := b.Context()
	env := benchEnvelope(1)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := pol.IngestEvent(ctx, env); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ============================================
// Streaming Policy Benchmarks
// ============================================

// BenchmarkStreamingPolicy_IngestEvent measures per-event ingestion throughput
// (count trigger only, large threshold so no flushes during benchmark).
func BenchmarkStreamingPolicy_IngestEvent(b *testing.B) {
	pol, err := NewStreamingPolicy(noopSink{}, StreamingConfig{
		FlushCount: 1_000_000,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(iox.CloseFunc(pol))

	ctx := b.Context()
	env := benchEnvelope(1)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		if err := pol.IngestEvent(ctx, env); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStreamingPolicy_IngestArtifactChunk measures per-chunk buffering throughput.
func BenchmarkStreamingPolicy_IngestArtifactChunk(b *testing.B) {
	pol, err := NewStreamingPolicy(noopSink{}, StreamingConfig{
		FlushCount: 1_000_000,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(iox.CloseFunc(pol))

	ctx := b.Context()
	chunk := benchChunk(1)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		if err := pol.IngestArtifactChunk(ctx, chunk); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStreamingPolicy_CountTriggerFlush measures throughput when every N events
// triggers a flush (realistic hot path).
func BenchmarkStreamingPolicy_CountTriggerFlush(b *testing.B) {
	for _, flushCount := range []int{10, 50, 100, 500} {
		b.Run(fmt.Sprintf("flushCount=%d", flushCount), func(b *testing.B) {
			pol, err := NewStreamingPolicy(noopSink{}, StreamingConfig{
				FlushCount: flushCount,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(iox.CloseFunc(pol))

			ctx := b.Context()

			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				if err := pol.IngestEvent(ctx, benchEnvelope(1)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStreamingPolicy_ConcurrentIngest measures contention under concurrent writers
// with periodic count-triggered flushes.
func BenchmarkStreamingPolicy_ConcurrentIngest(b *testing.B) {
	pol, err := NewStreamingPolicy(noopSink{}, StreamingConfig{
		FlushCount: 100,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(iox.CloseFunc(pol))

	ctx := b.Context()
	env := benchEnvelope(1)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := pol.IngestEvent(ctx, env); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkStreamingPolicy_SlowSink measures how the buffer swap strategy
// handles storage latency (ingestion should not block during writes).
func BenchmarkStreamingPolicy_SlowSink(b *testing.B) {
	for _, delay := range []time.Duration{100 * time.Microsecond, time.Millisecond} {
		b.Run(fmt.Sprintf("delay=%s", delay), func(b *testing.B) {
			pol, err := NewStreamingPolicy(slowSink{delay: delay}, StreamingConfig{
				FlushCount: 50,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(iox.CloseFunc(pol))

			ctx := b.Context()
			env := benchEnvelope(1)

			b.ResetTimer()
			for b.Loop() {
				if err := pol.IngestEvent(ctx, env); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ============================================
// Cross-Policy Comparison
// ============================================

// BenchmarkPolicies_IngestEvent_Comparison provides a side-by-side comparison
// of per-event ingestion cost across all three policies.
func BenchmarkPolicies_IngestEvent_Comparison(b *testing.B) {
	ctx := b.Context()
	env := benchEnvelope(1)

	b.Run("strict", func(b *testing.B) {
		pol := NewStrictPolicy(noopSink{})
		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			if err := pol.IngestEvent(ctx, env); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("buffered", func(b *testing.B) {
		pol, _ := NewBufferedPolicy(noopSink{}, BufferedConfig{
			MaxBufferEvents: 0, // bytes-only limit
			MaxBufferBytes:  1 << 62, // effectively unbounded
			FlushMode:       FlushAtLeastOnce,
		})
		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			if err := pol.IngestEvent(ctx, env); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("streaming", func(b *testing.B) {
		pol, _ := NewStreamingPolicy(noopSink{}, StreamingConfig{
			FlushCount: 1_000_000,
		})
		b.Cleanup(iox.CloseFunc(pol))
		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			if err := pol.IngestEvent(ctx, env); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPolicies_Stats_Comparison measures stats snapshot cost across policies.
func BenchmarkPolicies_Stats_Comparison(b *testing.B) {
	ctx := b.Context()
	env := benchEnvelope(1)

	b.Run("strict", func(b *testing.B) {
		pol := NewStrictPolicy(noopSink{})
		// Pre-populate some stats
		for range 100 {
			_ = pol.IngestEvent(ctx, env)
		}
		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			_ = pol.Stats()
		}
	})

	b.Run("buffered", func(b *testing.B) {
		pol, _ := NewBufferedPolicy(noopSink{}, BufferedConfig{
			MaxBufferEvents: 0, // bytes-only limit
			MaxBufferBytes:  1 << 62,
			FlushMode:       FlushAtLeastOnce,
		})
		for range 100 {
			_ = pol.IngestEvent(ctx, env)
		}
		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			_ = pol.Stats()
		}
	})

	b.Run("streaming", func(b *testing.B) {
		pol, _ := NewStreamingPolicy(noopSink{}, StreamingConfig{
			FlushCount: 1_000_000,
		})
		b.Cleanup(iox.CloseFunc(pol))
		for range 100 {
			_ = pol.IngestEvent(ctx, env)
		}
		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			_ = pol.Stats()
		}
	})
}

// BenchmarkPolicies_Concurrent_Comparison measures concurrent ingestion cost
// across all three policies with concurrent flush pressure.
func BenchmarkPolicies_Concurrent_Comparison(b *testing.B) {
	ctx := b.Context()
	env := benchEnvelope(1)

	b.Run("strict", func(b *testing.B) {
		pol := NewStrictPolicy(noopSink{})
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = pol.IngestEvent(ctx, env)
			}
		})
	})

	b.Run("buffered", func(b *testing.B) {
		pol, _ := NewBufferedPolicy(noopSink{}, BufferedConfig{
			MaxBufferEvents: 0, // bytes-only limit
			MaxBufferBytes:  1 << 62,
			FlushMode:       FlushAtLeastOnce,
		})
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = pol.IngestEvent(ctx, env)
			}
		})
	})

	b.Run("streaming/no-flush", func(b *testing.B) {
		pol, _ := NewStreamingPolicy(noopSink{}, StreamingConfig{
			FlushCount: 1_000_000,
		})
		b.Cleanup(iox.CloseFunc(pol))
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = pol.IngestEvent(ctx, env)
			}
		})
	})

	b.Run("streaming/with-flush", func(b *testing.B) {
		pol, _ := NewStreamingPolicy(noopSink{}, StreamingConfig{
			FlushCount: 100,
		})
		b.Cleanup(iox.CloseFunc(pol))
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = pol.IngestEvent(ctx, env)
			}
		})
	})
}

// BenchmarkPolicies_MixedWorkload simulates a realistic workload with
// events and artifact chunks interleaved, measured across all policies.
func BenchmarkPolicies_MixedWorkload(b *testing.B) {
	ctx := b.Context()

	b.Run("strict", func(b *testing.B) {
		pol := NewStrictPolicy(noopSink{})
		b.ResetTimer()
		b.ReportAllocs()
		for i := int64(0); b.Loop(); i++ {
			if i%10 == 0 {
				_ = pol.IngestArtifactChunk(ctx, benchChunk(i))
			} else {
				_ = pol.IngestEvent(ctx, benchEnvelope(i))
			}
		}
	})

	b.Run("buffered", func(b *testing.B) {
		pol, _ := NewBufferedPolicy(noopSink{}, BufferedConfig{
			MaxBufferEvents: 0, // bytes-only limit
			MaxBufferBytes:  1 << 62,
			FlushMode:       FlushAtLeastOnce,
		})
		b.ResetTimer()
		b.ReportAllocs()
		for i := int64(0); b.Loop(); i++ {
			if i%10 == 0 {
				_ = pol.IngestArtifactChunk(ctx, benchChunk(i))
			} else {
				_ = pol.IngestEvent(ctx, benchEnvelope(i))
			}
		}
	})

	b.Run("streaming", func(b *testing.B) {
		pol, _ := NewStreamingPolicy(noopSink{}, StreamingConfig{
			FlushCount: 100,
		})
		b.Cleanup(iox.CloseFunc(pol))
		b.ResetTimer()
		b.ReportAllocs()
		for i := int64(0); b.Loop(); i++ {
			if i%10 == 0 {
				_ = pol.IngestArtifactChunk(ctx, benchChunk(i))
			} else {
				_ = pol.IngestEvent(ctx, benchEnvelope(i))
			}
		}
	})
}

// BenchmarkStreamingPolicy_FlushUnderLoad measures flush cost while concurrent
// ingestion continues (the hot path for streaming in production).
func BenchmarkStreamingPolicy_FlushUnderLoad(b *testing.B) {
	pol, err := NewStreamingPolicy(noopSink{}, StreamingConfig{
		FlushCount: 1_000_000, // disable auto-flush
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(iox.CloseFunc(pol))

	ctx := b.Context()

	// Background writer goroutines
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			env := benchEnvelope(1)
			for {
				select {
				case <-stop:
					return
				default:
					_ = pol.IngestEvent(ctx, env)
				}
			}
		}()
	}

	// Let writers fill the buffer
	time.Sleep(time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = pol.Flush(ctx)
	}
	b.StopTimer()

	close(stop)
	wg.Wait()
}
