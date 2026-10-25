# Benchmarks

Quarry includes a Go benchmark suite covering all three ingestion policy
implementations. Benchmarks live in [`quarry/policy/benchmark_test.go`](../../quarry/policy/benchmark_test.go) and
exercise the hot path: event/chunk ingestion, flush cycles, concurrent
access, and slow-sink behavior.

## Running

```bash
# Full suite (may take several minutes for slow-sink benchmarks)
cd quarry && go test ./policy/... -bench=. -benchmem -count=1

# Strict policy only
go test ./policy/... -bench=BenchmarkStrictPolicy -benchmem

# Buffered policy only
go test ./policy/... -bench=BenchmarkBufferedPolicy -benchmem

# Streaming policy (use -benchtime=100x for fast iteration)
go test ./policy/... -bench=BenchmarkStreamingPolicy -benchmem -benchtime=100x
```

## Latest Results

**Environment:** AMD Ryzen 9 5900XT (16-core), linux/amd64, Go 1.24

### Strict Policy

Synchronous, unbuffered writes. Every event/chunk hits the sink immediately.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| IngestEvent | 19.31 | 8 | 1 |
| IngestArtifactChunk | 17.80 | 8 | 1 |
| ConcurrentIngest (1 goroutine) | 19.22 | 8 | 1 |
| ConcurrentIngest (4 goroutines) | 26.14 | 8 | 1 |
| ConcurrentIngest (8 goroutines) | 28.23 | 8 | 1 |
| SlowSink (10us delay) | 1,051,050 | 8 | 1 |
| SlowSink (100us delay) | 1,054,886 | 8 | 1 |
| SlowSink (1ms delay) | 1,056,466 | 8 | 1 |

**Key findings:**
- Atomic stats counters eliminate lock contention: 1 -> 4 -> 8 goroutines
  scales from 19ns to 28ns (1.47x), down from 4.7x with mutex-based stats.
- Sink latency dominates: 1ms delay -> ~1ms/op regardless of other overhead.
- 1 allocation per call (the `[]*EventEnvelope` batch-of-1 wrapper).

### Buffered Policy

Bounded buffer with drop rules and batch flushes.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| IngestEvent (at_least_once) | 42.73 | 47 | 0 |
| IngestEvent (chunks_first) | 26.17 | 45 | 0 |
| IngestEvent (two_phase) | 27.01 | 48 | 0 |
| IngestThenFlush (batch=10) | 2,879 | 5,598 | 41 |
| IngestThenFlush (batch=100) | 26,247 | 48,138 | 401 |
| IngestThenFlush (batch=1000) | 303,871 | 486,990 | 4,745 |
| DropPressure | 25.30 | 0 | 0 |
| ConcurrentIngest | 87.04 | 44 | 0 |

**Key findings:**
- Amortized ingestion cost is ~27-43ns/event (mode-dependent).
- `at_least_once` mode is ~60% slower than others due to buffer preservation
  semantics on flush; acceptable for its stronger durability guarantee.
- Drop pressure path (droppable events hitting full buffer) costs only 25ns
  with zero allocations.
- Flush cost scales linearly with batch size (~288ns/event at batch=10,
  ~304ns/event at batch=1000).

### Streaming Policy

Continuous persistence with bounded buffer, batched writes, and backpressure.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| IngestEvent | 13.20 | 0 | 0 |
| IngestArtifactChunk | 68.70 | 21 | 0 |
| CountTriggerFlush (count=10) | 399.2 | 635 | 4 |
| CountTriggerFlush (count=50) | 532.5 | 543 | 4 |
| CountTriggerFlush (count=100) | 352.8 | 531 | 4 |
| CountTriggerFlush (count=500) | 369.9 | 520 | 4 |
| ConcurrentIngest | 224.6 | 176 | 1 |
| SlowSink (100us delay) | 21,307 | 24 | 0 |
| SlowSink (1ms delay) | 21,033 | 24 | 0 |
| FlushUnderLoad | 1,253 | 46 | 0 |

**Key findings:**
- Ingestion is ~13ns/event (fastest of all policies) — append-only into a
  pre-allocated buffer with no sync write.
- Buffer swap strategy decouples ingestion from sink latency: with a 1ms
  sink delay, streaming achieves ~21us/op vs strict's ~1ms/op (50x faster).
- Flush cost is dominated by the buffer swap + write, not the count threshold.
- Concurrent ingestion (224ns) is higher than strict (28ns) due to mutex
  contention on the shared buffer; this is inherent to batched buffering.

## Contention Analysis

Before it moved to atomic counters, `statsRecorder` used a `sync.Mutex` for all counter
operations. This caused severe contention scaling:

| Goroutines | Mutex-based (ns/op) | Atomic-based (ns/op) | Improvement |
|---:|---:|---:|---:|
| 1 | 22 | 19 | 1.2x |
| 4 | 43 | 26 | 1.7x |
| 8 | 103 | 28 | 3.7x |

The fix replaced `sync.Mutex` + `Stats` struct with individual
`sync/atomic.Int64` fields. Counter methods (`incTotalEvents`, etc.)
became lock-free `atomic.Add` calls. The `droppedByType` map (cold path,
only written by buffered policy drops) retains caller-provided
synchronization via the policy's own mutex.

## What the Benchmarks Measure

| Benchmark | What it tests |
|---|---|
| `IngestEvent` | Hot-path event ingestion (no flush) |
| `IngestArtifactChunk` | Hot-path chunk ingestion |
| `ConcurrentIngest` | Multi-goroutine contention on ingestion |
| `SlowSink` | Sink latency impact on throughput |
| `IngestThenFlush` | Full ingest + flush cycle at various batch sizes |
| `DropPressure` | Droppable event ingestion when buffer is full |
| `CountTriggerFlush` | Ingestion that triggers count-based flushes |
| `FlushUnderLoad` | Flush behavior during concurrent ingestion |

All benchmarks use a no-op or configurable-delay sink to isolate policy
overhead from I/O. Real-world performance will be dominated by sink
latency (network/disk), not policy overhead.

## Run Listing: Records vs Partitions

Store work to find runs and their states, measured by
`TestMeasure_ListRunsVsPartitionEnumeration` in `quarry/reader`:

```
cd quarry && go test ./reader -run Measure -v
```

200 runs over 10 days, each written through the real write path (two to
four event batches, a chunk batch, and a metrics record), a quarter of
them without an end record:

| Method | `List` calls | Keys listed | Objects read | Result |
|---|---|---|---|---|
| Run records, one page of 50 | 1 | 350 | 100 | 50 runs with state and outcome |
| Run records, all runs | 1 | 350 | 400 | 200 runs with state and outcome |
| Partition enumeration, all runs | 1 | 2,200 | 0 | 200 runs, terminal event present or not |

Run records list two keys per finished run and one per unterminated run,
regardless of what the run wrote. The partition tree lists a data object and a
manifest per batch per event type, so its size grows with every flush a
run makes; the runs here flush only a few times, and runs that flush on
an interval grow it further. On S3, the number of keys listed bounds the number of
`ListObjectsV2` requests (at most 1,000 keys each). Records also give the
outcome, which the partition tree cannot: it shows only whether a
terminal event was written. A page of records reads only the records of
the runs it returns.
