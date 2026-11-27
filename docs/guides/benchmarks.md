# Benchmarks

Quarry includes a Go benchmark suite covering all three ingestion policy
implementations. Benchmarks live in [`quarry/policy/benchmark_test.go`](../../quarry/policy/benchmark_test.go) and
exercise the hot path: event ingestion, flush cycles, concurrent
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

## Historical Results

These results predate the current policies and have not been measured again. Do not
take them as the current performance.

**Environment:** AMD Ryzen 9 5900XT (16-core), linux/amd64, Go 1.24

### Strict Policy

Synchronous, unbuffered writes. Every event hits the sink immediately.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| IngestEvent | 19.31 | 8 | 1 |
| ConcurrentIngest (1 goroutine) | 19.22 | 8 | 1 |
| ConcurrentIngest (4 goroutines) | 26.14 | 8 | 1 |
| ConcurrentIngest (8 goroutines) | 28.23 | 8 | 1 |
| SlowSink (10us delay) | 1,051,050 | 8 | 1 |
| SlowSink (100us delay) | 1,054,886 | 8 | 1 |
| SlowSink (1ms delay) | 1,056,466 | 8 | 1 |

**Key findings:**
- Atomic stats counters eliminate lock contention: 1 -> 4 -> 8 goroutines
  scales from 19ns to 28ns (1.47x).
- Sink latency dominates: 1ms delay -> ~1ms/op regardless of other overhead.
- 1 allocation per call (the `[]*EventEnvelope` batch-of-1 wrapper).

### Buffered Policy

Bounded buffer with drop rules and batch flushes.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| IngestThenFlush (batch=10) | 2,879 | 5,598 | 41 |
| IngestThenFlush (batch=100) | 26,247 | 48,138 | 401 |
| IngestThenFlush (batch=1000) | 303,871 | 486,990 | 4,745 |
| DropPressure | 25.30 | 0 | 0 |
| ConcurrentIngest | 87.04 | 44 | 0 |

**Key findings:**
- Drop pressure path (droppable events hitting full buffer) costs only 25ns
  with zero allocations.
- Flush cost scales linearly with batch size (~288ns/event at batch=10,
  ~304ns/event at batch=1000).

### Streaming Policy

Continuous persistence with bounded buffer, batched writes, and backpressure.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| IngestEvent | 13.20 | 0 | 0 |
| CountTriggerFlush (count=10) | 399.2 | 635 | 4 |
| CountTriggerFlush (count=50) | 532.5 | 543 | 4 |
| CountTriggerFlush (count=100) | 352.8 | 531 | 4 |
| CountTriggerFlush (count=500) | 369.9 | 520 | 4 |
| ConcurrentIngest | 224.6 | 176 | 1 |
| SlowSink (100us delay) | 21,307 | 24 | 0 |
| SlowSink (1ms delay) | 21,033 | 24 | 0 |
| FlushUnderLoad | 1,253 | 46 | 0 |

**Key findings:**
- Ingestion is ~13ns/event (fastest of all policies), because it appends to a
  pre-allocated buffer with no sync write.
- Buffer swap strategy decouples ingestion from sink latency: with a 1ms
  sink delay, streaming achieves ~21us/op vs strict's ~1ms/op (50x faster).
- Flush cost is dominated by the buffer swap + write, not the count threshold.
- Concurrent ingestion (224ns) is higher than strict (28ns) due to mutex
  contention on the shared buffer. This is inherent to batched buffering.

## What the Benchmarks Measure

| Benchmark | What it tests |
|---|---|
| `IngestEvent` | Hot-path event ingestion (no flush) |
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

The benchmark writes 200 runs over 10 days, each through the real write path (two to
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
run makes. The runs here flush only a few times, and runs that flush on
an interval grow it further. On S3, the number of keys listed bounds the number of
`ListObjectsV2` requests (at most 1,000 keys each). Records also give the
outcome, which the partition tree cannot: it shows only whether a
terminal event was written. A page of records reads only the records of
the runs it returns.

## Prune Dry Run on a Large Dataset

A regression baseline for prune, measured once on 2026-11-27 against a
dataset on Cloudflare R2 with 44,917 keys, 14,834 snapshots and 119 runs.
The command was `quarry prune --dry-run --failed-days 0`, which selected 32
runs. No test reproduces it, because it needs a bucket of that size.

| Quarry | Lode | Wall time | Peak memory | Result |
|---|---|---|---|---|
| v0.20.2 | v0.18.5 | no report after 13 h 50 min | not measured | none |
| v0.20.3 | v0.18.6 | 989 s | not measured | 32 runs selected |
| quarry#492 | the code of v0.18.7 | 815 s | 107 MiB | the same per-run results |

The last row measured the merged code that v0.20.4 and Lode v0.18.7 release,
before either tag. A second run of that code used a store that refuses and
logs every write. It took 782 s, made 17,603 `GET`, 325 `LIST` and 32
existence requests, logged no write attempt, and reported the same per-run
results.

Where the 815 s went:

| Phase | Requests | Time |
|---|---|---|
| Listing the dataset's keys | 44,917 keys listed | 30 s |
| Loading every manifest, 128 ahead | 14,834 `GET` | about 30 s |
| Listing manifest keys again for each later run | 31 `LIST` of 14,833 keys | 265 s |
| Reading tombstones | 685 | 103 s |
| Loading manifests of the selected runs | 542 `GET` | 87 s |
| Reading the head pointer | 321 | 49 s |
| Listing each run's partition manifests | 63 `LIST` | 11 s |
| Artifacts, sidecar files, partitions and run records | the remainder | about 167 s |

The per-run phase is serial. Its largest cost, the repeated listing of every
manifest key, comes from how Lode discovers committed snapshots, and Quarry
cannot remove it. A change that raises the wall time or the request counts
of the same command on a dataset of this size is a regression.

## Dataset Compression Under `strict`

`BenchmarkLodeClient_WriteEvents_Compression` in `quarry/lode` measures one
strict commit of one item event into a memory store. Before timing, the
benchmark seeds a dataset with the setting under test, and the client adopts
the head's settings, as every Quarry writer does:

```
cd quarry && go test -run '^$' -bench 'WriteEvents_Compression' -benchmem -count 5 -benchtime 2000x ./lode/
```

The table gives medians across five runs of 2,000 commits each on macOS
arm64 (Apple M5) with Go 1.27.3, `GOMAXPROCS=10`, and Lode v0.18.2.
After timing ends, the benchmark reports two sizes per measured commit.
`stored-B/op` sums the data-object sizes that the manifests record.
`stored-total-B/op` is the growth of the whole store across the measured
commits: data objects and manifests. The latest pointer existed before
timing and each commit rewrites it, so it adds almost nothing. Neither size
counts the seed snapshot.

| Compression | Checksum | Time per commit | Data bytes per commit | Total bytes per commit | Bytes allocated | Allocations |
|---|---|---:|---:|---:|---:|---:|
| None | None | 13.9 µs | 315.9 | 1,696 | 9,670 | 101 |
| gzip | SHA-256 | 127.4 µs | 228.4 | 1,854 | 1,089,170 | 128 |
| zstd | SHA-256 | 22.0 µs | 224.1 | 1,852 | 16,620 | 108 |

For commits of one small event, zstd stores 29% fewer data bytes than
uncompressed JSONL, but the store holds 9% more bytes in total: the
manifests grow by about 248 bytes per commit, more than the
92 bytes that compression saves. Payloads that compress well shift that balance toward compression, by the
bytes that compression saves per commit.
The checksum gives each data object a verified digest at that cost.

Lode v0.18.2 reuses zstd encoders across streams. In this benchmark a zstd
commit took about 1.6 times as long as an uncompressed commit, and a gzip
commit took about 5.8 times as long as a zstd commit. The timings cover
local write costs for one small event per snapshot. Storage latency and
larger payloads can change the trade-off.
