package lode_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
)

type failingHeadStore struct {
	lode.Store
	err  error
	gets int
}

func (s *failingHeadStore) Get(context.Context, string) (io.ReadCloser, error) {
	s.gets++
	return nil, s.err
}

func TestOpenDataset_ValidatesOptionsBeforeReadingHead(t *testing.T) {
	t.Parallel()
	readErr := errors.New("head unavailable")
	cases := []struct {
		name     string
		option   lode.DatasetOption
		wantErr  error
		wantGets int
	}{
		{"negative retry count", lode.WithRetry(-1), lode.ErrInvalidArgument, 0},
		{"negative base delay", lode.WithRetryBaseDelay(-1), lode.ErrInvalidArgument, 0},
		{"negative max delay", lode.WithRetryMaxDelay(-1), lode.ErrInvalidArgument, 0},
		{"out-of-range jitter", lode.WithRetryJitter(2), lode.ErrInvalidArgument, 0},
		{"nil option", nil, lode.ErrInvalidArgument, 0},
		{"valid retry count", lode.WithRetry(1), readErr, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &failingHeadStore{Store: lode.NewMemory(), err: readErr}
			if _, err := qlode.OpenDataset(t.Context(), "quarry", store, tc.option); !errors.Is(err, tc.wantErr) {
				t.Errorf("open dataset: got %v, want %v", err, tc.wantErr)
			}
			if store.gets != tc.wantGets {
				t.Errorf("store Gets = %d, want %d", store.gets, tc.wantGets)
			}
		})
	}
}

// settingsRecord is one item event of run r1, in the partition that every
// test here reads.
func settingsRecord(seq int) lode.D {
	return lode.D{"source": "src", "category": "cat", "day": artifactTestDay, "run_id": "r1", "event_type": "item", "seq": seq}
}

// writeAndReadHead writes one record through OpenDataset and returns the
// head manifest and a view of the dataset.
func writeAndReadHead(t *testing.T, store lode.Store, opts ...lode.DatasetOption) (*lode.Manifest, *lode.RecordView[lode.D]) {
	t.Helper()
	ctx := t.Context()
	ds, err := qlode.OpenDataset(ctx, "quarry", store, opts...)
	if err != nil {
		t.Fatalf("open dataset: %v", err)
	}
	if _, err := ds.Write(ctx, []lode.D{settingsRecord(1)}, nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	view, err := qlode.OpenView(ctx, "quarry", store)
	if err != nil {
		t.Fatalf("open view: %v", err)
	}
	head, err := view.Latest(ctx)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	return head.Manifest, view
}

// A dataset without snapshots is created with Quarry's layout, zstd, and
// SHA-256: each data object carries the zstd extension and a digest, and
// reads back.
func TestOpenDataset_NewDatasetUsesZstdAndSHA256(t *testing.T) {
	t.Parallel()
	m, view := writeAndReadHead(t, lode.NewMemory())
	if m.Compressor != "zstd" || m.ChecksumAlgorithm != "sha256" {
		t.Fatalf("head records compressor %q and checksum %q, want zstd and sha256", m.Compressor, m.ChecksumAlgorithm)
	}
	if len(m.Objects) != 1 {
		t.Fatalf("head has %d objects, want 1", len(m.Objects))
	}
	obj := m.Objects[0]
	if !strings.HasSuffix(obj.Path, ".zst") || obj.Checksum == "" {
		t.Errorf("object %s has checksum %q, want a .zst object with a digest", obj.Path, obj.Checksum)
	}
	recs := readFirstPartition(t, view)
	if len(recs) != 1 || recs[0]["seq"] != float64(1) {
		t.Errorf("read %v, want the one record written", recs)
	}
}

// A dataset that an earlier release created uncompressed and without
// checksums keeps those settings, so its writers stay compatible with the
// head (Lode rejects a writer whose settings differ).
func TestOpenDataset_ExistingDatasetKeepsItsSettings(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := lode.NewMemory()
	legacy, err := lode.Open(lode.Use(store)).Records(ctx, "quarry", lode.JSONL[lode.D](),
		lode.WithLayout(lode.Hive("source", "category", "day", "run_id", "event_type")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Write(ctx, []lode.D{settingsRecord(0)}, nil); err != nil {
		t.Fatal(err)
	}
	m, view := writeAndReadHead(t, store)
	if m.Compressor != "noop" || m.ChecksumAlgorithm != "" {
		t.Fatalf("head records compressor %q and checksum %q, want noop and none", m.Compressor, m.ChecksumAlgorithm)
	}
	if recs := readFirstPartition(t, view); len(recs) != 2 {
		t.Errorf("read %d records, want 2", len(recs))
	}
}

type namedCompressor struct {
	lode.Compressor
	name string
}

func (c namedCompressor) Name() string { return c.name }

type namedChecksum struct {
	lode.Checksum
	name string
}

func (c namedChecksum) Name() string { return c.name }

// A head that records other bundled components keeps them: Lode reconstructs
// each by the name its manifest records, and the new objects read back.
func TestOpenDataset_UsesRecordedImplementations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		compressor lode.Compressor
		checksum   lode.Checksum
	}{
		{"zstd without a checksum", lode.Zstd(), nil},
		{"gzip with CRC32C", lode.Gzip(), lode.CRC32C()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := lode.NewMemory()
			head, err := lode.Open(lode.Use(store)).Records(ctx, "quarry", lode.JSONL[lode.D](),
				lode.WithLayout(lode.Hive("source", "category", "day", "run_id", "event_type")),
				lode.WithCompressor(tc.compressor), lode.WithChecksum(tc.checksum))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := head.Write(ctx, []lode.D{settingsRecord(0)}, nil); err != nil {
				t.Fatal(err)
			}
			m, view := writeAndReadHead(t, store)
			if m.Compressor != tc.compressor.Name() {
				t.Errorf("head records compressor %q, want %q", m.Compressor, tc.compressor.Name())
			}
			if got := m.Objects[0].Checksum != ""; got != (tc.checksum != nil) {
				t.Errorf("new object has a digest: %t, want %t", got, tc.checksum != nil)
			}
			if recs := readFirstPartition(t, view); len(recs) != 2 {
				t.Errorf("read %d records, want 2", len(recs))
			}
		})
	}
}

// A stored-byte mismatch takes precedence even when the JSONL decoder
// stops before Lode reaches the end of the stream.
func TestReadPartitionRecords_ChecksumMismatchFailsTheRead(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		payload string
	}{
		{"valid JSONL", "{\"seq\":2}\n"},
		{"invalid JSONL before EOF", "!\n" + strings.Repeat("x", 4096)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := lode.NewMemory()
			m, view := writeAndReadHead(t, store)
			obj := m.Objects[0]

			var replaced bytes.Buffer
			w, err := lode.Zstd().Compress(&replaced)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(w, tc.payload); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if err := store.Delete(ctx, obj.Path); err != nil {
				t.Fatal(err)
			}
			if err := store.Put(ctx, obj.Path, &replaced); err != nil {
				t.Fatal(err)
			}

			parts, err := view.ListPartitions(ctx, lode.PartitionListOptions{})
			if err != nil || len(parts) != 1 {
				t.Fatalf("partitions %v, %v; want one", parts, err)
			}
			if _, err := qlode.ReadPartitionRecords(ctx, view, parts[0]); !errors.Is(err, lode.ErrChecksumMismatch) {
				t.Fatalf("read of a replaced object = %v, want ErrChecksumMismatch", err)
			}
		})
	}
}

// readFirstPartition reads every record of the view's only partition.
func readFirstPartition(t *testing.T, view *lode.RecordView[lode.D]) []lode.D {
	t.Helper()
	parts, err := view.ListPartitions(t.Context(), lode.PartitionListOptions{})
	if err != nil || len(parts) != 1 {
		t.Fatalf("partitions %v, %v; want one", parts, err)
	}
	recs, err := qlode.ReadPartitionRecords(t.Context(), view, parts[0])
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return recs
}

// manifestFailingStore fails every write of a canonical manifest, as a
// process that stops after advancing the latest pointer leaves the dataset.
type manifestFailingStore struct{ lode.Store }

func (s manifestFailingStore) Put(ctx context.Context, key string, r io.Reader) error {
	if strings.HasSuffix(key, "/manifest.json") {
		return errors.New("injected: manifest write lost")
	}
	return s.Store.Put(ctx, key, r)
}

// A first commit that staged its manifest and advanced the pointer, then
// stopped before publishing the canonical manifest, leaves a pending head
// with the settings of its writer. Quarry adopts those settings, as Lode's
// writer does, and appends to the dataset.
func TestOpenDataset_PendingFirstHeadKeepsItsSettings(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := lode.NewMemory()
	legacy, err := lode.Open(lode.Use(manifestFailingStore{store})).Records(ctx, "quarry", lode.JSONL[lode.D](),
		lode.WithLayout(lode.Hive("source", "category", "day", "run_id", "event_type")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Write(ctx, []lode.D{settingsRecord(0)}, nil); err == nil {
		t.Fatal("first write succeeded, want the injected manifest failure")
	}

	ds, err := qlode.OpenDataset(ctx, "quarry", store)
	if err != nil {
		t.Fatalf("reopen after the interrupted first commit: %v", err)
	}
	if _, err := ds.Write(ctx, []lode.D{settingsRecord(1)}, nil); err != nil {
		t.Fatalf("append after the interrupted first commit: %v", err)
	}
}

// A head that records a compressor or checksum without a bundled match
// cannot be extended, so the writer is refused when it opens.
func TestOpenDataset_UnbundledRecordedComponentIsASettingsMismatch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		opts []lode.DatasetOption
	}{
		{"unknown compressor", []lode.DatasetOption{lode.WithCompressor(namedCompressor{lode.Gzip(), "brotli"})}},
		{"unknown checksum", []lode.DatasetOption{lode.WithChecksum(namedChecksum{lode.MD5(), "blake3"})}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := lode.NewMemory()
			opts := append([]lode.DatasetOption{lode.WithLayout(lode.Hive("source", "category", "day", "run_id", "event_type"))}, tc.opts...)
			head, err := lode.Open(lode.Use(store)).Records(ctx, "quarry", lode.JSONL[lode.D](), opts...)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := head.Write(ctx, []lode.D{settingsRecord(0)}, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := qlode.OpenDataset(ctx, "quarry", store); !errors.Is(err, lode.ErrSettingsMismatch) {
				t.Fatalf("OpenDataset = %v, want ErrSettingsMismatch", err)
			}
		})
	}
}
