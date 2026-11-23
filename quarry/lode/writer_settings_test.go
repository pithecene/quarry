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

// OpenDataset refuses an invalid option, and any layout, compressor, or checksum
// option since every Quarry writer shares those, before it reads the head.
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
		{"another partition layout", lode.WithLayout(lode.Hive("day")), lode.ErrInvalidArgument, 0},
		{"a compressor", lode.WithCompressor(lode.Gzip()), lode.ErrInvalidArgument, 0},
		{"a checksum", lode.WithChecksum(lode.CRC32C()), lode.ErrInvalidArgument, 0},
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
func writeAndReadHead(t *testing.T, store lode.Store) (*lode.Manifest, *lode.RecordView[lode.D]) {
	t.Helper()
	ctx := t.Context()
	ds, err := qlode.OpenDataset(ctx, "quarry", store)
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

// seedHead commits one record through a plain Lode writer with Quarry's
// layout and opts. A pending seed loses its canonical manifest after it
// advances the latest pointer, as a process that stops there leaves a first
// commit: the head is staged, not published.
func seedHead(t *testing.T, store lode.Store, pending bool, opts []lode.DatasetOption) {
	t.Helper()
	ctx := t.Context()
	seedStore := store
	if pending {
		seedStore = manifestFailingStore{store}
	}
	opts = append([]lode.DatasetOption{lode.WithLayout(lode.Hive("source", "category", "day", "run_id", "event_type"))}, opts...)
	seed, err := lode.Open(lode.Use(seedStore)).Records(ctx, "quarry", lode.JSONL[lode.D](), opts...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Write(ctx, []lode.D{settingsRecord(0)}, nil); (err != nil) != pending {
		t.Fatalf("seed write: err = %v, want an error only for a pending seed", err)
	}
}

// A dataset whose head records bundled components keeps them, whether an
// earlier release published that head or an interrupted first commit staged
// it: Lode reconstructs each component by its recorded name, and Quarry's
// commit appends to the dataset. A staged head's commit is abandoned, so its
// record is not in the next snapshot (Lode's CONTRACT_CORE.md, pending commits).
func TestOpenDataset_ExistingHeadKeepsItsComponents(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		pending    bool
		opts       []lode.DatasetOption
		compressor string
		checksum   string
		records    int
	}{
		{"uncompressed without a checksum", false, nil, "noop", "", 2},
		{"zstd without a checksum", false, []lode.DatasetOption{lode.WithCompressor(lode.Zstd())}, "zstd", "", 2},
		{"gzip with CRC32C", false, []lode.DatasetOption{lode.WithCompressor(lode.Gzip()), lode.WithChecksum(lode.CRC32C())}, "gzip", "crc32c", 2},
		{"a staged first head, uncompressed", true, nil, "noop", "", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := lode.NewMemory()
			seedHead(t, store, tc.pending, tc.opts)
			m, view := writeAndReadHead(t, store)
			if m.Compressor != tc.compressor || m.ChecksumAlgorithm != tc.checksum {
				t.Errorf("head records compressor %q and checksum %q, want %q and %q", m.Compressor, m.ChecksumAlgorithm, tc.compressor, tc.checksum)
			}
			for _, obj := range m.Objects {
				if got, want := obj.Checksum != "", tc.checksum != ""; got != want {
					t.Errorf("object %s has a digest: %t, want %t", obj.Path, got, want)
				}
			}
			if recs := readFirstPartition(t, view); len(recs) != tc.records {
				t.Errorf("read %d records, want %d", len(recs), tc.records)
			}
		})
	}
}
