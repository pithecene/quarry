package lode

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/iox"
)

// partitionKeys is the Hive layout every Quarry dataset is written with.
var partitionKeys = []string{"source", "category", "day", "run_id", "event_type"}

// recordCodec is the codec every Quarry dataset is written with. Its records
// are maps, so the Hive layout reads partition values from their keys.
func recordCodec() lode.Codec[lode.D] { return lode.JSONL[lode.D]() }

// layout is the Hive layout over partitionKeys, which every Quarry writer
// passes and every Quarry view expects.
func layout() lode.ComponentOption { return lode.WithLayout(lode.Hive(partitionKeys...)) }

// OpenDataset opens the dataset over store for writing with the settings
// every Quarry writer shares (CONTRACT_LODE.md §Writer Settings): the Hive
// layout, the JSONL codec, bounded commit retry, and the compressor and
// checksum that the head records, published or staged, or zstd and SHA-256
// for a dataset without a head. A layout, compressor, or checksum option fails
// with lode.ErrInvalidArgument, and a head that records a component Lode does
// not bundle fails with lode.ErrSettingsMismatch.
// Opening reads the head and writes nothing.
func OpenDataset(ctx context.Context, dataset string, store lode.Store, opts ...lode.DatasetOption) (*lode.Records[lode.D], error) {
	settings, err := writerOptions(opts)
	if err != nil {
		return nil, err
	}
	return lode.Open(lode.Use(store)).Records(ctx, lode.DatasetID(dataset), recordCodec(), settings...)
}

// OpenView opens a read-only view of the dataset over store, so reads come
// from exactly that store. The view expects the Hive layout that Quarry
// writers use, so it reads only that layout's manifest directory. It returns
// lode.ErrNotFound when the dataset has no snapshot and
// lode.ErrSettingsMismatch when the dataset records another layout.
func OpenView(ctx context.Context, dataset string, store lode.ObjectStore) (*lode.RecordView[lode.D], error) {
	return lode.OpenReadOnly(lode.Use(store)).Records(ctx, lode.DatasetID(dataset), recordCodec(), layout())
}

// OpenViewAt opens a read-only view of the dataset over store at one
// snapshot, which it reads in place of the head (Lode's WithAnchor). The
// view expects the Hive layout, so opening reads the snapshot's canonical
// manifest with one store request after Lode's removal fence check. The
// view's Snapshot of that ID reuses that manifest after it checks the fence
// and the manifest's existence again. It returns lode.ErrNotFound when the
// snapshot does not exist, lode.ErrSnapshotExpired when retention expired it,
// lode.ErrInvalidArgument when the ID is not one path segment, and
// lode.ErrSettingsMismatch when the snapshot records another layout.
func OpenViewAt(ctx context.Context, dataset string, store lode.ObjectStore, snapshot lode.SnapshotID) (*lode.RecordView[lode.D], error) {
	return lode.OpenReadOnly(lode.Use(store)).Records(ctx, lode.DatasetID(dataset), recordCodec(), layout(), lode.WithAnchor(snapshot))
}

// ReadPartitionRecords returns every record stored in one partition, across
// all of its committed snapshots, indexed there or not, each data file read
// once.
func ReadPartitionRecords(ctx context.Context, ds *lode.RecordView[lode.D], partition lode.PartitionPath) ([]lode.D, error) {
	prefix := partition.String() + "/"
	snapshots, err := RunSnapshots(ctx, ds, prefix)
	if err != nil {
		return nil, err
	}
	return ReadSnapshotRecords(ctx, ds, snapshots, prefix)
}

// ReadSnapshotPartitionRecords returns the records that one committed
// snapshot stored in one partition. It reads the snapshot's canonical
// manifest, so a snapshot that retention expired, whose partition manifest
// may remain as a residual object, is never served (Lode
// CONTRACT_RETENTION §Expired Snapshot). It returns lode.ErrNotFound or
// lode.ErrSnapshotExpired, wrapped, when the snapshot is gone, and
// lode.ErrNotFound when one of its data objects is.
func ReadSnapshotPartitionRecords(ctx context.Context, ds *lode.RecordView[lode.D], snapshot lode.SnapshotID, partition lode.PartitionPath) ([]lode.D, error) {
	snap, err := ds.Snapshot(ctx, snapshot)
	if err != nil {
		return nil, WrapReadError(err, partition.String())
	}
	return readManifestRecords(ctx, ds, snap.Manifest, partition.String()+"/", map[string]bool{})
}

// readManifestRecords reads the records of the manifest's objects under
// prefix, skipping an object that seen holds and adding each object it
// reads.
func readManifestRecords(ctx context.Context, ds *lode.RecordView[lode.D], m *lode.Manifest, prefix string, seen map[string]bool) ([]lode.D, error) {
	var out []lode.D
	for _, obj := range m.Objects {
		if seen[obj.Path] || !underPrefix(obj.Path, prefix) {
			continue
		}
		seen[obj.Path] = true
		recs, err := readObject(ctx, ds, m, obj)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

// readObject decodes one object of manifest m. OpenDecompressed checks that
// the object belongs to m's snapshot and applies m's compressor and checksum.
func readObject(ctx context.Context, ds *lode.RecordView[lode.D], m *lode.Manifest, ref lode.ObjectRef) ([]lode.D, error) {
	rc, err := ds.OpenDecompressed(ctx, m, ref)
	if err != nil {
		return nil, WrapReadError(err, ref.Path)
	}
	defer iox.DiscardClose(rc)
	recs, err := recordCodec().Decode(rc)
	if err == nil {
		return recs, nil
	}
	if m.ChecksumAlgorithm != "" && !errors.Is(err, lode.ErrChecksumMismatch) {
		// A decode error can stop before EOF. Give Lode a chance to verify
		// the recorded digest, keeping the decode error unless it finds a mismatch.
		if _, verifyErr := io.Copy(io.Discard, rc); errors.Is(verifyErr, lode.ErrChecksumMismatch) {
			err = verifyErr
		}
	}
	return nil, fmt.Errorf("decode %s: %w", ref.Path, err)
}

// datasetRetryCount bounds the commit retries of every Quarry writer, the
// dataset's and each artifact Volume's alike.
const datasetRetryCount = 3
