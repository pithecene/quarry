package lode

import (
	"strings"

	"github.com/pithecene-io/lode/lode"
)

// NewReadDataset creates a Lode Dataset for reading.
// Uses the same codec and layout as the write path to ensure compatibility.
func NewReadDataset(dataset string, factory lode.StoreFactory) (lode.Dataset, error) {
	return lode.NewDataset(
		lode.DatasetID(dataset),
		factory,
		lode.WithHiveLayout("source", "category", "day", "run_id", "event_type"),
		lode.WithCodec(lode.NewJSONLCodec()),
	)
}

// NewReadDatasetFS creates a read Dataset with filesystem storage.
func NewReadDatasetFS(dataset, rootPath string) (lode.Dataset, error) {
	return NewReadDataset(dataset, lode.NewFSFactory(rootPath))
}

// NewReadDatasetS3 creates a read Dataset with S3 storage. The store is
// built exactly as on the write path, including Endpoint and UsePathStyle.
func NewReadDatasetS3(dataset string, s3cfg S3Config) (lode.Dataset, error) {
	factory, err := newS3StoreFactory(s3cfg)
	if err != nil {
		return nil, err
	}
	return NewReadDataset(dataset, factory)
}

// isMetricsSnapshot checks if a snapshot contains metrics data
// by examining file paths for the event_type=metrics partition.
func isMetricsSnapshot(snap *lode.DatasetSnapshot) bool {
	for _, f := range snap.Manifest.Files {
		if matchesPartitionValue(f.Path, "event_type", "metrics") {
			return true
		}
	}
	return false
}

// snapshotMatchesFilter checks if a snapshot's file paths match
// the given partition key=value filter.
func snapshotMatchesFilter(snap *lode.DatasetSnapshot, key, value string) bool {
	if value == "" {
		return true
	}
	for _, f := range snap.Manifest.Files {
		if matchesPartitionValue(f.Path, key, value) {
			return true
		}
	}
	return false
}

// matchesPartitionValue checks if a Hive-partitioned path contains an exact
// key=value segment. Segments are delimited by "/" in paths. This avoids
// substring false positives (e.g., run_id=run-1 matching run_id=run-10).
func matchesPartitionValue(path, key, value string) bool {
	segment := key + "=" + value
	// Split path into segments and match exactly
	for _, part := range strings.Split(path, "/") {
		if part == segment {
			return true
		}
	}
	return false
}
