package lode

import (
	"flag"
	"testing"

	"github.com/pithecene/lode/lode"
)

var (
	s3Endpoint = flag.String("s3-endpoint", "", "S3 endpoint of the dataset to check (scripts/ci/s3/concurrent.ts)")
	s3Path     = flag.String("s3-path", "", "bucket/prefix of that dataset")
)

// TestS3SnapshotsFormOneChain checks a dataset that concurrent runs wrote: its
// snapshots form one chain, which ends at the head.
func TestS3SnapshotsFormOneChain(t *testing.T) {
	if *s3Endpoint == "" {
		t.Skip("needs -s3-endpoint and -s3-path")
	}
	factory, err := NewStoreFactory("s3", *s3Path, S3Config{Region: "us-east-1", Endpoint: *s3Endpoint, UsePathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	ds, _, ok, err := openReadStore(t.Context(), "quarry", factory)
	if err != nil || !ok {
		t.Fatalf("open the dataset: found = %v, %v", ok, err)
	}
	snapshots, err := ds.Snapshots(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	parents := make(map[lode.SnapshotID]lode.SnapshotID, len(snapshots))
	for _, snap := range snapshots {
		parents[snap.ID] = snap.ParentSnapshotID
	}
	head, err := ds.Latest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[lode.SnapshotID]bool, len(parents))
	for id := head.ID; id != ""; id = parents[id] {
		if _, found := parents[id]; !found || seen[id] {
			t.Fatalf("the chain breaks at %s", id)
		}
		seen[id] = true
	}
	if len(seen) != len(parents) || len(parents) != len(snapshots) || len(parents) == 0 {
		t.Fatalf("%d of %d snapshots are in the chain, with %d distinct IDs", len(seen), len(snapshots), len(parents))
	}
}
