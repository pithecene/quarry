package lode

import "testing"

// A partition manifest key below a run's prefix parses into its partition and
// snapshot ID from its end, so a dataset or a value named "segments" earlier in
// the key cannot shadow the snapshot directory.
func TestPartitionManifest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		rel, partition string
		id             string
		ok             bool
	}{
		{"event_type=item/segments/123-ab/manifest.json", "event_type=item", "123-ab", true},
		{"event_type=segments/segments/123-ab/manifest.json", "event_type=segments", "123-ab", true},
		{"event_type=item/segments/123-ab/data/manifest.json", "", "", false},
		{"event_type=item/segments/123-ab/data.jsonl", "", "", false},
		{"segments/123-ab/manifest.json", "", "", false},
	} {
		partition, id, ok := partitionManifest(tc.rel)
		if ok != tc.ok || partition != tc.partition || string(id) != tc.id {
			t.Errorf("partitionManifest(%q) = %q, %q, %t; want %q, %q, %t", tc.rel, partition, id, ok, tc.partition, tc.id, tc.ok)
		}
	}
}
