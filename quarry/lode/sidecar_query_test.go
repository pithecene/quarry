package lode

import (
	"reflect"
	"strings"
	"testing"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/types"
)

// A run's sidecar files are those of its own snapshots, in the order it wrote
// them, also when Lode escapes its run ID.
func TestQueryRunDetails_Sidecars(t *testing.T) {
	t.Parallel()
	factory := sharedMemoryFactory()
	key := func(runID string) Config {
		return Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-03-22", RunID: runID, Policy: "strict"}
	}
	// Each batch of files is recorded by the snapshot of one event.
	write := func(runID string, batches ...[]string) {
		t.Helper()
		client, err := NewLodeClientWithFactory(t.Context(), key(runID), factory)
		if err != nil {
			t.Fatal(err)
		}
		for i, batch := range batches {
			for _, name := range batch {
				if err := client.PutFile(t.Context(), name, "text/html", []byte(name)); err != nil {
					t.Fatal(err)
				}
			}
			event := &types.EventEnvelope{Type: types.EventTypeItem, Seq: int64(i + 1), RunID: runID, Payload: map[string]any{}}
			if err := client.WriteEvents(t.Context(), "quarry", runID, []*types.EventEnvelope{event}); err != nil {
				t.Fatal(err)
			}
		}
	}
	write("run-A", []string{"1.html", "2.html"}, []string{"3.html"})
	write("run-B", []string{"b.html"})
	write("run 001", []string{"escaped.html"})

	for _, tc := range []struct {
		runID string
		names []string
	}{
		{"run-A", []string{"1.html", "2.html", "3.html"}},
		{"run-B", []string{"b.html"}},
		{"run 001", []string{"escaped.html"}},
		{"run-none", nil},
	} {
		t.Run(tc.runID, func(t *testing.T) {
			var want []SidecarFileRef
			for _, name := range tc.names {
				want = append(want, SidecarFileRef{Path: SidecarFilesPrefix(key(tc.runID)) + name, Filename: name, ContentType: "text/html", Size: int64(len(name))})
			}
			if got := sidecarsOf(t, factory, key(tc.runID)); len(got) != len(want) || (len(want) > 0 && !reflect.DeepEqual(got, want)) {
				t.Errorf("sidecar files = %+v, want %+v", got, want)
			}
		})
	}
	if got := sidecarsOf(t, sharedMemoryFactory(), key("run-A")); got != nil {
		t.Errorf("sidecar files of an empty store = %+v, want nil", got)
	}
}

// A malformed sidecar inventory fails the query with an error that names
// its snapshot, rather than being skipped.
func TestQueryRunDetails_SidecarsMalformedInventoryFails(t *testing.T) {
	t.Parallel()
	store := lode.NewMemory()
	ctx := t.Context()
	ds, err := OpenDataset(ctx, "quarry", store)
	if err != nil {
		t.Fatal(err)
	}
	record := lode.D{"source": "src", "category": "cat", "day": "2026-03-22", "run_id": "run-bad", "event_type": "item", "seq": 1}
	snap, err := ds.Write(ctx, []lode.D{record}, map[string]any{MetadataKeySidecarFiles: "invalid"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = QueryRunDetails(ctx, sharedFactory(store), Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-03-22", RunID: "run-bad"})
	if err == nil || !strings.Contains(err.Error(), string(snap.ID)) {
		t.Fatalf("QueryRunDetails = %v, want an error that names snapshot %s", err, snap.ID)
	}
}
