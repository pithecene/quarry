package runtime

import (
	"bytes"
	"errors"
	"testing"

	lodepkg "github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"

	"github.com/pithecene/quarry/ipc"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/types"
)

// artifactStoreFixture is a Lode client over one memory store, as the CLI
// configures it for a run with storage.
type artifactStoreFixture struct {
	factory lodepkg.Factory[lodepkg.RangeStore]
	cfg     lode.Config
	client  *lode.LodeClient
}

func newArtifactStoreFixture(t *testing.T) *artifactStoreFixture {
	t.Helper()
	store := lodepkg.NewMemory()
	factory := func() (lodepkg.RangeStore, error) { return store, nil }
	cfg := lode.Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "run-123", Policy: "strict"}
	client, err := lode.NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return &artifactStoreFixture{factory: factory, cfg: cfg, client: client}
}

// volumeRemoved reports whether the artifact's Volume is removed.
func (f *artifactStoreFixture) volumeRemoved(t *testing.T, artifactID string) bool {
	t.Helper()
	vol, err := volume.New(volume.ID(artifactID), lodepkg.Prefixed(f.factory, lode.RunArtifactDir(f.cfg)), 1)
	if err != nil {
		t.Fatalf("volume.New: %v", err)
	}
	_, err = vol.Latest(t.Context())
	return errors.Is(err, volume.ErrRemoved)
}

func openFrame(id string, size int64) *types.ArtifactOpenFrame {
	return &types.ArtifactOpenFrame{Type: ipc.ArtifactOpenType, ArtifactID: id, SizeBytes: size}
}

func rangeFrame(id string, rangeID uint32, offset int64, data []byte) *types.ArtifactRangeFrame {
	return &types.ArtifactRangeFrame{Type: ipc.ArtifactRangeType, ArtifactID: id, RangeID: rangeID, Offset: offset, Data: data}
}

func TestArtifactManager_OpenRangesCommit(t *testing.T) {
	t.Parallel()
	f := newArtifactStoreFixture(t)
	m := NewArtifactManager(f.client)
	ctx := t.Context()
	if err := m.Open(ctx, openFrame("a1", 10)); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := m.WriteRange(ctx, rangeFrame("a1", 1, 5, []byte("56789"))); err != nil {
		t.Fatalf("WriteRange(5): %v", err)
	}
	if err := m.WriteRange(ctx, rangeFrame("a1", 2, 0, []byte("01234"))); err != nil {
		t.Fatalf("WriteRange(0): %v", err)
	}
	storage, err := m.Commit(ctx, "a1", 10)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if storage.Kind != types.ArtifactStorageVolume || storage.VolumeSnapshotID == "" || storage.SHA256 == "" {
		t.Fatalf("Commit = %+v; want a volume with its snapshot and digest", storage)
	}
	want := ArtifactStats{TotalArtifacts: 1, CommittedArtifacts: 1, TotalRanges: 2, TotalBytes: 10}
	if got := m.Stats(); got != want {
		t.Fatalf("Stats = %+v; want %+v", got, want)
	}
	if ids := m.OrphanIDs(); len(ids) != 0 {
		t.Fatalf("OrphanIDs = %v; want none", ids)
	}
	if err := m.Open(ctx, openFrame("a1", 10)); !errors.Is(err, ErrArtifactReopened) {
		t.Fatalf("Open after commit = %v; want ErrArtifactReopened", err)
	}
}

func TestArtifactManager_OpenErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		noStore bool
		frame   *types.ArtifactOpenFrame
		want    error // nil: any error
	}{
		{"no store", true, openFrame("a1", 1), ErrNoArtifactStore},
		{"over the maximum", false, openFrame("a1", MaxArtifactSize+1), nil},
		{"invalid id", false, openFrame("a/b", 1), lode.ErrInvalidArtifactID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var store lode.ArtifactStore
			if !tc.noStore {
				store = newArtifactStoreFixture(t).client
			}
			m := NewArtifactManager(store)
			err := m.Open(t.Context(), tc.frame)
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("Open = %v; want %v", err, tc.want)
			}
		})
	}
}

func TestArtifactManager_Reopen(t *testing.T) {
	t.Parallel()
	f := newArtifactStoreFixture(t)
	m := NewArtifactManager(f.client)
	if err := m.Open(t.Context(), openFrame("a1", 1)); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := m.Open(t.Context(), openFrame("a1", 1)); !errors.Is(err, ErrArtifactReopened) {
		t.Fatalf("second Open = %v; want ErrArtifactReopened", err)
	}
}

func TestArtifactManager_WriteRangeErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		frame *types.ArtifactRangeFrame
		want  error
	}{
		{"artifact not open", rangeFrame("nope", 1, 0, []byte("x")), ErrArtifactNotOpen},
		{"range over the maximum", rangeFrame("a1", 1, 0, bytes.Repeat([]byte("x"), ipc.MaxChunkSize+1)), lode.ErrRangeRefused},
		{"range past the size", rangeFrame("a1", 1, 8, []byte("xxx")), lode.ErrRangeRefused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newArtifactStoreFixture(t)
			m := NewArtifactManager(f.client)
			if err := m.Open(t.Context(), openFrame("a1", 10)); err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := m.WriteRange(t.Context(), tc.frame); !errors.Is(err, tc.want) {
				t.Fatalf("WriteRange = %v; want %v", err, tc.want)
			}
			if got := m.Stats(); got.TotalRanges != 0 || got.TotalBytes != 0 {
				t.Fatalf("Stats after a refused range = %+v; want no range counted", got)
			}
		})
	}
}

func TestArtifactManager_CommitErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		ranges []*types.ArtifactRangeFrame
		id     string
		size   int64
		want   error
	}{
		{"artifact not open", nil, "nope", 10, ErrArtifactNotOpen},
		{"size differs from the open frame", nil, "a1", 11, ErrArtifactSizeMismatch},
		{"ranges do not cover", []*types.ArtifactRangeFrame{rangeFrame("a1", 1, 0, []byte("01234"))}, "a1", 10, lode.ErrArtifactNotCovered},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newArtifactStoreFixture(t)
			m := NewArtifactManager(f.client)
			if err := m.Open(t.Context(), openFrame("a1", 10)); err != nil {
				t.Fatalf("Open: %v", err)
			}
			for _, r := range tc.ranges {
				if err := m.WriteRange(t.Context(), r); err != nil {
					t.Fatalf("WriteRange: %v", err)
				}
			}
			if _, err := m.Commit(t.Context(), tc.id, tc.size); !errors.Is(err, tc.want) {
				t.Fatalf("Commit = %v; want %v", err, tc.want)
			}
		})
	}
}

// At the run's end, every artifact opened without an artifact event loses
// its Volume, and the run reports it as an orphan.
func TestArtifactManager_AbandonUnfinished(t *testing.T) {
	t.Parallel()
	f := newArtifactStoreFixture(t)
	m := NewArtifactManager(f.client)
	ctx := t.Context()
	for _, id := range []string{"b-open", "a-open", "c-done"} {
		if err := m.Open(ctx, openFrame(id, 2)); err != nil {
			t.Fatalf("Open(%s): %v", id, err)
		}
		if err := m.WriteRange(ctx, rangeFrame(id, 1, 0, []byte("xy"))); err != nil {
			t.Fatalf("WriteRange(%s): %v", id, err)
		}
	}
	if _, err := m.Commit(ctx, "c-done", 2); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	ids, err := m.AbandonUnfinished(ctx)
	if err != nil {
		t.Fatalf("AbandonUnfinished: %v", err)
	}
	if len(ids) != 2 || ids[0] != "a-open" || ids[1] != "b-open" {
		t.Fatalf("AbandonUnfinished = %v; want [a-open b-open]", ids)
	}
	for _, id := range ids {
		if !f.volumeRemoved(t, id) {
			t.Fatalf("volume of %s is not removed", id)
		}
	}
	if f.volumeRemoved(t, "c-done") {
		t.Fatal("the committed artifact's volume is removed")
	}
	if got := m.OrphanIDs(); len(got) != 2 {
		t.Fatalf("OrphanIDs = %v; want the two abandoned", got)
	}
	want := ArtifactStats{TotalArtifacts: 3, CommittedArtifacts: 1, OrphanedArtifacts: 2, TotalRanges: 3, TotalBytes: 6}
	if got := m.Stats(); got != want {
		t.Fatalf("Stats = %+v; want %+v", got, want)
	}
	if ids, err := m.AbandonUnfinished(ctx); err != nil || len(ids) != 0 {
		t.Fatalf("second AbandonUnfinished = %v, %v; want nothing left", ids, err)
	}
}
