package retention

import (
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/pithecene/lode/lode"
)

// read returns the content of path through store, or "" and false when it
// is missing.
func read(t *testing.T, store lode.RangeStore, path string) (string, bool) {
	t.Helper()
	rc, err := store.Get(t.Context(), path)
	if errors.Is(err, lode.ErrNotFound) {
		return "", false
	}
	if err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data), true
}

func put(t *testing.T, store lode.RangeStore, path, content string) {
	t.Helper()
	if err := store.Put(t.Context(), path, strings.NewReader(content)); err != nil {
		t.Fatalf("put %s: %v", path, err)
	}
}

func TestSandboxStore_KeepsTheRealStoreUnchanged(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	real := lode.NewMemory()
	put(t, real, "a/kept", "1")
	put(t, real, "a/gone", "2")
	put(t, real, "a/ptr", "old")
	sandbox := newSandboxStore(real)

	put(t, sandbox, "a/new", "3")
	if err := sandbox.Put(ctx, "a/kept", strings.NewReader("x")); !errors.Is(err, lode.ErrPathExists) {
		t.Fatalf("Put over an existing path = %v, want ErrPathExists", err)
	}
	if err := sandbox.Delete(ctx, "a/gone"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := sandbox.CompareAndSwap(ctx, "a/ptr", "stale", "x"); !errors.Is(err, lode.ErrSnapshotConflict) {
		t.Fatalf("swap against stale content = %v, want ErrSnapshotConflict", err)
	}
	if err := sandbox.CompareAndSwap(ctx, "a/ptr", "old", "new"); err != nil {
		t.Fatalf("swap: %v", err)
	}
	if err := sandbox.CompareAndSwap(ctx, "a/created", "", "c"); err != nil {
		t.Fatalf("swap creating a path: %v", err)
	}
	if err := sandbox.CompareAndSwap(ctx, "a/missing", "x", "y"); !errors.Is(err, lode.ErrSnapshotConflict) {
		t.Fatalf("swap on a missing path = %v, want ErrSnapshotConflict", err)
	}

	// The sandbox sees its own changes.
	listed, err := sandbox.List(ctx, "a/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	slices.Sort(listed)
	if want := []string{"a/created", "a/kept", "a/new", "a/ptr"}; !slices.Equal(listed, want) {
		t.Fatalf("sandbox list = %v, want %v", listed, want)
	}
	if got, _ := read(t, sandbox, "a/ptr"); got != "new" {
		t.Fatalf("sandbox pointer = %q, want new", got)
	}
	if _, ok := read(t, sandbox, "a/gone"); ok {
		t.Fatal("a deleted path is readable through the sandbox")
	}
	if got, err := sandbox.ReadRange(ctx, "a/new", 0, 1); err != nil || string(got) != "3" {
		t.Fatalf("ReadRange = %q, %v", got, err)
	}
	if got, want := sandbox.Swaps(), []swap{{path: "a/ptr", expected: "old"}, {path: "a/created", expected: ""}}; !slices.Equal(got, want) {
		t.Fatalf("swaps = %v, want %v", got, want)
	}

	// The real store is as it was.
	listed, err = real.List(ctx, "a/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	slices.Sort(listed)
	if want := []string{"a/gone", "a/kept", "a/ptr"}; !slices.Equal(listed, want) {
		t.Fatalf("real list = %v, want %v", listed, want)
	}
	if got, _ := read(t, real, "a/ptr"); got != "old" {
		t.Fatalf("real pointer = %q, want old", got)
	}
}

func TestPinnedStore_AllowsOnlyThePinnedSwap(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	real := lode.NewMemory()
	put(t, real, "ptr", "head-1")
	pinned := &pinnedStore{RangeStore: real, pins: []swap{{path: "ptr", expected: "head-1"}}}

	if err := pinned.CompareAndSwap(ctx, "ptr", "head-2", "x"); !errors.Is(err, lode.ErrSnapshotConflict) {
		t.Fatalf("unpinned expectation = %v, want ErrSnapshotConflict", err)
	}
	if err := pinned.CompareAndSwap(ctx, "other", "", "x"); !errors.Is(err, lode.ErrSnapshotConflict) {
		t.Fatalf("unpinned path = %v, want ErrSnapshotConflict", err)
	}
	if err := pinned.CompareAndSwap(ctx, "ptr", "head-1", "removed"); err != nil {
		t.Fatalf("pinned swap: %v", err)
	}
	if got, _ := read(t, real, "ptr"); got != "removed" {
		t.Fatalf("pointer = %q, want removed", got)
	}
	if _, ok := read(t, real, "other"); ok {
		t.Fatal("a refused swap created its path")
	}
}
