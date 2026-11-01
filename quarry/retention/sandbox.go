package retention

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/pithecene/lode/lode"
)

// swap is one CompareAndSwap that Lode made: the path and the content it
// expected there. Retention never interprets either; it only repeats them.
type swap struct {
	path, expected string
}

// sandboxStore runs Lode's own code against a store without changing it.
// Reads go to the real store; writes, deletes, and swaps are kept in memory
// and seen by later reads through the sandbox, so Lode behaves as it would
// against the real store. A dry run removes Volumes through it, and the
// swaps it records pin the real removal (pinnedStore).
//
// Thread safety: safe for concurrent use; one sandbox serves one call.
type sandboxStore struct {
	real lode.RangeStore

	mu      sync.Mutex
	written map[string][]byte
	deleted map[string]bool
	swaps   []swap
}

func newSandboxStore(real lode.RangeStore) *sandboxStore {
	return &sandboxStore{real: real, written: map[string][]byte{}, deleted: map[string]bool{}}
}

// Changed reports whether anything was written, deleted, or swapped through
// the sandbox.
func (s *sandboxStore) Changed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.written) > 0 || len(s.deleted) > 0 || len(s.swaps) > 0
}

// Swaps returns the swaps recorded, in order.
func (s *sandboxStore) Swaps() []swap {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]swap(nil), s.swaps...)
}

// content returns the content of path as the sandbox sees it, and whether
// the path exists.
func (s *sandboxStore) content(ctx context.Context, path string) ([]byte, bool, error) {
	s.mu.Lock()
	data, written := s.written[path]
	deleted := s.deleted[path]
	s.mu.Unlock()
	switch {
	case written:
		return data, true, nil
	case deleted:
		return nil, false, nil
	}
	rc, err := s.real.Get(ctx, path)
	if errors.Is(err, lode.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	data, err = io.ReadAll(rc)
	if closeErr := rc.Close(); err == nil {
		err = closeErr
	}
	return data, err == nil, err
}

// Put creates path in the sandbox; it refuses an existing path, as every
// store does.
func (s *sandboxStore) Put(ctx context.Context, path string, r io.Reader) error {
	_, exists, err := s.content(ctx, path)
	if err != nil {
		return err
	}
	if exists {
		return lode.ErrPathExists
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("sandbox: put: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.written[path] = data
	delete(s.deleted, path)
	return nil
}

func (s *sandboxStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	data, exists, err := s.content(ctx, path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, lode.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *sandboxStore) Exists(ctx context.Context, path string) (bool, error) {
	_, exists, err := s.content(ctx, path)
	return exists, err
}

// List merges the real listing with the sandbox's writes and deletes.
func (s *sandboxStore) List(ctx context.Context, prefix string) ([]string, error) {
	listed, err := s.real.List(ctx, prefix)
	if err != nil && !errors.Is(err, lode.ErrNotFound) {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, path := range listed {
		if !s.deleted[path] && !seen[path] {
			seen[path] = true
			out = append(out, path)
		}
	}
	for path := range s.written {
		if strings.HasPrefix(path, prefix) && !seen[path] {
			seen[path] = true
			out = append(out, path)
		}
	}
	return out, nil
}

// Delete hides path from later reads; a missing path is not an error.
func (s *sandboxStore) Delete(_ context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.written, path)
	s.deleted[path] = true
	return nil
}

// CompareAndSwap follows the store contract against the sandbox's view and
// records the swap when it succeeds.
func (s *sandboxStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	data, exists, err := s.content(ctx, path)
	if err != nil {
		return err
	}
	if (exists && string(data) != expected) || (!exists && expected != "") {
		return lode.ErrSnapshotConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.written[path] = []byte(replacement)
	delete(s.deleted, path)
	s.swaps = append(s.swaps, swap{path: path, expected: expected})
	return nil
}

func (s *sandboxStore) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	data, exists, err := s.content(ctx, path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, lode.ErrNotFound
	}
	if offset < 0 || length < 0 || offset > int64(len(data)) || length > int64(len(data))-offset {
		return nil, fmt.Errorf("sandbox: range [%d, %d) outside %d bytes: %w", offset, offset+length, len(data), lode.ErrInvalidArgument)
	}
	return data[offset : offset+length], nil
}

func (s *sandboxStore) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	data, exists, err := s.content(ctx, path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, lode.ErrNotFound
	}
	return bytes.NewReader(data), nil
}

// pinnedStore passes every call to the real store but allows only the swaps
// a sandbox run recorded, each against the content Lode expected then. Any
// other swap fails with lode.ErrSnapshotConflict. The real store compares
// atomically, so a removal through it fences only the head that the dry
// run judged: a Volume that committed since then is refused, never removed.
//
// Thread safety: immutable after construction; safe for concurrent use.
type pinnedStore struct {
	lode.RangeStore
	pins []swap
}

func (s *pinnedStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	for _, pin := range s.pins {
		if pin.path == path && pin.expected == expected {
			return s.RangeStore.CompareAndSwap(ctx, path, expected, replacement)
		}
	}
	return lode.ErrSnapshotConflict
}
