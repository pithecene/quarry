package lode

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

// =============================================================================
// Phase 8 + Phase 10: Storage Failure Hardening Tests (Typed Assertions)
// =============================================================================

// FailingStore is a lode.RangeStore that returns configurable errors.
type FailingStore struct {
	PutErr    error
	GetErr    error
	ExistsErr error
	ListErr   error
	DeleteErr error

	// Track calls for verification
	PutCalls    int
	PutPaths    []string
	CloseCalled bool
}

func (s *FailingStore) Put(_ context.Context, path string, _ io.Reader) error {
	s.PutCalls++
	s.PutPaths = append(s.PutPaths, path)
	return s.PutErr
}

func (s *FailingStore) Get(_ context.Context, _ string) (io.ReadCloser, error) {
	if s.GetErr != nil {
		return nil, s.GetErr
	}
	// FailingStore never stores data; return not-found so callers
	// (e.g. Lode's readLatestPointer) handle the absence gracefully
	// instead of receiving a nil reader with a nil error.
	return nil, lode.ErrNotFound
}

func (s *FailingStore) Exists(_ context.Context, _ string) (bool, error) {
	return false, s.ExistsErr
}

func (s *FailingStore) List(_ context.Context, _ string) ([]string, error) {
	return nil, s.ListErr
}

func (s *FailingStore) Delete(_ context.Context, _ string) error {
	return s.DeleteErr
}

func (s *FailingStore) ReadRange(_ context.Context, _ string, _, _ int64) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (s *FailingStore) ReaderAt(_ context.Context, _ string) (io.ReaderAt, error) {
	return nil, errors.New("not implemented")
}

// CompareAndSwap is a write: it records the path like Put and returns PutErr,
// so an injected write failure also fails the commit's latest-pointer swap.
// FailingStore never stores data, so only an empty expected value matches.
func (s *FailingStore) CompareAndSwap(_ context.Context, path, expected, _ string) error {
	s.PutCalls++
	s.PutPaths = append(s.PutPaths, path)
	if s.PutErr != nil {
		return s.PutErr
	}
	if expected != "" {
		return lode.ErrSnapshotConflict
	}
	return nil
}

var _ lode.RangeStore = (*FailingStore)(nil)

// FailingStoreFactory creates a factory that returns a FailingStore.
func FailingStoreFactory(store *FailingStore) lode.Factory[lode.RangeStore] {
	return func() (lode.RangeStore, error) {
		return store, nil
	}
}

// FailingFactoryFactory creates a factory that fails to create a store.
func FailingFactoryFactory(err error) lode.Factory[lode.RangeStore] {
	return func() (lode.RangeStore, error) {
		return nil, err
	}
}

// =============================================================================
// FS: Directory Creation Failure Tests
// =============================================================================

func TestLodeClient_FSDirectoryCreationFailure_NonExistentParent(t *testing.T) {
	t.Parallel()

	// Use t.TempDir() for isolation, then reference a non-existent subdirectory
	tmpDir := t.TempDir()
	nonExistentPath := filepath.Join(tmpDir, "does-not-exist", "nested", "path")

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	// Failure can occur at factory creation OR at write time
	client, factoryErr := NewLodeClient(t.Context(), cfg, nonExistentPath)

	if factoryErr != nil {
		// Factory creation failed - MUST be a typed StorageError with appropriate kind
		var storageErr *StorageError
		if !errors.As(factoryErr, &storageErr) {
			t.Fatalf("expected *StorageError for factory error, got %T: %v", factoryErr, factoryErr)
		}
		if !errors.Is(factoryErr, ErrNotFound) && !errors.Is(factoryErr, ErrPermissionDenied) && !errors.Is(factoryErr, ErrAccessDenied) {
			t.Errorf("expected ErrNotFound, ErrPermissionDenied, or ErrAccessDenied, got kind: %v", storageErr.Kind)
		}
		if storageErr.Op != "init" {
			t.Errorf("expected Op=init for factory error, got %s", storageErr.Op)
		}
		return
	}
	defer iox.DiscardClose(client)

	// Factory succeeded - write must fail
	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
	}

	writeErr := client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)
	if writeErr == nil {
		t.Fatal("expected error for non-existent directory, got nil")
	}

	// Assert it's a typed storage error with ErrNotFound
	var storageErr *StorageError
	if !errors.As(writeErr, &storageErr) {
		t.Fatalf("expected *StorageError, got %T", writeErr)
	}
	if !errors.Is(writeErr, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got kind: %v", storageErr.Kind)
	}
}

func TestLodeClient_FSDirectoryCreationFailure_ReadOnlyParent(t *testing.T) {
	t.Parallel()

	if os.Getuid() == 0 {
		t.Skip("skipping: test requires non-root user")
	}

	// Create a read-only directory
	tmpDir := t.TempDir()
	readOnlyDir := filepath.Join(tmpDir, "readonly")
	if err := os.MkdirAll(readOnlyDir, 0o555); err != nil {
		t.Fatalf("failed to create read-only dir: %v", err)
	}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	// Try to use a subdirectory of the read-only dir
	storePath := filepath.Join(readOnlyDir, "data")
	client, factoryErr := NewLodeClient(t.Context(), cfg, storePath)

	if factoryErr != nil {
		// Factory creation failed - MUST be a typed StorageError
		var storageErr *StorageError
		if !errors.As(factoryErr, &storageErr) {
			t.Fatalf("expected *StorageError for factory error, got %T: %v", factoryErr, factoryErr)
		}
		// Read-only parent can manifest as permission denied or not-found
		if !errors.Is(factoryErr, ErrPermissionDenied) && !errors.Is(factoryErr, ErrNotFound) {
			t.Errorf("expected ErrPermissionDenied or ErrNotFound, got kind: %v", storageErr.Kind)
		}
		if storageErr.Op != "init" {
			t.Errorf("expected Op=init for factory error, got %s", storageErr.Op)
		}
		return
	}
	defer iox.DiscardClose(client)

	// Factory succeeded - write must fail with permission error
	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
	}

	writeErr := client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)
	if writeErr == nil {
		t.Fatal("expected permission error, got nil")
	}

	// Assert it's a typed storage error with ErrPermissionDenied or ErrNotFound
	var storageErr *StorageError
	if !errors.As(writeErr, &storageErr) {
		t.Fatalf("expected *StorageError, got %T", writeErr)
	}
	if !errors.Is(writeErr, ErrPermissionDenied) && !errors.Is(writeErr, ErrNotFound) {
		t.Errorf("expected ErrPermissionDenied or ErrNotFound, got kind: %v", storageErr.Kind)
	}
}

// =============================================================================
// FS: File Write Failure Tests (Typed Error Injection)
// =============================================================================

func TestLodeClient_WriteFailure_DiskFull(t *testing.T) {
	t.Parallel()

	// Inject a disk-full error via FailingStore
	store := &FailingStore{
		PutErr: errors.New("write /data/quarry/events.jsonl: no space left on device"),
	}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
	}

	err = client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)
	if err == nil {
		t.Fatal("expected disk full error, got nil")
	}

	// Typed assertion: errors.Is should match ErrDiskFull
	if !errors.Is(err, ErrDiskFull) {
		t.Errorf("expected errors.Is(err, ErrDiskFull) to be true, got: %v", err)
	}

	// Also verify we can extract the StorageError
	var storageErr *StorageError
	if !errors.As(err, &storageErr) {
		t.Fatalf("expected *StorageError, got %T", err)
	}
	if storageErr.Op != "write" {
		t.Errorf("expected Op=write, got %s", storageErr.Op)
	}

	// Verify write was attempted
	if store.PutCalls != 1 {
		t.Errorf("expected 1 put call, got %d", store.PutCalls)
	}
}

func TestLodeClient_WriteFailure_PermissionDenied(t *testing.T) {
	t.Parallel()

	store := &FailingStore{
		PutErr: errors.New("write /data/quarry/events.jsonl: permission denied"),
	}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
	}

	err = client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)
	if err == nil {
		t.Fatal("expected permission error, got nil")
	}

	// Typed assertion
	if !errors.Is(err, ErrPermissionDenied) {
		t.Errorf("expected errors.Is(err, ErrPermissionDenied) to be true, got: %v", err)
	}
}

func TestLodeClient_ChunkWriteFailure_DiskFull(t *testing.T) {
	t.Parallel()

	store := &FailingStore{
		PutErr: errors.New("write /data/quarry/chunks.jsonl: no space left on device"),
	}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("data")},
	}

	err = client.WriteChunks(t.Context(), cfg.Dataset, cfg.RunID, chunks)
	if err == nil {
		t.Fatal("expected disk full error, got nil")
	}

	// Typed assertion
	if !errors.Is(err, ErrDiskFull) {
		t.Errorf("expected errors.Is(err, ErrDiskFull) to be true, got: %v", err)
	}
}

// =============================================================================
// FS: Atomic Write Semantics / Partial Write Detection
// =============================================================================

func TestLodeClient_PartialWriteDetection_ChunksNotMarkedOnFailure(t *testing.T) {
	t.Parallel()

	// Verify that on write failure, chunks are not marked as seen
	store := &FailingStore{
		PutErr: errors.New("simulated write failure"),
	}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("data")},
	}

	// Write should fail
	err = client.WriteChunks(t.Context(), cfg.Dataset, cfg.RunID, chunks)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Verify chunks are NOT marked as seen (no state mutation on failure)
	if _, seen := client.chunksSeen["art-1"]; seen {
		t.Error("chunks should not be marked as seen after write failure")
	}

	// Verify offset is NOT updated
	if offset := client.offsets["art-1"]; offset != 0 {
		t.Errorf("offset should be 0 after failure, got %d", offset)
	}
}

func TestLodeClient_PartialWriteDetection_OffsetsNotUpdatedOnFailure(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	// First, successfully write some chunks
	successStore := &FailingStore{PutErr: nil}
	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(successStore))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	chunks1 := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("12345")}, // 5 bytes
	}
	if err := client.WriteChunks(t.Context(), cfg.Dataset, cfg.RunID, chunks1); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Verify offset is 5
	if client.offsets["art-1"] != 5 {
		t.Errorf("offset after first write = %d, want 5", client.offsets["art-1"])
	}

	// Now make the store fail
	successStore.PutErr = errors.New("simulated failure")

	chunks2 := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 2, Data: []byte("67890")}, // 5 more bytes
	}
	err = client.WriteChunks(t.Context(), cfg.Dataset, cfg.RunID, chunks2)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Verify offset is STILL 5 (not updated due to failure)
	if client.offsets["art-1"] != 5 {
		t.Errorf("offset after failed write = %d, want 5 (unchanged)", client.offsets["art-1"])
	}
}

// =============================================================================
// S3: Auth Failure Tests
// =============================================================================

func TestLodeClient_S3AuthFailure(t *testing.T) {
	t.Parallel()

	store := &FailingStore{
		PutErr: errors.New("NoCredentialProviders: no valid credentials found"),
	}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
	}

	err = client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)
	if err == nil {
		t.Fatal("expected auth error, got nil")
	}

	// Typed assertion
	if !errors.Is(err, ErrAuth) {
		t.Errorf("expected errors.Is(err, ErrAuth) to be true, got: %v", err)
	}
}

// =============================================================================
// S3: Access Denied Tests
// =============================================================================

func TestLodeClient_S3AccessDenied(t *testing.T) {
	t.Parallel()

	store := &FailingStore{
		PutErr: errors.New("AccessDenied: Access Denied for s3://my-bucket/quarry/data.jsonl"),
	}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
	}

	err = client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)
	if err == nil {
		t.Fatal("expected access denied error, got nil")
	}

	// Typed assertion
	if !errors.Is(err, ErrAccessDenied) {
		t.Errorf("expected errors.Is(err, ErrAccessDenied) to be true, got: %v", err)
	}
}

// =============================================================================
// S3: Network Timeout Tests
// =============================================================================

// timeoutError implements the Timeout() interface
type timeoutError struct {
	msg string
}

func (e *timeoutError) Error() string   { return e.msg }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

func TestLodeClient_S3NetworkTimeout(t *testing.T) {
	t.Parallel()

	store := &FailingStore{
		PutErr: &timeoutError{msg: "RequestTimeout: PutObject timed out after 30s"},
	}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
	}

	err = client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	// Typed assertion
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("expected errors.Is(err, ErrTimeout) to be true, got: %v", err)
	}
}

// =============================================================================
// S3: Throttling (429) Tests
// =============================================================================

func TestLodeClient_S3Throttling(t *testing.T) {
	t.Parallel()

	store := &FailingStore{
		PutErr: errors.New("SlowDown: Rate exceeded, retry after 5s"),
	}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
	}

	err = client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)
	if err == nil {
		t.Fatal("expected throttling error, got nil")
	}

	// Typed assertion
	if !errors.Is(err, ErrThrottled) {
		t.Errorf("expected errors.Is(err, ErrThrottled) to be true, got: %v", err)
	}
}

// =============================================================================
// Error Wrapping: StorageError Contains Context
// =============================================================================

func TestLodeClient_StorageError_ContainsOperationAndPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		err       error
		wantKind  error
		wantOp    string
		wantInErr string // substring that should appear in error message
	}{
		{
			name:      "disk full",
			err:       errors.New("no space left on device"),
			wantKind:  ErrDiskFull,
			wantOp:    "write",
			wantInErr: "no space left",
		},
		{
			name:      "permission denied",
			err:       errors.New("permission denied"),
			wantKind:  ErrPermissionDenied,
			wantOp:    "write",
			wantInErr: "permission denied",
		},
		{
			name:      "S3 access denied",
			err:       errors.New("AccessDenied: Access Denied"),
			wantKind:  ErrAccessDenied,
			wantOp:    "write",
			wantInErr: "AccessDenied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &FailingStore{PutErr: tt.err}

			cfg := Config{
				Dataset:  "quarry",
				Source:   "test-source",
				Category: "test-cat",
				Day:      "2026-02-03",
				RunID:    "run-1",
				Policy:   "strict",
			}

			client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
			if err != nil {
				t.Fatalf("NewLodeClientWithFactory failed: %v", err)
			}

			events := []*types.EventEnvelope{
				{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
			}

			err = client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			// Extract StorageError
			var storageErr *StorageError
			if !errors.As(err, &storageErr) {
				t.Fatalf("expected *StorageError, got %T", err)
			}

			// Verify kind
			if !errors.Is(storageErr.Kind, tt.wantKind) {
				t.Errorf("kind = %v, want %v", storageErr.Kind, tt.wantKind)
			}

			// Verify operation
			if storageErr.Op != tt.wantOp {
				t.Errorf("op = %s, want %s", storageErr.Op, tt.wantOp)
			}

			// Verify path contains partition info
			if storageErr.Path == "" {
				t.Error("expected non-empty path in StorageError")
			}

			// Verify original error is in the chain
			errStr := err.Error()
			if !strings.Contains(errStr, tt.wantInErr) {
				t.Errorf("error %q should contain %q", errStr, tt.wantInErr)
			}
		})
	}
}

// =============================================================================
// Policy Failure Propagation (Storage Errors → Run Outcome)
// =============================================================================

func TestLodeClient_ErrorPropagation_EventWrite(t *testing.T) {
	t.Parallel()

	originalErr := errors.New("storage backend unavailable")
	store := &FailingStore{PutErr: originalErr}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
	}

	err = client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)

	// Error must propagate (not be swallowed)
	if err == nil {
		t.Fatal("error was swallowed, expected propagation")
	}

	// Original error should be in the chain via Unwrap
	var storageErr *StorageError
	if !errors.As(err, &storageErr) {
		t.Fatalf("expected *StorageError, got %T", err)
	}

	// The underlying error should be accessible
	if !errors.Is(storageErr.Err, originalErr) {
		// Check if error message is preserved
		if !strings.Contains(err.Error(), "storage backend unavailable") {
			t.Errorf("original error not in chain: %v", err)
		}
	}
}

func TestLodeClient_ErrorPropagation_ChunkWrite(t *testing.T) {
	t.Parallel()

	originalErr := errors.New("storage backend unavailable")
	store := &FailingStore{PutErr: originalErr}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("data")},
	}

	err = client.WriteChunks(t.Context(), cfg.Dataset, cfg.RunID, chunks)

	// Error must propagate
	if err == nil {
		t.Fatal("error was swallowed, expected propagation")
	}

	// Verify wrapped correctly
	var storageErr *StorageError
	if !errors.As(err, &storageErr) {
		t.Fatalf("expected *StorageError, got %T", err)
	}
}

// =============================================================================
// No Silent Corruption Paths
// =============================================================================

func TestLodeClient_NoSilentCorruption_FailedWritePreservesState(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	// Start with working store
	store := &FailingStore{PutErr: nil}
	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Write first batch successfully
	chunks1 := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("first-chunk")},
	}
	if err := client.WriteChunks(ctx, cfg.Dataset, cfg.RunID, chunks1); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	initialOffset := client.offsets["art-1"]
	if initialOffset != 11 { // len("first-chunk")
		t.Fatalf("initial offset = %d, want 11", initialOffset)
	}

	// Now make store fail
	store.PutErr = errors.New("simulated corruption scenario")

	// Try to write second batch (will fail)
	chunks2 := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 2, Data: []byte("second-chunk")},
	}
	err = client.WriteChunks(ctx, cfg.Dataset, cfg.RunID, chunks2)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Verify state is unchanged (no partial mutation)
	if client.offsets["art-1"] != initialOffset {
		t.Errorf("offset changed on failure: got %d, want %d", client.offsets["art-1"], initialOffset)
	}

	// Restore store and verify we can continue from correct offset
	store.PutErr = nil
	if err := client.WriteChunks(ctx, cfg.Dataset, cfg.RunID, chunks2); err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	// Offset should now reflect both batches
	expectedOffset := int64(11 + 12) // "first-chunk" + "second-chunk"
	if client.offsets["art-1"] != expectedOffset {
		t.Errorf("final offset = %d, want %d", client.offsets["art-1"], expectedOffset)
	}
}

func TestLodeClient_NoSilentCorruption_CommitFailurePreservesChunksState(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	// Start with working store
	store := &FailingStore{PutErr: nil}
	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Write chunks
	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("data")},
	}
	if err := client.WriteChunks(ctx, cfg.Dataset, cfg.RunID, chunks); err != nil {
		t.Fatalf("chunk write failed: %v", err)
	}

	// Verify chunks are marked
	if _, seen := client.chunksSeen["art-1"]; !seen {
		t.Fatal("chunks should be marked after write")
	}

	// Make commit fail
	store.PutErr = errors.New("commit failed")

	commitEvent := &types.EventEnvelope{
		Type: types.EventTypeArtifact,
		Seq:  2,
		Payload: map[string]any{
			"artifact_id":  "art-1",
			"name":         "test.txt",
			"content_type": "text/plain",
			"size_bytes":   float64(4),
		},
	}

	err = client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, []*types.EventEnvelope{commitEvent})
	if err == nil {
		t.Fatal("expected commit error, got nil")
	}

	// Verify chunks are STILL marked (commit failure doesn't clear state)
	if _, seen := client.chunksSeen["art-1"]; !seen {
		t.Error("chunks should still be marked after failed commit")
	}

	// Verify offset is preserved
	if client.offsets["art-1"] != 4 {
		t.Errorf("offset should be preserved: got %d, want 4", client.offsets["art-1"])
	}
}

// =============================================================================
// Wrapped Cause Chain Tests
// =============================================================================

func TestLodeClient_ErrorChain_UnwrapPreservesOriginal(t *testing.T) {
	t.Parallel()

	// Create a specific error we can test for
	originalErr := errors.New("original underlying error")
	store := &FailingStore{PutErr: originalErr}

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test",
		Category: "test",
		Day:      "2026-02-03",
		RunID:    "run-1",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, FailingStoreFactory(store))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
	}

	err = client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// The error chain should include a StorageError
	var storageErr *StorageError
	if !errors.As(err, &storageErr) {
		t.Fatalf("expected *StorageError in chain, got %T", err)
	}

	// The original error message should be preserved in the chain
	// (Note: Lode may wrap the error, so we check the message is present)
	errStr := err.Error()
	if !strings.Contains(errStr, "original underlying error") {
		t.Errorf("original error message not in chain: %v", err)
	}

	// StorageError.Unwrap should return something (the wrapped error from Lode)
	unwrapped := errors.Unwrap(storageErr)
	if unwrapped == nil {
		t.Error("StorageError.Unwrap() returned nil, expected wrapped error")
	}
}
