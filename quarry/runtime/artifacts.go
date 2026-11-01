// Package runtime implements the Quarry runtime orchestration.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/pithecene/quarry/ipc"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/types"
)

// MaxArtifactSize is the maximum artifact size (1 GiB), which the runtime
// enforces on the artifact_open frame (CONTRACT_IPC.md §Size Constraints).
const MaxArtifactSize = 1 * 1024 * 1024 * 1024

var (
	// ErrNoArtifactStore is returned for an artifact frame when the run has
	// no artifact storage configured.
	ErrNoArtifactStore = errors.New("artifact frame received but no ArtifactStore configured; ensure storage is properly configured for artifacts")
	// ErrArtifactNotOpen is returned for a range or a commit of an artifact
	// that no artifact_open frame opened.
	ErrArtifactNotOpen = errors.New("artifact not open")
	// ErrArtifactReopened is returned for an artifact_open frame whose
	// artifact is open or committed already.
	ErrArtifactReopened = errors.New("artifact already open")
	// ErrArtifactSizeMismatch is returned for an artifact event whose
	// size_bytes differs from the opened size.
	ErrArtifactSizeMismatch = errors.New("artifact size mismatch")
)

// ArtifactManager holds the state of a run's artifacts between their
// artifact_open frame and their artifact event (CONTRACT_IPC.md §Artifact
// Frames): each open artifact's writer, and the counts the run report
// shows. It keeps no artifact bytes. Thread-safe.
type ArtifactManager struct {
	mu        sync.Mutex
	store     lode.ArtifactStore // nil when the run has no storage
	open      map[string]*openArtifact
	committed map[string]struct{}
	abandoned []string
	stats     ArtifactStats
}

// openArtifact is an artifact whose open frame arrived and whose artifact
// event has not.
type openArtifact struct {
	writer lode.ArtifactWriter
	size   int64
}

// NewArtifactManager creates a manager over store. A nil store refuses
// every artifact frame with ErrNoArtifactStore.
func NewArtifactManager(store lode.ArtifactStore) *ArtifactManager {
	return &ArtifactManager{
		store:     store,
		open:      map[string]*openArtifact{},
		committed: map[string]struct{}{},
	}
}

// Open creates the artifact's storage from its open frame. It returns an
// error, which the ingestion loop treats as a stream error, when the run
// has no artifact storage, the size exceeds MaxArtifactSize, the artifact
// is open or committed already, or the store refuses the artifact.
func (m *ArtifactManager) Open(ctx context.Context, frame *types.ArtifactOpenFrame) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.store == nil {
		return ErrNoArtifactStore
	}
	if frame.SizeBytes > MaxArtifactSize {
		return fmt.Errorf("artifact %s: declared size %d exceeds max %d", frame.ArtifactID, frame.SizeBytes, MaxArtifactSize)
	}
	if _, open := m.open[frame.ArtifactID]; open {
		return fmt.Errorf("%w: %s", ErrArtifactReopened, frame.ArtifactID)
	}
	if _, committed := m.committed[frame.ArtifactID]; committed {
		return fmt.Errorf("%w: %s is committed", ErrArtifactReopened, frame.ArtifactID)
	}
	w, err := m.store.OpenArtifact(ctx, frame.ArtifactID, frame.SizeBytes, frame.ExpectedSHA256)
	if err != nil {
		return fmt.Errorf("artifact %s: open: %w", frame.ArtifactID, err)
	}
	m.open[frame.ArtifactID] = &openArtifact{writer: w, size: frame.SizeBytes}
	m.stats.TotalArtifacts++
	return nil
}

// WriteRange stages and commits one range of an open artifact. Its error
// is the range's acknowledgement error: the artifact is not open, the range
// is larger than ipc.MaxChunkSize, the store refuses the range
// (lode.ErrRangeRefused), or the store fails. A refused or failed range
// commits nothing, and ingestion continues.
func (m *ArtifactManager) WriteRange(ctx context.Context, frame *types.ArtifactRangeFrame) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.open[frame.ArtifactID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrArtifactNotOpen, frame.ArtifactID)
	}
	if len(frame.Data) > ipc.MaxChunkSize {
		return fmt.Errorf("%w: artifact %s range of %d bytes exceeds max %d", lode.ErrRangeRefused, frame.ArtifactID, len(frame.Data), ipc.MaxChunkSize)
	}
	if err := a.writer.WriteRange(ctx, frame.Offset, frame.Data); err != nil {
		return err
	}
	m.stats.TotalRanges++
	m.stats.TotalBytes += int64(len(frame.Data))
	return nil
}

// Commit settles an open artifact on its artifact event: sizeBytes must
// equal the opened size, and the committed ranges must cover the artifact
// with the expected digest (lode.ArtifactWriter.Finish). It returns the
// storage the commit record names and releases the artifact's state. Its
// error is a stream error.
func (m *ArtifactManager) Commit(ctx context.Context, artifactID string, sizeBytes int64) (types.ArtifactStorage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.open[artifactID]
	if !ok {
		return types.ArtifactStorage{}, fmt.Errorf("%w: %s", ErrArtifactNotOpen, artifactID)
	}
	if sizeBytes != a.size {
		return types.ArtifactStorage{}, fmt.Errorf("%w: artifact %s opened with %d bytes, event says %d", ErrArtifactSizeMismatch, artifactID, a.size, sizeBytes)
	}
	storage, err := a.writer.Finish(ctx)
	if err != nil {
		return types.ArtifactStorage{}, err
	}
	delete(m.open, artifactID)
	m.committed[artifactID] = struct{}{}
	m.stats.CommittedArtifacts++
	return storage, nil
}

// AbandonUnfinished removes the storage of every artifact that is open
// without an artifact event, at the run's end (CONTRACT_IPC.md §Volumes
// Without a Record), and returns their IDs in order. A removal that fails
// is reported in the joined error, and its artifact is still listed: the
// prune removes what the runtime could not.
func (m *ArtifactManager) AbandonUnfinished(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := slices.Sorted(maps.Keys(m.open))
	var errs []error
	for _, id := range ids {
		if err := m.open[id].writer.Abandon(ctx); err != nil {
			errs = append(errs, fmt.Errorf("artifact %s: %w", id, err))
		}
		delete(m.open, id)
		m.abandoned = append(m.abandoned, id)
	}
	return ids, errors.Join(errs...)
}

// OrphanIDs returns the IDs of the artifacts opened without an artifact
// event: those still open, and those AbandonUnfinished removed.
func (m *ArtifactManager) OrphanIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := slices.Collect(maps.Keys(m.open))
	ids = append(ids, m.abandoned...)
	slices.Sort(ids)
	return ids
}

// Stats returns the run's artifact counts.
func (m *ArtifactManager) Stats() ArtifactStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.stats
	stats.OrphanedArtifacts = int64(len(m.open) + len(m.abandoned))
	return stats
}

// ArtifactStats holds a run's artifact counts.
type ArtifactStats struct {
	// TotalArtifacts is the number of artifacts opened.
	TotalArtifacts int64
	// CommittedArtifacts is the number whose artifact event the runtime accepted.
	CommittedArtifacts int64
	// OrphanedArtifacts is the number opened without an artifact event.
	OrphanedArtifacts int64
	// TotalRanges is the number of ranges committed.
	TotalRanges int64
	// TotalBytes is the number of bytes committed.
	TotalBytes int64
}
