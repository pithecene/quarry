// Package runtime implements the Quarry runtime orchestration.
package runtime

import (
	"fmt"
	"sync"

	"github.com/pithecene/quarry/ipc"
	"github.com/pithecene/quarry/types"
)

// MaxArtifactSize is the maximum allowed artifact size (1 GiB).
// Per CONTRACT_IPC.md, this is implementation-defined (recommended: 1 GiB).
const MaxArtifactSize = 1 * 1024 * 1024 * 1024

// ArtifactManager manages artifact chunk accumulation and orphan tracking.
// Per CONTRACT_IPC.md, chunks may arrive before the artifact event.
// Thread-safe for concurrent access.
type ArtifactManager struct {
	mu           sync.RWMutex
	accumulators map[string]*types.ArtifactAccumulator
	// pendingCommits tracks artifacts where commit arrived before all chunks.
	// Maps artifact_id -> declared size_bytes for reconciliation.
	pendingCommits map[string]int64
}

// NewArtifactManager creates a new artifact manager.
func NewArtifactManager() *ArtifactManager {
	return &ArtifactManager{
		accumulators:   make(map[string]*types.ArtifactAccumulator),
		pendingCommits: make(map[string]int64),
	}
}

// AddChunk adds a chunk to an artifact.
// Per CONTRACT_IPC.md:
//   - seq must start at 1 and be strictly increasing
//   - chunks may arrive before the artifact event
//
// Returns error if:
//   - seq is not the expected next sequence
//   - chunk arrives after is_last=true was seen
//   - chunk data exceeds max chunk size
//   - accumulated size exceeds MaxArtifactSize
//   - size mismatch when commit arrived before chunks and is_last is seen
func (m *ArtifactManager) AddChunk(chunk *types.ArtifactChunk) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate chunk size per CONTRACT_IPC.md
	if len(chunk.Data) > ipc.MaxChunkSize {
		return fmt.Errorf("artifact %s: chunk size %d exceeds max %d",
			chunk.ArtifactID, len(chunk.Data), ipc.MaxChunkSize)
	}

	acc, exists := m.accumulators[chunk.ArtifactID]
	if !exists {
		acc = &types.ArtifactAccumulator{
			ArtifactID: chunk.ArtifactID,
			Chunks:     make([]*types.ArtifactChunk, 0),
			NextSeq:    1,
		}
		m.accumulators[chunk.ArtifactID] = acc
	}

	// Reject operations on artifacts in error state
	if acc.ErrorState {
		return fmt.Errorf("artifact %s: in error state, rejecting further operations", chunk.ArtifactID)
	}

	// Validate sequence ordering per CONTRACT_IPC.md
	if chunk.Seq != acc.NextSeq {
		return fmt.Errorf("artifact %s: expected seq %d, got %d",
			chunk.ArtifactID, acc.NextSeq, chunk.Seq)
	}

	// Check if already completed (is_last was already seen)
	if acc.Complete {
		return fmt.Errorf("artifact %s: chunk received after is_last", chunk.ArtifactID)
	}

	// Check max artifact size
	newTotal := acc.TotalBytes + int64(len(chunk.Data))
	if newTotal > MaxArtifactSize {
		return fmt.Errorf("artifact %s: size %d exceeds max %d",
			chunk.ArtifactID, newTotal, MaxArtifactSize)
	}

	// Add chunk
	acc.Chunks = append(acc.Chunks, chunk)
	acc.TotalBytes = newTotal
	acc.NextSeq++

	if chunk.IsLast {
		acc.Complete = true

		// If commit arrived before chunks, reconcile size now
		if declaredSize, pending := m.pendingCommits[chunk.ArtifactID]; pending {
			// Always clean up pending commit to avoid inconsistent state
			delete(m.pendingCommits, chunk.ArtifactID)

			if acc.TotalBytes != declaredSize {
				// Mark accumulator in error state to prevent further operations
				acc.ErrorState = true
				return fmt.Errorf("artifact %s: size mismatch (chunks=%d, declared=%d)",
					chunk.ArtifactID, acc.TotalBytes, declaredSize)
			}
			// Size matches, mark as committed
			acc.Committed = true
		}
	}

	return nil
}

// CommitArtifact marks an artifact as committed (artifact event received).
// Per CONTRACT_IPC.md, the artifact event is the authoritative commit record.
// Chunks may arrive before or after this call.
//
// Returns error if:
//   - size_bytes exceeds MaxArtifactSize
//   - size_bytes doesn't match accumulated bytes (when chunks are complete)
func (m *ArtifactManager) CommitArtifact(artifactID string, sizeBytes int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate max artifact size
	if sizeBytes > MaxArtifactSize {
		return fmt.Errorf("artifact %s: declared size %d exceeds max %d",
			artifactID, sizeBytes, MaxArtifactSize)
	}

	acc, exists := m.accumulators[artifactID]
	if !exists {
		// Artifact event arrived before any chunks - this is valid per contract.
		// Track the declared size for reconciliation when chunks complete.
		m.pendingCommits[artifactID] = sizeBytes
		acc = &types.ArtifactAccumulator{
			ArtifactID: artifactID,
			Chunks:     make([]*types.ArtifactChunk, 0),
			NextSeq:    1,
			// Note: NOT marked Committed yet - will be marked when chunks complete
		}
		m.accumulators[artifactID] = acc
		return nil
	}

	// Reject operations on artifacts in error state
	if acc.ErrorState {
		return fmt.Errorf("artifact %s: in error state, rejecting further operations", artifactID)
	}

	// If chunks are complete, verify size matches
	if acc.Complete {
		if acc.TotalBytes != sizeBytes {
			// Mark as error state - size mismatch is a terminal contract violation
			acc.ErrorState = true
			return fmt.Errorf("artifact %s: size mismatch (chunks=%d, declared=%d)",
				artifactID, acc.TotalBytes, sizeBytes)
		}
		acc.Committed = true
	} else {
		// Chunks not complete yet - track for reconciliation
		m.pendingCommits[artifactID] = sizeBytes
	}

	return nil
}

// GetOrphanIDs returns the list of artifact IDs with chunks but no commit.
// These are eligible for GC per CONTRACT_IPC.md.
//
// Note: Artifacts with a pending commit (commit arrived before chunks complete)
// are NOT considered orphans - they have a valid commit, just waiting for data.
func (m *ArtifactManager) GetOrphanIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var orphans []string
	for id, acc := range m.accumulators {
		// Not an orphan if:
		// - Already committed
		// - Has a pending commit (commit arrived, waiting for chunks)
		// - No chunks yet (nothing to orphan)
		// - In error state (already failed)
		if acc.Committed || acc.ErrorState || len(acc.Chunks) == 0 {
			continue
		}

		// Check if there's a pending commit for this artifact
		if _, hasPendingCommit := m.pendingCommits[id]; hasPendingCommit {
			continue
		}

		// This artifact has chunks but no commit - it's an orphan
		orphans = append(orphans, id)
	}
	return orphans
}

// GetArtifact returns the accumulator for an artifact.
func (m *ArtifactManager) GetArtifact(artifactID string) (*types.ArtifactAccumulator, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	acc, exists := m.accumulators[artifactID]
	return acc, exists
}

// IsCommitted returns true if the artifact has been committed.
func (m *ArtifactManager) IsCommitted(artifactID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	acc, exists := m.accumulators[artifactID]
	return exists && acc.Committed
}

// Stats returns artifact accumulation statistics.
func (m *ArtifactManager) Stats() ArtifactStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := ArtifactStats{}
	for id, acc := range m.accumulators {
		stats.TotalArtifacts++
		stats.TotalChunks += int64(len(acc.Chunks))
		stats.TotalBytes += acc.TotalBytes

		switch {
		case acc.Committed:
			stats.CommittedArtifacts++
		case acc.ErrorState:
			// Error state artifacts are counted separately (not orphaned, not committed)
		case len(acc.Chunks) > 0:
			// Only count as orphan if no pending commit
			if _, hasPendingCommit := m.pendingCommits[id]; !hasPendingCommit {
				stats.OrphanedArtifacts++
			}
		}
	}
	return stats
}

// ArtifactStats holds artifact accumulation statistics.
type ArtifactStats struct {
	TotalArtifacts     int64
	CommittedArtifacts int64
	OrphanedArtifacts  int64
	TotalChunks        int64
	TotalBytes         int64
}
