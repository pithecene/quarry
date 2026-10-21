package lode

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pithecene-io/lode/lode"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

// checksumEnabled controls whether MD5 checksums are computed for chunks.
// Default is false per CONTRACT_LODE.md (checksum is optional).
const checksumEnabled = false

// ErrCommitWithoutChunks is returned when an artifact commit is attempted
// without any chunks having been written for that artifact.
var ErrCommitWithoutChunks = errors.New("artifact commit rejected: no chunks written for artifact")

// ErrMissingArtifactID is returned when an artifact commit event is missing
// the required artifact_id field.
var ErrMissingArtifactID = errors.New("artifact commit rejected: missing or empty artifact_id")

// LodeClient is a real Lode-backed implementation of Client.
// Uses Lode's HiveLayout with partition keys: source/category/day/run_id/event_type.
type LodeClient struct { //nolint:revive // intentional naming for clarity
	dataset      lode.Dataset
	config       Config
	storeFactory lode.StoreFactory // for sidecar file writes via FileWriter

	mu           sync.Mutex          // guards offsets, chunksSeen, and pendingFiles
	offsets      map[string]int64    // cumulative offset per artifact across batches
	chunksSeen   map[string]struct{} // tracks artifacts that have had chunks written
	pendingFiles []SidecarFileRef    // sidecar files written since last snapshot flush

	storeOnce sync.Once  // lazy store initialization for FileWriter
	store     lode.Store // lazily created from storeFactory
	storeErr  error      // error from lazy store creation

	retryGate *retryGate // suppresses the retry observer during WriteMetrics
}

// retryGate suppresses Config.RetryObserver while the metrics record itself
// is being persisted. A retry incurred writing the snapshot cannot be
// reflected in that snapshot, and counting it would make the persisted
// record disagree with the in-memory collector the CLI prints.
// Lode invokes the observer synchronously on the committing goroutine, so
// the flag is read on the same goroutine that set it.
type retryGate struct {
	suppressed atomic.Bool
}

func (g *retryGate) wrap(fn func(attempt int)) func(attempt int) {
	return func(attempt int) {
		if !g.suppressed.Load() {
			fn(attempt)
		}
	}
}

// newClient creates a LodeClient from a dataset, config, store factory, and
// the retry gate that was installed on the dataset's observer.
// All constructors must use this to ensure consistent initialization.
func newClient(ds lode.Dataset, cfg Config, factory lode.StoreFactory, gate *retryGate) *LodeClient {
	return &LodeClient{
		dataset:      ds,
		config:       cfg,
		storeFactory: factory,
		offsets:      make(map[string]int64),
		chunksSeen:   make(map[string]struct{}),
		retryGate:    gate,
	}
}

// NewLodeClient creates a new Lode client with filesystem storage.
// The root parameter is the base directory for Hive-partitioned storage.
func NewLodeClient(cfg Config, root string) (*LodeClient, error) {
	return NewLodeClientWithFactory(cfg, lode.NewFSFactory(root))
}

// NewLodeClientWithFactory creates a new Lode client with a custom store factory.
// Use lode.NewMemoryFactory() for testing.
func NewLodeClientWithFactory(cfg Config, factory lode.StoreFactory) (*LodeClient, error) {
	gate := &retryGate{}
	ds, err := lode.NewDataset(lode.DatasetID(cfg.Dataset), factory, writeDatasetOptions(cfg, gate)...)
	if err != nil {
		return nil, WrapInitError(err, cfg.Dataset)
	}

	return newClient(ds, cfg, factory, gate), nil
}

// writeDatasetOptions returns the Dataset options shared by every write-path
// constructor: Hive layout, JSONL codec, bounded CAS retry per
// CONTRACT_LODE.md, and the gated retry observer when one is configured.
func writeDatasetOptions(cfg Config, gate *retryGate) []lode.Option {
	opts := []lode.Option{
		lode.WithHiveLayout("source", "category", "day", "run_id", "event_type"),
		lode.WithCodec(lode.NewJSONLCodec()),
		lode.WithRetryCount(3),
	}
	if cfg.RetryObserver != nil {
		opts = append(opts, lode.WithRetryObserver(gate.wrap(cfg.RetryObserver)))
	}
	return opts
}

// WriteEvents writes a batch of events to Lode.
// Artifact events (type=artifact) are converted to ArtifactCommitRecord format.
// Other events use EventRecord format.
// Events are partitioned by event_type (included in each record).
//
// Enforces "chunks before commit" invariant: artifact commit events are rejected
// if no chunks have been written for that artifact. After a successful commit,
// the artifact's offset and chunksSeen state are reset.
func (c *LodeClient) WriteEvents(ctx context.Context, _, _ string, events []*types.EventEnvelope) error {
	if len(events) == 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Collect artifact IDs being committed for post-write cleanup
	var committedArtifacts []string

	records := make([]any, 0, len(events))
	for _, e := range events {
		var record map[string]any
		if e.Type == types.EventTypeArtifact {
			// Extract artifact_id from payload and validate
			artifactID := extractArtifactID(e.Payload)
			if artifactID == "" {
				return ErrMissingArtifactID
			}
			if _, seen := c.chunksSeen[artifactID]; !seen {
				return fmt.Errorf("%w: %s", ErrCommitWithoutChunks, artifactID)
			}
			committedArtifacts = append(committedArtifacts, artifactID)
			record = toArtifactCommitRecordMap(e, c.config)
		} else {
			record = toEventRecordMap(e, c.config)
		}
		records = append(records, record)
	}

	_, err := c.dataset.Write(ctx, records, c.snapshotMetadata())
	if err != nil {
		return WrapWriteError(err, buildPartitionPath(c.config, string(events[0].Type)))
	}

	// Reset state for committed artifacts
	for _, artifactID := range committedArtifacts {
		delete(c.offsets, artifactID)
		delete(c.chunksSeen, artifactID)
	}
	c.drainPendingFiles()

	return nil
}

// WriteChunks writes a batch of artifact chunks to Lode.
// Chunks are written to event_type=artifact partition with record_kind=artifact_chunk.
// Offset is computed cumulatively across batches per artifact.
// Marks artifacts as having chunks for the "chunks before commit" invariant.
// State (offsets, chunksSeen) is only updated after successful write.
func (c *LodeClient) WriteChunks(ctx context.Context, _, _ string, chunks []*types.ArtifactChunk) error {
	if len(chunks) == 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	records := make([]any, 0, len(chunks))

	// Compute offsets locally first (don't modify state yet)
	localOffsets := make(map[string]int64)
	for id, offset := range c.offsets {
		localOffsets[id] = offset
	}

	for _, chunk := range chunks {
		offset := localOffsets[chunk.ArtifactID]
		record := toChunkRecordMap(chunk, offset, c.config)

		// Add checksum if enabled
		if checksumEnabled {
			checksum := computeMD5(chunk.Data)
			record["checksum"] = checksum
			record["checksum_algo"] = "md5"
		}

		records = append(records, record)
		localOffsets[chunk.ArtifactID] = offset + int64(len(chunk.Data))
	}

	// Write to storage.
	// Chunk writes use empty metadata — sidecar file refs are only flushed
	// on event and metrics writes, which are the consumer-facing boundaries.
	_, err := c.dataset.Write(ctx, records, lode.Metadata{})
	if err != nil {
		return WrapWriteError(err, buildPartitionPath(c.config, "artifact"))
	}

	// Only update state after successful write
	for _, chunk := range chunks {
		c.offsets[chunk.ArtifactID] = localOffsets[chunk.ArtifactID]
		c.chunksSeen[chunk.ArtifactID] = struct{}{}
	}

	return nil
}

// WriteMetrics writes a metrics snapshot to Lode.
// Written to event_type=metrics partition with record_kind=metrics.
// This is a standalone write (not part of the event/chunk pipeline).
// Retries incurred by this write are not observed: the snapshot being
// persisted was taken before the write began, so counting them would make
// the persisted record and the live collector disagree (CONTRACT_LODE.md).
func (c *LodeClient) WriteMetrics(ctx context.Context, snap metrics.Snapshot, completedAt time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.retryGate.suppressed.Store(true)
	defer c.retryGate.suppressed.Store(false)

	record := toMetricsRecordMap(snap, c.config, completedAt)
	_, err := c.dataset.Write(ctx, []any{record}, c.snapshotMetadata())
	if err != nil {
		return WrapWriteError(err, buildPartitionPath(c.config, "metrics"))
	}
	c.drainPendingFiles()
	return nil
}

// Close releases client resources.
func (c *LodeClient) Close() error {
	// Dataset doesn't require explicit close in current Lode API
	return nil
}

// MetadataKeySidecarFiles is the Metadata key for sidecar file inventory.
// Downstream consumers read this to enumerate files without prefix-scanning.
const MetadataKeySidecarFiles = "sidecar_files"

// snapshotMetadata returns Metadata containing any pending sidecar file refs.
// Returns empty metadata if no files are pending.
// Only called from WriteEvents and WriteMetrics — chunk writes do not flush
// file refs because chunks are not consumer-facing commit boundaries.
// Must be called under c.mu.
func (c *LodeClient) snapshotMetadata() lode.Metadata {
	if len(c.pendingFiles) == 0 {
		return lode.Metadata{}
	}
	// Copy pending files into metadata value to decouple from mutable state.
	refs := make([]SidecarFileRef, len(c.pendingFiles))
	copy(refs, c.pendingFiles)
	return lode.Metadata{MetadataKeySidecarFiles: refs}
}

// drainPendingFiles clears accumulated sidecar file refs after a successful write.
// Must be called under c.mu.
func (c *LodeClient) drainPendingFiles() {
	c.pendingFiles = c.pendingFiles[:0]
}

// computeMD5 returns the hex-encoded MD5 digest of data.
func computeMD5(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// extractArtifactID extracts the artifact_id from an event payload.
// Returns empty string if not found or not a string.
func extractArtifactID(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	if id, ok := payload["artifact_id"].(string); ok {
		return id
	}
	return ""
}

// buildPartitionPath constructs a human-readable partition path for error messages.
func buildPartitionPath(cfg Config, eventType string) string {
	return cfg.Dataset + "/" + cfg.Source + "/" + cfg.Category + "/" + cfg.Day + "/" + cfg.RunID + "/" + eventType
}

// Verify LodeClient implements Client.
var _ Client = (*LodeClient)(nil)
