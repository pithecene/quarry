package lode

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/types"
)

// ErrCommitWithoutStorage is returned when an artifact commit event carries
// no storage result: the runtime attaches one once the artifact's ranges
// cover it (CONTRACT_LODE.md §Artifact Storage).
var ErrCommitWithoutStorage = errors.New("artifact commit rejected: no storage result for artifact")

// ErrMissingArtifactID is returned when an artifact commit event is missing
// the required artifact_id field.
var ErrMissingArtifactID = errors.New("artifact commit rejected: missing or empty artifact_id")

// LodeClient is a real Lode-backed implementation of Client.
// Uses Lode's HiveLayout with partition keys: source/category/day/run_id/event_type.
type LodeClient struct { //nolint:revive // intentional naming for clarity
	dataset *lode.Records[lode.D]
	config  Config
	// store is the dataset's store; sidecar files, run records, and
	// artifact Volumes are written to it directly, outside the dataset's
	// snapshots.
	store lode.RangeStore

	mu           sync.Mutex           // guards the fields below
	pendingFiles []SidecarFileRef     // sidecar files written since last snapshot flush
	files        []SidecarFileRef     // every sidecar file the run wrote
	fileStates   map[string]fileState // by filename
}

// openClient creates the store once, opens the dataset over it, and returns
// a client that writes sidecar files and run records to the same store.
// All constructors must use this to ensure consistent initialization.
func openClient(ctx context.Context, cfg Config, factory lode.Factory[lode.RangeStore]) (*LodeClient, error) {
	if factory == nil {
		return nil, errors.New("lode client: nil store factory")
	}
	store, err := factory()
	if err != nil {
		return nil, err
	}
	var opts []lode.DatasetOption
	if cfg.RetryObserver != nil {
		opts = append(opts, lode.WithRetryObserver(cfg.RetryObserver))
	}
	ds, err := OpenDataset(ctx, cfg.Dataset, store, opts...)
	if err != nil {
		return nil, err
	}
	return &LodeClient{
		fileStates: make(map[string]fileState),
		dataset:    ds,
		config:     cfg,
		store:      store,
	}, nil
}

// NewLodeClient creates a new Lode client with filesystem storage.
// The root parameter is the base directory for Hive-partitioned storage.
func NewLodeClient(ctx context.Context, cfg Config, root string) (*LodeClient, error) {
	return NewLodeClientWithFactory(ctx, cfg, lode.FS(root))
}

// NewLodeClientWithFactory creates a new Lode client with a custom store
// factory, such as lode.Memory() in tests. Opening reads the dataset's head
// under ctx.
func NewLodeClientWithFactory(ctx context.Context, cfg Config, factory lode.Factory[lode.RangeStore]) (*LodeClient, error) {
	c, err := openClient(ctx, cfg, factory)
	if err != nil {
		return nil, WrapInitError(err, cfg.Dataset)
	}
	return c, nil
}

// WriteEvents writes a batch of events to Lode.
// Artifact events (type=artifact) are written as artifact commit records, and other
// events as event records.
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

	records := make([]lode.D, 0, len(events))
	for _, e := range events {
		var record lode.D
		if e.Type == types.EventTypeArtifact {
			// Extract artifact_id from payload and validate
			artifactID := extractArtifactID(e.Payload)
			if artifactID == "" {
				return ErrMissingArtifactID
			}
			// The storage result names the artifact's Volume, or says the
			// artifact is empty; the runtime attaches it before the event
			// reaches a policy.
			if e.Storage == nil {
				return fmt.Errorf("%w: %s", ErrCommitWithoutStorage, artifactID)
			}
			// A size that is present must be an int64, or the record would store
			// a size the artifact does not have.
			if v, present := e.Payload["size_bytes"]; present {
				if _, ok := int64Of(v); !ok {
					return fmt.Errorf("artifact %s: size_bytes %v (%T) is not an int64", artifactID, v, v)
				}
			}
			record = toArtifactCommitRecordMap(e, c.config)
		} else {
			record = toEventRecordMap(e, c.config)
		}
		records = append(records, record)
	}

	snap, err := c.dataset.Write(ctx, records, c.snapshotMetadata())
	if err != nil {
		return WrapWriteError(err, buildPartitionPath(c.config, string(events[0].Type)))
	}

	c.drainPendingFiles()
	c.indexCommits(ctx, events, snap.ID)

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

// snapshotMetadata returns metadata containing any pending sidecar file refs,
// or an empty object if no files are pending. The empty case must be a
// non-nil map: a nil map encodes as JSON null, which Lode rejects.
// Only WriteEvents calls it.
// Must be called under c.mu.
func (c *LodeClient) snapshotMetadata() map[string]any {
	if len(c.pendingFiles) == 0 {
		return map[string]any{}
	}
	// Copy pending files into metadata value to decouple from mutable state.
	refs := make([]SidecarFileRef, len(c.pendingFiles))
	copy(refs, c.pendingFiles)
	return map[string]any{MetadataKeySidecarFiles: refs}
}

// drainPendingFiles clears accumulated sidecar file refs after a successful write.
// Must be called under c.mu.
func (c *LodeClient) drainPendingFiles() {
	c.pendingFiles = c.pendingFiles[:0]
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
