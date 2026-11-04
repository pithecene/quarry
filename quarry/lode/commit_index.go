package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

// commitsSegment is the directory under a run's artifact directory that
// holds the commit index: one entry per committed artifact, named by the
// artifact ID (CONTRACT_LODE.md §Commit Index).
const commitsSegment = "commits"

// commitIndexMaxBytes bounds an entry read back. An entry holds one
// snapshot ID, so anything larger is not an entry the runtime wrote.
const commitIndexMaxBytes = 4 << 10

// CommitIndexKey is the key of an artifact's commit index entry.
func CommitIndexKey(cfg Config, artifactID string) string {
	return RunArtifactDir(cfg) + commitsSegment + "/" + artifactID
}

// CommitIndexPrefix is the key prefix of a run's commit index entries.
func CommitIndexPrefix(cfg Config) string {
	return RunArtifactDir(cfg) + commitsSegment + "/"
}

// commitIndexEntry names the Dataset snapshot whose artifact partition
// holds an artifact's commit record. It states no fact about the artifact:
// a reader takes every fact from the record itself.
type commitIndexEntry struct {
	SnapshotID string `json:"snapshot_id"`
}

// indexCommits writes an entry for each artifact commit event among events,
// naming snapshot, which the Dataset write of those events returned. The
// commit records are persisted already, so a failed entry fails nothing:
// it is reported to the IndexFailureObserver, and a reader finds the
// artifact's record by reading the partition.
func (c *LodeClient) indexCommits(ctx context.Context, events []*types.EventEnvelope, snapshot lode.DatasetSnapshotID) {
	for _, e := range events {
		if e.Type != types.EventTypeArtifact {
			continue
		}
		id := extractArtifactID(e.Payload)
		if err := putCommitIndex(ctx, c.store, c.config, id, snapshot); err != nil && c.config.IndexFailureObserver != nil {
			c.config.IndexFailureObserver(id, err)
		}
	}
}

// putCommitIndex writes one entry, create-only: an entry that exists
// already is left as it is, and the write returns lode.ErrPathExists.
func putCommitIndex(ctx context.Context, store lode.Store, cfg Config, artifactID string, snapshot lode.DatasetSnapshotID) error {
	if err := ValidateArtifactID(artifactID); err != nil {
		return err
	}
	data, err := json.Marshal(commitIndexEntry{SnapshotID: string(snapshot)})
	if err != nil {
		return fmt.Errorf("commit index entry marshal failed: %w", err)
	}
	path := CommitIndexKey(cfg, artifactID)
	if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		return WrapWriteError(err, path)
	}
	return nil
}

// ReadCommitIndex returns the snapshot that an artifact's commit index
// entry names. It reports found false, with no error, when the entry is
// absent or is not an entry the runtime wrote: either way the reader reads
// the partition instead. A store error other than a missing entry is
// returned.
func ReadCommitIndex(ctx context.Context, store lode.Store, cfg Config, artifactID string) (snapshot lode.DatasetSnapshotID, found bool, err error) {
	if ValidateArtifactID(artifactID) != nil {
		return "", false, nil
	}
	path := CommitIndexKey(cfg, artifactID)
	rc, err := store.Get(ctx, path)
	if errors.Is(err, lode.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, WrapReadError(err, path)
	}
	defer iox.DiscardClose(rc)
	data, err := io.ReadAll(io.LimitReader(rc, commitIndexMaxBytes+1))
	if err != nil {
		return "", false, WrapReadError(err, path)
	}
	var entry commitIndexEntry
	if len(data) > commitIndexMaxBytes || json.Unmarshal(data, &entry) != nil || entry.SnapshotID == "" {
		return "", false, nil
	}
	return lode.DatasetSnapshotID(entry.SnapshotID), true, nil
}
