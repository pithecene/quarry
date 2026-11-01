package lode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"slices"
	"strings"

	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"

	"github.com/pithecene/quarry/types"
)

var (
	// ErrInvalidArtifactID is returned by OpenArtifact for an artifact ID
	// that is not one path segment.
	ErrInvalidArtifactID = errors.New("invalid artifact id")
	// ErrRangeRefused is returned by WriteRange for a range the artifact
	// cannot hold: an empty one, one past the artifact's size, one that
	// overlaps a committed range, or any range of an empty artifact.
	ErrRangeRefused = errors.New("artifact range refused")
	// ErrArtifactNotCovered is returned by Finish when the committed ranges
	// do not cover the artifact.
	ErrArtifactNotCovered = errors.New("artifact ranges do not cover its size")
	// ErrArtifactDigestMismatch is returned by Finish when the SHA-256 of
	// the committed bytes differs from the one the script expected.
	ErrArtifactDigestMismatch = errors.New("artifact digest mismatch")
)

// ArtifactStore opens the storage of a run's artifacts (CONTRACT_LODE.md
// §Artifact Storage). LodeClient implements it.
type ArtifactStore interface {
	// OpenArtifact opens the artifact's storage: a Volume under the run's
	// artifact directory with size as its total length, or no Volume when
	// size is zero. expectedSHA256 is the hex digest the script declared, or
	// empty. It returns ErrInvalidArtifactID for an ID that is not one path
	// segment, and a storage error when the Volume cannot be opened.
	OpenArtifact(ctx context.Context, artifactID string, size int64, expectedSHA256 string) (ArtifactWriter, error)
}

// ArtifactWriter writes one artifact's ranges and settles its storage. It
// is externally synchronized: the runtime calls it from the ingestion loop.
type ArtifactWriter interface {
	// WriteRange stages and commits one range, so a range that returns nil
	// is durable. It returns ErrRangeRefused for a range the artifact cannot
	// hold, and a storage error when the store fails. A failed range may be
	// written again: its staged block, if any, is reused.
	WriteRange(ctx context.Context, offset int64, data []byte) error
	// Finish checks that the committed ranges cover the artifact and that
	// the digest of its bytes matches the expected one, and returns the
	// storage the commit record names. It returns ErrArtifactNotCovered or
	// ErrArtifactDigestMismatch otherwise, and a storage error when a
	// committed range cannot be read back for the digest.
	Finish(ctx context.Context) (types.ArtifactStorage, error)
	// Abandon removes the artifact's Volume, if it has one, and the Volume
	// stays removed. A second call completes an interrupted removal.
	Abandon(ctx context.Context) error
}

// Verify LodeClient implements ArtifactStore.
var _ ArtifactStore = (*LodeClient)(nil)

// OpenArtifact implements ArtifactStore. The Volume is created in the
// run's artifact directory, RunArtifactDir, through lode.Prefixed over the
// client's store.
func (c *LodeClient) OpenArtifact(ctx context.Context, artifactID string, size int64, expectedSHA256 string) (ArtifactWriter, error) {
	if err := validateArtifactID(artifactID); err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, fmt.Errorf("%w: artifact %q has size %d", ErrInvalidArtifactID, artifactID, size)
	}
	w := &artifactWriter{
		id: artifactID, size: size, expected: expectedSHA256,
		hasher: sha256.New(), ahead: map[int64]volume.BlockRef{}, staged: map[int64]stagedBlock{},
	}
	if size == 0 {
		return w, nil
	}
	dir := RunArtifactDir(c.config)
	w.factory = lode.Prefixed(lode.Use(c.store), dir)
	vol, err := volume.New(volume.ID(artifactID), w.factory, size, c.volumeOptions()...)
	if err != nil {
		return nil, WrapWriteError(err, dir+"volumes/"+artifactID)
	}
	w.vol = vol
	return w, nil
}

// volumeOptions gives a Volume the bounded commit retry of the dataset
// (OpenDataset) and the run's retry observer, through the same gate.
func (c *LodeClient) volumeOptions() []volume.Option {
	opts := []volume.Option{volume.WithRetryCount(datasetRetryCount)}
	if c.config.RetryObserver != nil {
		opts = append(opts, volume.WithRetryObserver(c.retryGate.wrap(c.config.RetryObserver)))
	}
	return opts
}

// lodeReservedPrefix begins the names Lode keeps for its own staged
// objects, which it refuses as IDs.
const lodeReservedPrefix = ".lode-tmp-"

// validateArtifactID requires one path segment, as Lode's Volume ID does.
func validateArtifactID(id string) error {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, `/\`) || strings.HasPrefix(id, lodeReservedPrefix) {
		return fmt.Errorf("%w: %q", ErrInvalidArtifactID, id)
	}
	return nil
}

// artifactWriter is one artifact's Volume with the state the digest needs:
// the committed blocks, the hashed prefix [0, hashed), and the committed
// blocks ahead of it (docs/plans/artifact-volumes.md §4 Digest). It never
// keeps the artifact's bytes.
type artifactWriter struct {
	id       string
	size     int64
	expected string

	factory  lode.Factory[lode.RangeStore]
	vol      volume.Volume // nil for an empty artifact
	snapshot volume.SnapshotID
	blocks   []volume.BlockRef      // committed, sorted by offset
	staged   map[int64]stagedBlock // staged blocks whose commit failed, by offset

	hasher hash.Hash
	hashed int64
	ahead  map[int64]volume.BlockRef // committed blocks past hashed, by offset
}

func (w *artifactWriter) WriteRange(ctx context.Context, offset int64, data []byte) error {
	if err := w.admit(offset, int64(len(data))); err != nil {
		return err
	}
	blk, err := w.stage(ctx, offset, data)
	if err != nil {
		return err
	}
	snap, err := w.vol.Commit(ctx, []volume.BlockRef{blk}, nil)
	if err != nil {
		return w.failedCommit(ctx, blk, data, err)
	}
	w.committed(snap.ID, blk, data)
	return nil
}

// stagedBlock is a block a failed commit left staged, with the digest of
// the bytes it holds.
type stagedBlock struct {
	ref volume.BlockRef
	sum [sha256.Size]byte
}

// stage stages the range's block, or reuses the block a failed commit of
// the same range left staged, since a block's path names its offset and
// length and a retry of the range would otherwise find its path taken. A
// retry that carries other bytes, or another length, than the staged
// block is refused: the staged block stays until the artifact is
// abandoned, and a second block at the offset would be left behind.
func (w *artifactWriter) stage(ctx context.Context, offset int64, data []byte) (volume.BlockRef, error) {
	if s, ok := w.staged[offset]; ok {
		if s.ref.Length != int64(len(data)) || s.sum != sha256.Sum256(data) {
			return volume.BlockRef{}, fmt.Errorf("%w: artifact %q range at %d was staged with other bytes", ErrRangeRefused, w.id, offset)
		}
		return s.ref, nil
	}
	blk, err := w.vol.StageWriteAt(ctx, offset, bytes.NewReader(data))
	if err != nil {
		return volume.BlockRef{}, WrapWriteError(err, w.path())
	}
	return blk, nil
}

// committed records a committed block: in the sorted block list, in the
// digest when it continues the hashed prefix, or among the blocks ahead of
// it otherwise. data is the block's bytes when the caller holds them, so
// that a block at the prefix is hashed without a read back.
func (w *artifactWriter) committed(snapshot volume.SnapshotID, blk volume.BlockRef, data []byte) {
	w.snapshot = snapshot
	delete(w.staged, blk.Offset)
	i, found := slices.BinarySearchFunc(w.blocks, blk, func(a, b volume.BlockRef) int { return int(a.Offset - b.Offset) })
	if !found {
		w.blocks = slices.Insert(w.blocks, i, blk)
	}
	if blk.Offset == w.hashed && data != nil {
		w.hasher.Write(data)
		w.hashed = blk.Offset + blk.Length
		return
	}
	if blk.Offset >= w.hashed {
		w.ahead[blk.Offset] = blk
	}
}

// failedCommit settles a range whose Commit returned an error. A store can
// commit and then lose its response, so the writer reads the head: a
// manifest that names the block is that commit, and the range stands.
// Otherwise the block stays staged for a retry of the range, since the
// commit's outcome is unknown and a deleted block would leave a pending
// commit without its data, and the error stands.
func (w *artifactWriter) failedCommit(ctx context.Context, blk volume.BlockRef, data []byte, commitErr error) error {
	if head, err := w.vol.Latest(ctx); err == nil && slices.Contains(head.Manifest.Blocks, blk) {
		w.committed(head.ID, blk, data)
		return nil
	}
	w.staged[blk.Offset] = stagedBlock{ref: blk, sum: sha256.Sum256(data)}
	return WrapWriteError(commitErr, w.path())
}

// admit refuses a range the artifact cannot hold.
func (w *artifactWriter) admit(offset, length int64) error {
	switch {
	case w.vol == nil:
		return fmt.Errorf("%w: artifact %q is empty", ErrRangeRefused, w.id)
	case length == 0:
		return fmt.Errorf("%w: artifact %q range at %d is empty", ErrRangeRefused, w.id, offset)
	case offset < 0 || length > w.size-offset:
		return fmt.Errorf("%w: artifact %q range [%d, %d) is outside [0, %d)", ErrRangeRefused, w.id, offset, offset+length, w.size)
	}
	// The committed blocks are sorted and disjoint, so the range overlaps a
	// block only if it overlaps its neighbors.
	i, _ := slices.BinarySearchFunc(w.blocks, volume.BlockRef{Offset: offset}, func(a, b volume.BlockRef) int { return int(a.Offset - b.Offset) })
	if i > 0 && w.blocks[i-1].Offset+w.blocks[i-1].Length > offset {
		return fmt.Errorf("%w: artifact %q range at %d overlaps the range at %d", ErrRangeRefused, w.id, offset, w.blocks[i-1].Offset)
	}
	if i < len(w.blocks) && w.blocks[i].Offset < offset+length {
		return fmt.Errorf("%w: artifact %q range at %d overlaps the range at %d", ErrRangeRefused, w.id, offset, w.blocks[i].Offset)
	}
	return nil
}

// hashAhead hashes every committed block that now continues the hashed
// prefix, reading each back once. It runs at Finish, so a read that fails
// leaves every range durable and the digest to be retried.
func (w *artifactWriter) hashAhead(ctx context.Context) error {
	for {
		blk, ok := w.ahead[w.hashed]
		if !ok {
			return nil
		}
		data, err := w.vol.ReadAt(ctx, w.snapshot, blk.Offset, blk.Length)
		if err != nil {
			return WrapReadError(err, w.path())
		}
		w.hasher.Write(data)
		delete(w.ahead, blk.Offset)
		w.hashed += blk.Length
	}
}

func (w *artifactWriter) Finish(ctx context.Context) (types.ArtifactStorage, error) {
	if err := w.hashAhead(ctx); err != nil {
		return types.ArtifactStorage{}, err
	}
	if w.hashed != w.size {
		return types.ArtifactStorage{}, fmt.Errorf("%w: artifact %q has %d of %d bytes", ErrArtifactNotCovered, w.id, w.hashed, w.size)
	}
	sum := hex.EncodeToString(w.hasher.Sum(nil))
	if w.expected != "" && !strings.EqualFold(w.expected, sum) {
		return types.ArtifactStorage{}, fmt.Errorf("%w: artifact %q has sha256 %s, the script expected %s", ErrArtifactDigestMismatch, w.id, sum, w.expected)
	}
	storage := types.ArtifactStorage{Kind: types.ArtifactStorageEmpty, SHA256: sum}
	if w.vol != nil {
		storage.Kind = types.ArtifactStorageVolume
		storage.VolumeSnapshotID = string(w.snapshot)
	}
	return storage, nil
}

func (w *artifactWriter) Abandon(ctx context.Context) error {
	if w.vol == nil {
		return nil
	}
	if err := volume.Remove(ctx, volume.ID(w.id), w.factory); err != nil {
		return WrapWriteError(err, w.path())
	}
	return nil
}

// path names the artifact's Volume in errors.
func (w *artifactWriter) path() string {
	return "volumes/" + w.id
}
