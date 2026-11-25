package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/iox"
)

// SidecarFileRef describes a sidecar file written via PutFile.
// Accumulated by LodeClient and flushed into snapshot Metadata so
// downstream consumers can enumerate files without prefix-scanning storage.
type SidecarFileRef struct {
	Path        string `json:"path"`
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
}

// FileWriter writes sidecar files to Lode Store.
// Files land at Hive-partitioned paths under files/, bypassing Dataset
// segment/manifest machinery entirely.
type FileWriter interface {
	// PutFile writes a file to the Hive-partitioned files/ prefix.
	// The filename must not contain path separators or "..".
	PutFile(ctx context.Context, filename, contentType string, data []byte) error
}

// Verify LodeClient implements FileWriter.
var _ FileWriter = (*LodeClient)(nil)

// recoveryTimeout bounds the readback of a failed write.
const recoveryTimeout = 10 * time.Second

// PutFile writes a sidecar file to Lode Store at the computed Hive path.
// Writes the data file and a companion .meta.json with content type, to the
// dataset's store.
func (c *LodeClient) PutFile(ctx context.Context, filename, contentType string, data []byte) error {
	prefix := SidecarFilesPrefix(c.config)
	key := collapseSidecarSlashes(prefix) + filename
	// Lode takes ".../segments/<name>/manifest.json" for a snapshot's manifest,
	// and its listing would then fail for every run of the dataset.
	if segs := strings.Split(key, "/"); segs[len(segs)-1] == "manifest.json" && segs[len(segs)-3] == "segments" {
		return WrapWriteError(fmt.Errorf("%s: %w: the key is reserved for a snapshot", key, lode.ErrInvalidPath), key)
	}
	if !c.reserveFilename(filename) {
		// An S3 store takes an identical second upload for its own first.
		return WrapWriteError(fmt.Errorf("%s: %w", key, lode.ErrPathExists), key)
	}
	recorded := false
	defer func() {
		if !recorded {
			c.releaseFilename(filename)
		}
	}()
	if err := c.putData(ctx, key, data); err != nil {
		return WrapWriteError(err, key)
	}

	// Write companion metadata file preserving content type.
	// Store.Put has no metadata parameter, so content type is persisted
	// as a sidecar JSON file alongside the data.
	meta, err := json.Marshal(fileMetadata{ContentType: contentType, RunID: c.config.RunID})
	if err != nil {
		return fmt.Errorf("file write metadata marshal failed: %w", err)
	}
	metaPath := key + ".meta.json"
	if err := c.putReconciled(ctx, metaPath, meta); err != nil {
		// The data stays: the store can have taken another write's equal bytes for
		// these, so no state of it makes a removal safe.
		return WrapWriteError(err, metaPath)
	}

	// Track the written file for inclusion in snapshot Metadata.
	c.mu.Lock()
	ref := SidecarFileRef{
		Path:        prefix + filename,
		Filename:    filename,
		ContentType: contentType,
		Size:        int64(len(data)),
	}
	c.pendingFiles = append(c.pendingFiles, ref)
	c.files = append(c.files, ref)
	recorded = true
	c.mu.Unlock()

	return nil
}

// reserveFilename reserves filename for one put. It is false for a filename of a
// recorded file or of a put in progress.
func (c *LodeClient) reserveFilename(filename string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.filenames[filename] {
		return false
	}
	c.filenames[filename] = true
	return true
}

func (c *LodeClient) releaseFilename(filename string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.filenames, filename)
}

// putData writes a file's data. Unlike its companion, the data names no run, so
// an existing path is a refusal without a read.
func (c *LodeClient) putData(ctx context.Context, key string, data []byte) error {
	putErr := c.store.Put(ctx, key, bytes.NewReader(data))
	if putErr == nil || errors.Is(putErr, lode.ErrPathExists) {
		return putErr
	}
	return c.reconcile(ctx, key, data, putErr)
}

// putReconciled writes data at key, create-only. A write that reports an error
// can have persisted, so it succeeds when the object reads back as data. The
// error may be the context's, so the read does not use its cancellation.
func (c *LodeClient) putReconciled(ctx context.Context, key string, data []byte) error {
	putErr := c.store.Put(ctx, key, bytes.NewReader(data))
	if putErr == nil {
		return nil
	}
	return c.reconcile(ctx, key, data, putErr)
}

// reconcile decides a write of data at key that failed with putErr.
func (c *LodeClient) reconcile(ctx context.Context, key string, data []byte, putErr error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recoveryTimeout)
	defer cancel()
	rc, err := c.store.Get(ctx, key)
	if err != nil {
		return putErr
	}
	defer iox.DiscardClose(rc)
	if !holds(rc, data) {
		return putErr
	}
	if c.config.ReconcileObserver != nil {
		c.config.ReconcileObserver(key, putErr)
	}
	return nil
}

// holds reports whether r yields exactly want.
func holds(r io.Reader, want []byte) bool {
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		if !bytes.HasPrefix(want, buf[:n]) {
			return false
		}
		want = want[n:]
		if err != nil {
			return errors.Is(err, io.EOF) && len(want) == 0
		}
	}
}

// fileMetadata is the companion metadata written alongside sidecar files.
type fileMetadata struct {
	ContentType string `json:"content_type"`
	// RunID tells this run's companion from another's at the same key.
	RunID string `json:"run_id"`
}

// Older Lode stores collapsed repeated slashes. Keep that object location
// while leaving other key checks to the store.
func collapseSidecarSlashes(key string) string {
	for strings.Contains(key, "//") {
		key = strings.ReplaceAll(key, "//", "/")
	}
	return key
}

// SidecarFilesPrefix is the key prefix of the run's sidecar files. Its
// components are interpolated unencoded (CONTRACT_LODE.md §Key Encoding,
// known limitation), so it equals the run's encoded partition prefix only
// when Hive encoding leaves every component unchanged.
func SidecarFilesPrefix(cfg Config) string {
	return fmt.Sprintf("datasets/%s/partitions/source=%s/category=%s/day=%s/run_id=%s/files/",
		cfg.Dataset, cfg.Source, cfg.Category, cfg.Day, cfg.RunID)
}
