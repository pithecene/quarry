package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

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

// recoveryTimeout bounds each step taken after a failed write: reading the
// object back, and removing a file whose companion failed.
const recoveryTimeout = 10 * time.Second

// PutFile writes a sidecar file to Lode Store at the computed Hive path.
// Writes the data file and a companion .meta.json with content type, to the
// dataset's store.
func (c *LodeClient) PutFile(ctx context.Context, filename, contentType string, data []byte) error {
	prefix := SidecarFilesPrefix(c.config)
	key := collapseSidecarSlashes(prefix) + filename
	if err := c.store.Put(ctx, key, bytes.NewReader(data)); err != nil {
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
		// A file that was not written whole is no file of the run. The context
		// may have ended the write, so the removal does not use its cancellation.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), recoveryTimeout)
		defer cancel()
		if delErr := c.store.Delete(cleanup, key); delErr != nil {
			err = errors.Join(err, fmt.Errorf("remove the incomplete file: %w", delErr))
		}
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
	c.mu.Unlock()

	return nil
}

// putReconciled writes data at key, create-only. A write that reports an error
// can have persisted, so it succeeds when the object reads back as data. The
// error may be the context's, so the read does not use its cancellation.
func (c *LodeClient) putReconciled(ctx context.Context, key string, data []byte) error {
	putErr := c.store.Put(ctx, key, bytes.NewReader(data))
	if putErr == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recoveryTimeout)
	defer cancel()
	rc, err := c.store.Get(ctx, key)
	if err != nil {
		return putErr
	}
	defer iox.DiscardClose(rc)
	stored, err := io.ReadAll(io.LimitReader(rc, int64(len(data))+1))
	if err != nil || !bytes.Equal(stored, data) {
		return putErr
	}
	if c.config.ReconcileObserver != nil {
		c.config.ReconcileObserver(key, putErr)
	}
	return nil
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

// StubFileWriter records PutFile calls for testing.
type StubFileWriter struct {
	mu    sync.Mutex
	Files []StubFileRecord
}

// StubFileRecord is a recorded file write for testing.
type StubFileRecord struct {
	Filename    string
	ContentType string
	Data        []byte
}

// NewStubFileWriter creates a new stub file writer.
func NewStubFileWriter() *StubFileWriter {
	return &StubFileWriter{}
}

// PutFile implements FileWriter by recording the call.
func (w *StubFileWriter) PutFile(_ context.Context, filename, contentType string, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.Files = append(w.Files, StubFileRecord{
		Filename:    filename,
		ContentType: contentType,
		Data:        data,
	})
	return nil
}

// Verify StubFileWriter implements FileWriter.
var _ FileWriter = (*StubFileWriter)(nil)
