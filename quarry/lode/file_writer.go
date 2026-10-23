package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pithecene/lode/lode"
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

// PutFile writes a sidecar file to Lode Store at the computed Hive path.
// Writes the data file and a companion .meta.json with content type.
// Uses lazy store initialization via storeFactory.
func (c *LodeClient) PutFile(ctx context.Context, filename, contentType string, data []byte) error {
	store, err := c.getOrCreateStore()
	if err != nil {
		return fmt.Errorf("file write store init failed: %w", err)
	}

	path := c.buildFilePath(filename)
	if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		return WrapWriteError(err, path)
	}

	// Write companion metadata file preserving content type.
	// Store.Put has no metadata parameter, so content type is persisted
	// as a sidecar JSON file alongside the data.
	meta, err := json.Marshal(fileMetadata{ContentType: contentType})
	if err != nil {
		return fmt.Errorf("file write metadata marshal failed: %w", err)
	}
	metaPath := path + ".meta.json"
	if err := store.Put(ctx, metaPath, bytes.NewReader(meta)); err != nil {
		return WrapWriteError(err, metaPath)
	}

	// Track the written file for inclusion in snapshot Metadata.
	c.mu.Lock()
	c.pendingFiles = append(c.pendingFiles, SidecarFileRef{
		Path:        path,
		Filename:    filename,
		ContentType: contentType,
		Size:        int64(len(data)),
	})
	c.mu.Unlock()

	return nil
}

// fileMetadata is the companion metadata written alongside sidecar files.
type fileMetadata struct {
	ContentType string `json:"content_type"`
}

// getOrCreateStore lazily initializes the Store from the factory.
func (c *LodeClient) getOrCreateStore() (lode.Store, error) {
	c.storeOnce.Do(func() {
		c.store, c.storeErr = c.storeFactory()
	})
	return c.store, c.storeErr
}

// buildFilePath computes the Hive-partitioned path for a sidecar file.
// Format: datasets/<dataset>/partitions/source=<s>/category=<c>/day=<d>/run_id=<r>/files/<filename>
func (c *LodeClient) buildFilePath(filename string) string {
	return SidecarFilesPrefix(c.config) + filename
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
