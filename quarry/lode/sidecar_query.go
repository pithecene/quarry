package lode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/pithecene/lode/lode"
)

// QuerySidecarFiles collects all sidecar file refs from snapshot metadata
// for a given run of the dataset stored behind factory. Returns the union of
// sidecar_files across all snapshots that match the runID filter.
// Each path is the stored object's key: consecutive slashes collapsed, as
// Quarry stores the object. QueryRunDetails keeps the path that the inventory
// records.
//
// Returns a nil slice if no sidecar files are found, and an error if a
// matching snapshot contains malformed sidecar metadata.
func QuerySidecarFiles(ctx context.Context, factory lode.Factory[lode.RangeStore], dataset, runID string) ([]SidecarFileRef, error) {
	_, snapshots, err := readSnapshots(ctx, dataset, factory)
	if err != nil {
		return nil, err
	}
	files, err := sidecarFiles(snapshots, runID, "")
	if err != nil {
		return nil, err
	}
	for i := range files {
		files[i].Path = collapseSidecarSlashes(files[i].Path)
	}
	return files, nil
}

func sidecarFiles(snapshots []*lode.Snapshot, runID, expectedPrefix string) ([]SidecarFileRef, error) {
	var result []SidecarFileRef

	for _, snap := range snapshots {
		if !snapshotMatchesFilter(snap, "run_id", runID) {
			continue
		}

		files, err := decodeSidecarFiles(snap.Metadata, expectedPrefix)
		if err != nil {
			return nil, WrapReadError(err, "quarry/snapshot/"+string(snap.ID))
		}
		result = append(result, files...)
	}
	return result, nil
}

func decodeSidecarFiles(metadata lode.Metadata, expectedPrefix string) ([]SidecarFileRef, error) {
	var meta map[string]json.RawMessage
	if err := metadata.Decode(&meta); err != nil {
		return nil, err
	}
	raw, found := meta[MetadataKeySidecarFiles]
	if !found {
		return nil, nil
	}

	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("%s: expected an array: %w", MetadataKeySidecarFiles, err)
	}
	if entries == nil {
		return nil, fmt.Errorf("%s: expected an array, got null", MetadataKeySidecarFiles)
	}
	files := make([]SidecarFileRef, 0, len(entries))
	for i, entry := range entries {
		file, err := decodeSidecarFileRef(entry, expectedPrefix)
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", MetadataKeySidecarFiles, i, err)
		}
		files = append(files, file)
	}
	return files, nil
}

func decodeSidecarFileRef(raw json.RawMessage, expectedPrefix string) (SidecarFileRef, error) {
	// A zero-byte file is valid, so distinguish zero from a missing or null size.
	var file struct {
		Path        string  `json:"path"`
		Filename    string  `json:"filename"`
		ContentType *string `json:"content_type"`
		Size        *int64  `json:"size"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		return SidecarFileRef{}, err
	}
	if file.Path == "" || file.Filename == "" || file.ContentType == nil || file.Size == nil || *file.Size < 0 {
		return SidecarFileRef{}, errors.New("expected a file reference with path, filename, content_type, and a nonnegative size")
	}
	if strings.ContainsAny(file.Filename, `/\`) {
		return SidecarFileRef{}, errors.New("expected a flat filename with no path separators")
	}
	if file.Filename == "." || file.Filename == ".." {
		return SidecarFileRef{}, errors.New("expected a filename that is not a dot segment")
	}
	if expected := expectedPrefix + file.Filename; expectedPrefix != "" && collapseSidecarSlashes(file.Path) != expected {
		return SidecarFileRef{}, fmt.Errorf("path %q does not match the selected run's file %q", file.Path, expected)
	}
	return SidecarFileRef{
		Path:        file.Path,
		Filename:    file.Filename,
		ContentType: *file.ContentType,
		Size:        *file.Size,
	}, nil
}
