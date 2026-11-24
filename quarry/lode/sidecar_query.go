package lode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/pithecene/lode/lode"
)

// sidecarOwner selects the inventory entries of one run.
type sidecarOwner struct {
	// prefix is the run's sidecar prefix as the writer records it,
	// consecutive slashes included (CONTRACT_LODE.md § Sidecar File Inventory).
	prefix string
	// dataset is the run's dataset, which its records do not carry.
	dataset string
	// store holds the dataset, whose snapshots runPrefixes reads for their run
	// keys when an entry names another run's file.
	store lode.ObjectStore
}

func runSidecarOwner(key Config, store lode.ObjectStore) sidecarOwner {
	return sidecarOwner{prefix: SidecarFilesPrefix(key), dataset: key.Dataset, store: store}
}

// recordKey is the part of a record that names its run. Reading a snapshot
// into it keeps four fields per record in place of each record's payload: the
// JSONL codec's identity does not depend on the Go type. The fields are any,
// so a record whose key field is not a string fails the check below. Each
// tag is case:strict, since a record names its run by the exact key run_id.
type recordKey struct {
	Source   any `json:"source,case:strict"`
	Category any `json:"category,case:strict"`
	Day      any `json:"day,case:strict"`
	RunID    any `json:"run_id,case:strict"`
}

// runPrefixes returns the sidecar prefix of every run whose records snap
// holds. One snapshot can hold the records of several runs and the files of
// each, so an entry belongs to another run only when it is that run's file.
// A record whose key is not a valid run key names no owner, so the read fails
// rather than accept an entry under that key.
func (o sidecarOwner) runPrefixes(ctx context.Context, snap *lode.Snapshot) (map[string]bool, error) {
	// Read the snapshot once: opening each object on its own validates the
	// whole manifest per object.
	view, err := lode.OpenReadOnly(lode.Use(o.store)).Records(ctx, lode.DatasetID(o.dataset), lode.JSONL[recordKey](), layout(), lode.WithAnchor(snap.ID))
	if err != nil {
		return nil, err
	}
	records, err := view.Read(ctx, snap.ID)
	if err != nil {
		return nil, err
	}
	prefixes := make(map[string]bool)
	for i, record := range records {
		key := Config{Dataset: o.dataset}
		var ok [4]bool
		key.Source, ok[0] = record.Source.(string)
		key.Category, ok[1] = record.Category.(string)
		key.Day, ok[2] = record.Day.(string)
		key.RunID, ok[3] = record.RunID.(string)
		if ok != [4]bool{true, true, true, true} {
			return nil, fmt.Errorf("record %d: expected string source, category, day, and run_id", i)
		}
		if err := validateRecordKey(key); err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		prefixes[SidecarFilesPrefix(key)] = true
	}
	return prefixes, nil
}

// sidecarFiles returns the union of the snapshots' inventories. A
// write that reached storage but returned an error leaves its refs pending
// for the next snapshot, so two snapshots can record the same file once each
// (CONTRACT_LODE.md § Sidecar File Inventory).
func sidecarFiles(ctx context.Context, snapshots []*lode.Snapshot, owner sidecarOwner) ([]SidecarFileRef, error) {
	var result []SidecarFileRef
	seen := make(map[SidecarFileRef]bool)

	for _, snap := range snapshots {
		files, err := owner.ownedFiles(ctx, snap)
		if err != nil {
			return nil, WrapReadError(err, "quarry/snapshot/"+string(snap.ID))
		}
		for _, file := range files {
			if !seen[file] {
				seen[file] = true
				result = append(result, file)
			}
		}
	}
	return result, nil
}

// ownedFiles returns the entries of snap's inventory that belong to the owner's
// run. It skips another run's file and rejects an entry that belongs to no
// run of snap.
func (o sidecarOwner) ownedFiles(ctx context.Context, snap *lode.Snapshot) ([]SidecarFileRef, error) {
	var meta map[string]json.RawMessage
	if err := snap.Metadata.Decode(&meta); err != nil {
		return nil, err
	}
	files, err := decodeSidecarFiles(meta)
	if err != nil {
		return nil, err
	}
	var others map[string]bool
	owned := files[:0]
	for i, file := range files {
		// Compare the path as recorded: collapsing slashes would equate the
		// keys of different runs, such as category "raw//x" and category "raw/x".
		if file.Path == o.prefix+file.Filename {
			owned = append(owned, file)
			continue
		}
		if others == nil {
			if others, err = o.runPrefixes(ctx, snap); err != nil {
				return nil, err
			}
		}
		prefix, found := strings.CutSuffix(file.Path, file.Filename)
		if !found || !others[prefix] {
			return nil, fmt.Errorf("%s[%d]: path %q does not match the selected run's file %q",
				MetadataKeySidecarFiles, i, file.Path, o.prefix+file.Filename)
		}
	}
	return owned, nil
}

// decodeSidecarFiles reads the inventory of a snapshot's metadata or of an end
// record.
func decodeSidecarFiles(meta map[string]json.RawMessage) ([]SidecarFileRef, error) {
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
		file, err := decodeSidecarFileRef(entry)
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", MetadataKeySidecarFiles, i, err)
		}
		files = append(files, file)
	}
	return files, nil
}

func decodeSidecarFileRef(raw json.RawMessage) (SidecarFileRef, error) {
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
	if file.Filename == "." {
		return SidecarFileRef{}, errors.New("expected a filename that is not a dot segment")
	}
	// The writer's filename rule (CONTRACT_EMIT.md § Filename Rules) forbids
	// any "..", not only a ".." segment.
	if strings.Contains(file.Filename, "..") {
		return SidecarFileRef{}, errors.New(`expected a filename that does not contain ".."`)
	}
	return SidecarFileRef{
		Path:        file.Path,
		Filename:    file.Filename,
		ContentType: *file.ContentType,
		Size:        *file.Size,
	}, nil
}
