package retention

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
)

// metaSuffix names the content-type companion PutFile writes beside each
// sidecar file.
const metaSuffix = ".meta.json"

// sidecarPaths returns the run's sidecar objects, sorted: every file its
// snapshots and end record name, with its companion, and, when the sidecar prefix is
// unambiguous, every object listed under it. Sidecar files are caller-owned,
// so Lode's expiry never deletes them (CONTRACT_LODE.md §Sidecar File
// Inventory). Recorded paths outside the run's sidecar prefix are returned
// as skipped and are never deleted.
func (p *pruner) sidecarPaths(ctx context.Context, cfg qlode.Config, snapshots []*lode.Snapshot) (paths, skipped []string, err error) {
	prefix := qlode.SidecarFilesPrefix(cfg)
	// The run key was validated before selection. Its recorded prefix may
	// contain repeated slashes that older Lode stores collapsed.
	objectPrefix := path.Clean(prefix) + "/"
	set := map[string]bool{}
	recorded, err := recordedSidecars(snapshots)
	if err != nil {
		return nil, nil, err
	}
	// The run's prefix can hold another run's file when its key is ambiguous, so
	// the end record is used only while the run has snapshots. They expire before
	// any sidecar goes, so each path still holds the run's own object: without
	// them, an interrupted prune may have removed it, and another run may have
	// written the path since. The path is then left in place and reported.
	ended, err := qlode.ReadEndSidecars(ctx, p.store, cfg)
	if errors.Is(err, qlode.ErrMalformedSidecars) {
		err = fmt.Errorf("%w: %w", errMalformedInventory, err)
	}
	if err != nil {
		return nil, nil, err
	}
	if !unambiguous(cfg) {
		for _, file := range ended {
			if len(snapshots) > 0 {
				recorded = append(recorded, file.Path)
			} else {
				skipped = append(skipped, file.Path)
			}
		}
	}
	for _, recordedPath := range recorded {
		name, belongs := strings.CutPrefix(recordedPath, prefix)
		if !belongs || name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
			skipped = append(skipped, recordedPath)
			continue
		}
		key := objectPrefix + name
		set[key], set[key+metaSuffix] = true, true
	}
	// A file of a run without an end record can be recorded nowhere, so the
	// prefix is listed too, but only when its direct children can belong to
	// no other run: sidecar paths are built from unencoded values.
	if unambiguous(cfg) {
		listed, err := p.store.List(ctx, prefix)
		if err != nil && !errors.Is(err, lode.ErrNotFound) {
			return nil, nil, qlode.WrapReadError(err, prefix)
		}
		// Only direct children: a deeper path belongs to a run whose ID
		// continues this prefix, such as "r/files/x" beneath run "r".
		for _, path := range listed {
			if !strings.Contains(strings.TrimPrefix(path, prefix), "/") {
				set[path] = true
			}
		}
	}
	for path := range set {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, skipped, nil
}

// unambiguous reports whether Hive encoding leaves every component of the
// run's key unchanged. An encoded value contains no "/", so a direct child
// of the sidecar prefix then has exactly as many path segments as this
// run's sidecar paths, which another run's paths match only if all of its
// components are the same.
func unambiguous(cfg qlode.Config) bool {
	for _, v := range []string{cfg.Source, cfg.Category, cfg.Day, cfg.RunID} {
		if url.PathEscape(v) != v {
			return false
		}
	}
	return true
}

// recordedSidecars returns the sidecar paths that the run's snapshots record
// in their metadata (CONTRACT_LODE.md §Sidecar File Inventory).
func recordedSidecars(snapshots []*lode.Snapshot) ([]string, error) {
	var out []string
	for _, snap := range snapshots {
		var meta map[string]any
		if err := snap.Metadata.Decode(&meta); err != nil {
			return nil, fmt.Errorf("%w: snapshot %s: %w", errMalformedInventory, snap.ID, err)
		}
		raw, recorded := meta[qlode.MetadataKeySidecarFiles]
		if !recorded {
			continue // the writer omits the key when a snapshot adds no file
		}
		paths, ok := sidecarRefPaths(raw)
		if !ok {
			return nil, fmt.Errorf("%w: snapshot %s", errMalformedInventory, snap.ID)
		}
		out = append(out, paths...)
	}
	return out, nil
}

// errMalformedInventory reports a sidecar_files value that names no paths in
// the recorded form. The files it was meant to name cannot be found, so the
// run is kept rather than losing them.
var errMalformedInventory = errors.New("malformed sidecar inventory")

// sidecarRefPaths extracts the paths from a sidecar_files metadata value,
// which a JSON round trip turns into []any of map[string]any. It is false
// when the value, including null, or an entry has another shape, or an entry
// lacks a path.
func sidecarRefPaths(raw any) ([]string, bool) {
	refs, ok := raw.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		m, _ := r.(map[string]any)
		path, ok := m["path"].(string)
		if !ok || path == "" {
			return nil, false
		}
		out = append(out, path)
	}
	return out, true
}
