package retention

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
)

// metaSuffix names the content-type companion PutFile writes beside each
// sidecar file.
const metaSuffix = ".meta.json"

// sidecarPaths returns the run's sidecar objects, sorted: every file its
// snapshots record, with its companion, and, when the sidecar prefix is
// unambiguous, every object listed under it. Sidecar files are caller-owned,
// so Lode's expiry never deletes them (CONTRACT_LODE.md §Sidecar File
// Inventory). Recorded paths outside the run's sidecar prefix are returned
// as skipped and are never deleted.
func (p *pruner) sidecarPaths(ctx context.Context, cfg qlode.Config, partition string) (paths, skipped []string, err error) {
	prefix := qlode.SidecarFilesPrefix(cfg)
	set := map[string]bool{}
	recorded, err := p.recordedSidecars(ctx, partition)
	if err != nil {
		return nil, nil, err
	}
	for _, path := range recorded {
		name := strings.TrimPrefix(path, prefix)
		if !strings.HasPrefix(path, prefix) || name == "" || strings.Contains(name, "/") {
			skipped = append(skipped, path)
			continue
		}
		set[path], set[path+metaSuffix] = true, true
	}
	// A file written after the run's last commit is recorded nowhere, so the
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
func (p *pruner) recordedSidecars(ctx context.Context, partition string) ([]string, error) {
	ds := lode.DatasetID(p.dataset)
	parts, err := p.dr.ListPartitions(ctx, ds, lode.PartitionListOptions{Prefix: partition})
	if errors.Is(err, lode.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, qlode.WrapReadError(err, partition)
	}
	seen := map[lode.DatasetSnapshotID]bool{}
	var out []string
	for _, part := range parts {
		refs, err := p.dr.ListManifests(ctx, ds, part.Path, lode.ManifestListOptions{})
		if err != nil {
			return nil, qlode.WrapReadError(err, part.Path)
		}
		for _, ref := range refs {
			if seen[ref.ID] {
				continue
			}
			seen[ref.ID] = true
			m, err := p.dr.GetManifest(ctx, ds, ref)
			if errors.Is(err, lode.ErrNotFound) {
				continue // expired since the listing
			}
			if err != nil {
				return nil, qlode.WrapReadError(err, part.Path)
			}
			paths, ok := sidecarRefPaths(m.Metadata[qlode.MetadataKeySidecarFiles])
			if !ok {
				return nil, fmt.Errorf("%w: snapshot %s", errMalformedInventory, ref.ID)
			}
			out = append(out, paths...)
		}
	}
	return out, nil
}

// errMalformedInventory reports a sidecar_files value that names no paths in
// the recorded form. The files it was meant to name cannot be found, so the
// run is kept rather than losing them.
var errMalformedInventory = errors.New("malformed sidecar inventory")

// sidecarRefPaths extracts the paths from a sidecar_files metadata value,
// which a JSON round trip turns into []any of map[string]any. An absent
// value records no files. It is false when the value or an entry has
// another shape, or an entry lacks a path.
func sidecarRefPaths(raw any) ([]string, bool) {
	if raw == nil {
		return nil, true
	}
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
