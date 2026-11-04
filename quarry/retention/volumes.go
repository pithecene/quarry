package retention

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/reader"
)

// Reasons a selected run is kept because of its artifact Volumes.
const (
	keptVolumePending = "an artifact Volume has a commit in flight; rerun the prune"
	keptVolumeWritten = "an artifact Volume was written during the prune; rerun the prune"
	keptVolumeUnknown = "its artifact directory holds a name that is no artifact ID"
	keptVolumeUndated = "an artifact Volume's head cannot be told, so its last write has no instant"
)

// volumesSegment is the directory under a run's artifact directory that
// holds its Volumes, one per artifact ID (CONTRACT_LODE.md §Storage Layout).
const volumesSegment = "volumes"

// commitsSegment is the directory under a run's artifact directory that
// holds its commit index, one entry per artifact ID (CONTRACT_LODE.md
// §Commit Index).
const commitsSegment = "commits"

// Retention reads only the names of a run's Volumes. Each Volume's own
// directory is Lode's, so its objects are reached only through Lode: a
// Volume's head is read by volume.Head, a Volume is removed by
// volume.Remove, and a dry run calls volume.Remove against a sandboxStore,
// which records the one pointer swap Lode makes. The real removal then
// goes through a pinnedStore that allows only that swap, so it removes
// exactly the head the dry run judged.

// volumeHeads returns the head snapshot ID of each of the run's Volumes
// that has a commit, by artifact ID, a pending commit included, read
// without the artifact's size. A Volume with no commit, or a removed one,
// dates no write. It returns a reason to keep the run when a name is no
// artifact ID, or when a Volume's head cannot be told.
func (p *pruner) volumeHeads(ctx context.Context, cfg qlode.Config) (heads map[string]string, kept string, err error) {
	ids, invalid, err := p.listVolumes(ctx, cfg)
	if err != nil {
		return nil, "", err
	}
	if len(invalid) > 0 {
		return nil, keptVolumeUnknown, nil
	}
	heads = map[string]string{}
	for _, id := range ids {
		head, err := qlode.ArtifactVolumeHead(ctx, p.store, cfg, id)
		switch {
		case errors.Is(err, lode.ErrNoSnapshots), errors.Is(err, volume.ErrRemoved):
			continue
		case errors.Is(err, lode.ErrHeadUncommitted):
			return nil, keptVolumeUndated, nil
		case err != nil:
			return nil, "", qlode.WrapReadError(err, volumePath(cfg, id))
		}
		heads[id] = head
	}
	return heads, "", nil
}

// headsUnchanged reads the run's Volume heads again and returns a reason to
// keep the run when they differ from the heads that dated it: a Volume
// committed or appeared since, and the age did not judge that write.
func (p *pruner) headsUnchanged(ctx context.Context, cfg qlode.Config, dated map[string]string) (kept string, err error) {
	now, kept, err := p.volumeHeads(ctx, cfg)
	switch {
	case kept != "" || err != nil:
		return kept, err
	case !maps.Equal(now, dated):
		return keptVolumeWritten, nil
	}
	return "", nil
}

// volumeProbe is the dry-run removal of one Volume.
type volumeProbe struct {
	id string
	// pins are the swaps the dry run recorded.
	pins []swap
	// changed reports that the dry run changed anything. A removed Volume
	// keeps the object that fences it, so removing it again changes
	// nothing, and it is neither removed again nor counted.
	changed bool
}

// volumePreview is a run's Volumes, each removed in a dry run.
type volumePreview struct {
	// listed are the artifact IDs listed, sorted.
	listed []string
	// probes are the listed Volumes whose removal changes anything.
	probes []volumeProbe
}

// listVolumes returns the names under the run's Volume directory, split
// into artifact IDs and names that the write path could not have produced.
// It never interprets anything below a name.
func (p *pruner) listVolumes(ctx context.Context, cfg qlode.Config) (ids, invalid []string, err error) {
	prefix := qlode.RunArtifactDir(cfg) + volumesSegment + "/"
	keys, err := p.store.List(ctx, prefix)
	if errors.Is(err, lode.ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, qlode.WrapReadError(err, prefix)
	}
	// A name is a Volume only as a directory: an object stored at the name
	// itself lies outside the directory volume.Remove deletes.
	valid := map[string]bool{}
	for _, key := range keys {
		name, _, isDir := strings.Cut(strings.TrimPrefix(key, prefix), "/")
		ok := isDir && qlode.ValidateArtifactID(name) == nil
		if prior, seen := valid[name]; seen {
			ok = ok && prior
		}
		valid[name] = ok
	}
	for name, ok := range valid {
		if ok {
			ids = append(ids, name)
		} else {
			invalid = append(invalid, name)
		}
	}
	sort.Strings(ids)
	sort.Strings(invalid)
	return ids, invalid, nil
}

// previewVolumes lists the run's Volumes and removes each in a dry run. It
// returns a reason to keep the run when a name is no artifact ID, or when
// a Volume has a commit in flight, so the run is kept before anything of
// it is deleted.
func (p *pruner) previewVolumes(ctx context.Context, cfg qlode.Config) (volumePreview, string, error) {
	ids, invalid, err := p.listVolumes(ctx, cfg)
	if err != nil {
		return volumePreview{}, "", err
	}
	if len(invalid) > 0 {
		return volumePreview{}, keptVolumeUnknown, nil
	}
	preview := volumePreview{listed: ids}
	for _, id := range ids {
		probe, pending, err := p.probeVolume(ctx, cfg, id)
		if err != nil {
			return volumePreview{}, "", err
		}
		if pending {
			return volumePreview{}, keptVolumePending, nil
		}
		if probe.changed {
			preview.probes = append(preview.probes, probe)
		}
	}
	return preview, "", nil
}

// probeVolume removes Volume id in a dry run. pending reports that Lode
// would refuse the removal for a commit in flight.
func (p *pruner) probeVolume(ctx context.Context, cfg qlode.Config, id string) (probe volumeProbe, pending bool, err error) {
	sandbox := newSandboxStore(p.store)
	err = qlode.RemoveArtifactVolume(ctx, sandbox, cfg, id)
	probe = volumeProbe{id: id, pins: sandbox.Swaps(), changed: sandbox.Changed()}
	switch {
	case errors.Is(err, lode.ErrHeadUncommitted):
		return probe, true, nil
	case err != nil:
		return probe, false, qlode.WrapReadError(err, volumePath(cfg, id))
	}
	return probe, false, nil
}

// removeProbed removes a Volume that probeVolume removed in a dry run,
// allowing only the swaps the dry run recorded. It returns
// lode.ErrSnapshotConflict when the Volume committed since the dry run, and
// lode.ErrHeadUncommitted when a commit is now in flight.
func (p *pruner) removeProbed(ctx context.Context, cfg qlode.Config, probe volumeProbe) error {
	return qlode.RemoveArtifactVolume(ctx, &pinnedStore{RangeStore: p.store, pins: probe.pins}, cfg, probe.id)
}

// removeVolumes removes the previewed Volumes and lists the directory
// again. It returns a reason to keep the run when a Volume committed or a
// commit began since the preview, or when the listing shows a name the
// preview did not: a Volume opened since, or a name that is no artifact
// ID. Removed counts the Volumes removed, so an interrupted removal
// reports how far it got.
func (p *pruner) removeVolumes(ctx context.Context, cfg qlode.Config, preview volumePreview) (removed int, kept string, err error) {
	for _, probe := range preview.probes {
		err := p.removeProbed(ctx, cfg, probe)
		switch {
		case errors.Is(err, lode.ErrSnapshotConflict):
			return removed, keptVolumeWritten, nil
		case errors.Is(err, lode.ErrHeadUncommitted):
			return removed, keptVolumePending, nil
		case err != nil:
			return removed, "", qlode.WrapWriteError(err, volumePath(cfg, probe.id))
		}
		removed++
	}
	ids, invalid, err := p.listVolumes(ctx, cfg)
	switch {
	case err != nil:
		return removed, "", err
	case len(invalid) > 0:
		return removed, keptVolumeUnknown, nil
	case !slices.Equal(ids, preview.listed):
		return removed, keptVolumeWritten, nil
	}
	return removed, "", nil
}

// orphanVolumes is one run key's Volumes and commit index entries in the
// artifact namespace, without a start record beside them. Entries holds
// the entries' keys.
type orphanVolumes struct {
	key     reader.Key
	ids     []string
	entries []string
}

// sweepVolumes removes the Volumes of every run in the artifact namespace
// that has no start record, once the end of the run key's day is older
// than the grace period: a run whose records an earlier prune removed, or
// an object written after a prune's final listing. Each Volume is removed
// as a run's are, in a dry run first. A Volume with a commit in flight, or
// one that commits meanwhile, is left for the next sweep, and a dry run
// does not count it. Keys the write path could not have produced are
// returned as unparseable and never acted on.
func (p *pruner) sweepVolumes(ctx context.Context, grace time.Duration) (swept sweptArtifacts, unparseable []string, err error) {
	orphans, unparseable, err := p.orphanVolumes(ctx, grace)
	if err != nil {
		return swept, unparseable, err
	}
	for _, o := range orphans {
		cfg := p.configOf(o.key)
		for _, id := range o.ids {
			removed, err := p.sweepVolume(ctx, cfg, id)
			if err != nil {
				return swept, unparseable, err
			}
			if removed {
				swept.volumes++
			}
		}
		// An entry is never authoritative (CONTRACT_LODE.md §Commit Index), so
		// a run's entries go whatever became of its Volumes.
		if !p.opts.DryRun {
			if err := p.deleteAll(ctx, o.entries); err != nil {
				return swept, unparseable, err
			}
		}
		swept.entries += len(o.entries)
	}
	return swept, unparseable, nil
}

// sweptArtifacts counts what the sweep removed from the artifact namespace
// (in a dry run, would remove).
type sweptArtifacts struct {
	volumes int
	entries int
}

// sweepVolume removes one orphaned Volume, or in a dry run reports that it
// would. removed is false for a Volume left for the next sweep.
func (p *pruner) sweepVolume(ctx context.Context, cfg qlode.Config, id string) (removed bool, err error) {
	probe, pending, err := p.probeVolume(ctx, cfg, id)
	if err != nil || pending || !probe.changed {
		return false, err
	}
	if p.opts.DryRun {
		return true, nil
	}
	err = p.removeProbed(ctx, cfg, probe)
	switch {
	case errors.Is(err, lode.ErrSnapshotConflict), errors.Is(err, lode.ErrHeadUncommitted):
		return false, nil
	case err != nil:
		return false, qlode.WrapWriteError(err, volumePath(cfg, id))
	}
	return true, nil
}

// orphanVolumes lists the artifact namespace and groups the Volumes of the
// runs old enough for the sweep that have no start record, in key order.
func (p *pruner) orphanVolumes(ctx context.Context, grace time.Duration) (orphans []orphanVolumes, unparseable []string, err error) {
	root := qlode.ArtifactPrefix(p.dataset)
	keys, err := p.store.List(ctx, root)
	if errors.Is(err, lode.ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, qlode.WrapReadError(err, root)
	}
	sort.Strings(keys)
	byRun := map[reader.Key]*orphanVolumes{}
	var order []reader.Key
	for _, key := range keys {
		k, id, segment, ok := artifactKey(strings.TrimPrefix(key, root))
		if !ok {
			unparseable = append(unparseable, key)
			continue
		}
		run, listed := byRun[k]
		if !listed {
			run = &orphanVolumes{key: k}
			byRun[k] = run
			order = append(order, k)
		}
		switch {
		case segment == commitsSegment:
			run.entries = append(run.entries, key)
		case len(run.ids) == 0 || run.ids[len(run.ids)-1] != id:
			run.ids = append(run.ids, id)
		}
	}
	for _, k := range order {
		due, err := p.orphanDue(ctx, p.configOf(k), grace)
		if err != nil {
			return nil, unparseable, err
		}
		if due {
			orphans = append(orphans, *byRun[k])
		}
	}
	return orphans, unparseable, nil
}

// orphanDue reports whether a run key in the artifact namespace names no
// start record and its day ended more than the grace period ago. A run's
// day is its start's day, and the grace period exceeds the longest run, so
// a live run is never due.
func (p *pruner) orphanDue(ctx context.Context, cfg qlode.Config, grace time.Duration) (bool, error) {
	day, _ := time.Parse(time.DateOnly, cfg.Day) // artifactKey checked the day
	if p.opts.Now.Sub(day.Add(24*time.Hour)) <= grace {
		return false, nil
	}
	start := qlode.RunRecordDir(cfg) + qlode.StartRecordName
	exists, err := p.store.Exists(ctx, start)
	if err != nil {
		return false, qlode.WrapReadError(err, start)
	}
	return !exists, nil
}

// artifactKey decodes a key relative to the artifact namespace into the run
// it belongs to, its artifact ID, and the segment it lies in: a Volume's
// object, <source>/<category>/<day>/<run_id>/volumes/<artifact_id>/...,
// whose remainder is Lode's and is not read, or a commit index entry,
// <source>/<category>/<day>/<run_id>/commits/<artifact_id>. It is false for
// a key in any other shape, or whose components the write path could not
// have encoded (CONTRACT_LODE.md §Key Encoding).
func artifactKey(rel string) (reader.Key, string, string, bool) {
	parts := strings.SplitN(rel, "/", 7)
	volume := len(parts) == 7 && parts[4] == volumesSegment
	entry := len(parts) == 6 && parts[4] == commitsSegment
	if (!volume && !entry) || qlode.ValidateArtifactID(parts[5]) != nil {
		return reader.Key{}, "", "", false
	}
	var decoded [4]string
	for i, part := range parts[:4] {
		v, err := url.PathUnescape(part)
		if err != nil || url.PathEscape(v) != part || qlode.ValidateKeyComponent("key", v) != nil {
			return reader.Key{}, "", "", false
		}
		decoded[i] = v
	}
	if _, err := time.Parse(time.DateOnly, decoded[2]); err != nil {
		return reader.Key{}, "", "", false // the write path writes only YYYY-MM-DD
	}
	return reader.Key{Source: decoded[0], Category: decoded[1], Day: decoded[2], RunID: decoded[3]}, parts[5], parts[4], true
}

// volumePath names a Volume in errors.
func volumePath(cfg qlode.Config, id string) string {
	return fmt.Sprintf("%s%s/%s", qlode.RunArtifactDir(cfg), volumesSegment, id)
}
