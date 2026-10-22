package lode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/pithecene-io/lode/lode"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

// Rebuild backfills run records for runs that predate them (CONTRACT_RUN.md
// §Backfilled Run Records). It derives runs from Lode's partition tree, the
// one place that enumerates partitions, and writes only what storage proves.

// RebuildOptions controls a run-record rebuild.
type RebuildOptions struct {
	// DryRun reports what would be written and writes nothing.
	DryRun bool
}

// RebuiltRun is one run the rebuild wrote records for, would write records
// for in a dry run, or skipped.
type RebuiltRun struct {
	Source    string `json:"source" yaml:"source"`
	Category  string `json:"category" yaml:"category"`
	Day       string `json:"day" yaml:"day"`
	RunID     string `json:"run_id" yaml:"run_id"`
	Partition string `json:"partition" yaml:"partition"`
	// WroteStart and WroteEnd say which records were written (or, in a dry
	// run, would be).
	WroteStart bool `json:"wrote_start" yaml:"wrote_start"`
	WroteEnd   bool `json:"wrote_end" yaml:"wrote_end"`
	// NoEnd says why no end record was written for an indexed run.
	NoEnd string `json:"no_end,omitempty" yaml:"no_end,omitempty"`
	// Skipped says why the run was not indexed at all.
	Skipped string `json:"skipped,omitempty" yaml:"skipped,omitempty"`
}

// RebuildReport summarizes a rebuild. Runs lists only runs that needed a
// record or were skipped; runs already indexed are counted, not listed.
type RebuildReport struct {
	Dataset        string       `json:"dataset" yaml:"dataset"`
	DryRun         bool         `json:"dry_run" yaml:"dry_run"`
	RunsScanned    int          `json:"runs_scanned" yaml:"runs_scanned"`
	AlreadyIndexed int          `json:"already_indexed" yaml:"already_indexed"`
	StartsWritten  int          `json:"starts_written" yaml:"starts_written"`
	EndsWritten    int          `json:"ends_written" yaml:"ends_written"`
	Skipped        int          `json:"skipped" yaml:"skipped"`
	Runs           []RebuiltRun `json:"runs" yaml:"runs"`
}

// Reasons a rebuild does not write a record.
const (
	noEndNoTerminal  = "no terminal event"
	noEndAmbiguous   = "more than one terminal event"
	noEndBadTs       = "terminal event has no RFC 3339 ts"
	skipNoLineage    = "no event carries the run's lineage"
	skipNonCanonical = "partition path is not canonically encoded"
	skipConcurrent   = "a start record appeared during the rebuild"
)

// metricsEventType is the event_type partition of a run's metrics record.
const metricsEventType = "metrics"

// legacyRun is one run found in the partition tree: its decoded key and the
// partition paths of its event types.
type legacyRun struct {
	cfg        Config
	prefix     string
	partitions map[string]string // event_type -> partition path
}

// RebuildRunRecords writes run records for every run in the dataset's
// partition tree that has none: a start record, and an end record when the
// run has exactly one terminal event. A run that already has a start record
// written by the runtime is never touched, and a backfilled run gains only a
// missing end record, so rerunning is idempotent. Store errors abort the
// rebuild and return the report so far; a rerun resumes.
func RebuildRunRecords(ctx context.Context, dataset string, factory lode.StoreFactory, opts RebuildOptions) (RebuildReport, error) {
	report := RebuildReport{Dataset: dataset, DryRun: opts.DryRun, Runs: []RebuiltRun{}}
	if err := ValidateRunKey(dataset, "x", "x", "x"); err != nil {
		return report, err
	}
	store, err := openStore(factory)
	if err != nil {
		return report, err
	}
	dr, err := NewPartitionReader(store)
	if err != nil {
		return report, WrapReadError(err, "datasets/"+dataset)
	}
	existing, err := listKeys(ctx, store, RunRecordPrefix(dataset))
	if err != nil {
		return report, err
	}
	runs, err := legacyRuns(ctx, dr, dataset)
	if err != nil {
		return report, err
	}
	rb := rebuilder{store: store, dr: dr, ds: lode.DatasetID(dataset), existing: existing, dryRun: opts.DryRun}
	for _, run := range runs {
		report.RunsScanned++
		out, indexed, err := rb.rebuild(ctx, run)
		if err != nil {
			// Report what this run got before the error, so the report
			// covers every record written.
			if out.WroteStart || out.WroteEnd {
				report.add(out, false)
			}
			return report, err
		}
		report.add(out, indexed)
	}
	return report, nil
}

func (r *RebuildReport) add(run RebuiltRun, alreadyIndexed bool) {
	switch {
	case alreadyIndexed:
		r.AlreadyIndexed++
		return
	case run.Skipped != "":
		r.Skipped++
	}
	if run.WroteStart {
		r.StartsWritten++
	}
	if run.WroteEnd {
		r.EndsWritten++
	}
	r.Runs = append(r.Runs, run)
}

func openStore(factory lode.StoreFactory) (lode.Store, error) {
	if factory == nil {
		return nil, errors.New("rebuild: nil store factory")
	}
	store, err := factory()
	if err != nil {
		return nil, fmt.Errorf("rebuild store init failed: %w", err)
	}
	if store == nil {
		return nil, errors.New("rebuild: store factory returned a nil store")
	}
	return store, nil
}

func listKeys(ctx context.Context, store lode.Store, prefix string) (map[string]bool, error) {
	keys, err := store.List(ctx, prefix)
	if errors.Is(err, lode.ErrNotFound) {
		return map[string]bool{}, nil
	}
	if err != nil {
		return nil, WrapReadError(err, prefix)
	}
	set := make(map[string]bool, len(keys))
	for _, k := range keys {
		set[k] = true
	}
	return set, nil
}

// legacyRuns groups the dataset's partitions by run, in key order.
func legacyRuns(ctx context.Context, dr lode.DatasetReader, dataset string) ([]legacyRun, error) {
	parts, err := dr.ListPartitions(ctx, lode.DatasetID(dataset), lode.PartitionListOptions{})
	if errors.Is(err, lode.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, WrapReadError(err, "datasets/"+dataset)
	}
	byPrefix := map[string]*legacyRun{}
	for _, p := range parts {
		values, ok := splitPartition(p.Path)
		if !ok {
			return nil, fmt.Errorf("rebuild: partition %q does not have the layout %s", p.Path, strings.Join(partitionKeys, "/"))
		}
		prefix := p.Path[:strings.LastIndex(p.Path, "/")+1]
		run, seen := byPrefix[prefix]
		if !seen {
			run = &legacyRun{prefix: prefix, partitions: map[string]string{}, cfg: Config{
				Dataset: dataset, Source: values[0], Category: values[1], Day: values[2], RunID: values[3],
			}}
			byPrefix[prefix] = run
		}
		run.partitions[values[4]] = p.Path
	}
	out := make([]legacyRun, 0, len(byPrefix))
	for _, run := range byPrefix {
		out = append(out, *run)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].prefix < out[j].prefix })
	return out, nil
}

// splitPartition decodes a partition path's values in layout order.
func splitPartition(path string) ([]string, bool) {
	segs := strings.Split(path, "/")
	if len(segs) != len(partitionKeys) {
		return nil, false
	}
	values := make([]string, len(segs))
	for i, seg := range segs {
		key, enc, ok := strings.Cut(seg, "=")
		if !ok || key != partitionKeys[i] {
			return nil, false
		}
		v, err := url.PathUnescape(enc)
		if err != nil {
			return nil, false
		}
		values[i] = v
	}
	return values, true
}

type rebuilder struct {
	store    lode.Store
	dr       lode.DatasetReader
	ds       lode.DatasetID
	existing map[string]bool
	dryRun   bool
}

// rebuild indexes one run. indexed reports a run that needs nothing.
func (b *rebuilder) rebuild(ctx context.Context, run legacyRun) (out RebuiltRun, indexed bool, err error) {
	cfg := run.cfg
	out = RebuiltRun{Source: cfg.Source, Category: cfg.Category, Day: cfg.Day, RunID: cfg.RunID, Partition: run.prefix}
	if err := validateRecordKey(cfg); err != nil {
		out.Skipped = err.Error()
		return out, false, nil
	}
	if PartitionPrefix(cfg) != run.prefix {
		out.Skipped = skipNonCanonical
		return out, false, nil
	}
	dir := RunRecordDir(cfg)
	if b.existing[dir+EndRecordName] && b.existing[dir+StartRecordName] {
		return out, true, nil
	}
	needStart := !b.existing[dir+StartRecordName]
	if !needStart {
		backfilled, err := b.isBackfilled(ctx, dir+StartRecordName)
		if err != nil || !backfilled {
			return out, true, err
		}
	}

	terminal, noEnd, err := b.terminalEvent(ctx, run)
	if err != nil {
		return out, false, err
	}
	out.NoEnd = noEnd
	if needStart {
		skipped, err := b.writeStart(ctx, run, terminal)
		if err != nil || skipped != "" {
			out.Skipped = skipped
			return out, false, err
		}
		out.WroteStart = true
	}
	if terminal == nil {
		return out, !out.WroteStart && out.NoEnd == noEndNoTerminal, nil
	}
	out.WroteEnd, err = b.writeEnd(ctx, run, terminal)
	return out, false, err
}

func (b *rebuilder) isBackfilled(ctx context.Context, key string) (bool, error) {
	rc, err := b.store.Get(ctx, key)
	if err != nil {
		return false, WrapReadError(err, key)
	}
	defer iox.DiscardClose(rc)
	data, err := io.ReadAll(rc)
	if err != nil {
		return false, WrapReadError(err, key)
	}
	var start StartRecord
	if err := json.Unmarshal(data, &start); err != nil {
		return false, fmt.Errorf("run record %s: %w", key, err)
	}
	return start.Backfilled, nil
}

// terminalEvent returns the run's one terminal event, or the reason there is
// none to end the run with.
func (b *rebuilder) terminalEvent(ctx context.Context, run legacyRun) (map[string]any, string, error) {
	var terminals []map[string]any
	for _, et := range []types.EventType{types.EventTypeRunComplete, types.EventTypeRunError} {
		path, ok := run.partitions[string(et)]
		if !ok {
			continue
		}
		recs, err := ReadPartitionRecords(ctx, b.dr, b.ds, path)
		if err != nil {
			return nil, "", err
		}
		for _, rec := range recs {
			if rec["record_kind"] == RecordKindEvent && rec["type"] == string(et) {
				terminals = append(terminals, rec)
			}
		}
	}
	switch len(terminals) {
	case 0:
		return nil, noEndNoTerminal, nil
	case 1:
		if _, ok := recordTime(terminals[0]); !ok {
			return nil, noEndBadTs, nil
		}
		return terminals[0], "", nil
	}
	return nil, noEndAmbiguous, nil
}

// writeStart writes the run's backfilled start record, or returns why it
// skipped the run.
func (b *rebuilder) writeStart(ctx context.Context, run legacyRun, terminal map[string]any) (string, error) {
	evidence := terminal
	if evidence == nil {
		found, err := b.lineageEvent(ctx, run)
		if err != nil {
			return "", err
		}
		evidence = found
	}
	meta, ok := lineage(evidence)
	if !ok {
		return skipNoLineage, nil
	}
	rec := NewStartRecord(run.cfg, meta, time.Time{})
	rec.StartedAt = nil
	rec.Backfilled = true
	wrote, err := b.put(ctx, run.cfg, StartRecordName, rec)
	if err != nil || wrote {
		return "", err
	}
	return skipConcurrent, nil
}

// writeEnd writes the run's backfilled end record from its terminal event.
func (b *rebuilder) writeEnd(ctx context.Context, run legacyRun, terminal map[string]any) (bool, error) {
	endedAt, _ := recordTime(terminal)
	t, _ := terminal["type"].(string)
	_, metrics := run.partitions[metricsEventType]
	rec := EndRecord{
		RecordKind:       RecordKindRunEnd,
		SchemaVersion:    RunRecordSchemaVersion,
		RunID:            run.cfg.RunID,
		EndedAt:          formatRecordTime(endedAt),
		Terminal:         &t,
		MetricsPersisted: metrics,
		Backfilled:       true,
	}
	return b.put(ctx, run.cfg, EndRecordName, rec)
}

// put writes one record unless this is a dry run. A record that already
// exists was written concurrently: it is reported as not written.
func (b *rebuilder) put(ctx context.Context, cfg Config, name string, rec any) (bool, error) {
	if b.dryRun {
		return !b.existing[RunRecordDir(cfg)+name], nil
	}
	err := putRunRecord(ctx, b.store, cfg, name, rec)
	if errors.Is(err, ErrPathExists) {
		return false, nil
	}
	return err == nil, err
}

// lineageEvent returns the first event record of the run, reading its event
// type partitions in name order; metrics records carry no lineage.
func (b *rebuilder) lineageEvent(ctx context.Context, run legacyRun) (map[string]any, error) {
	names := make([]string, 0, len(run.partitions))
	for et := range run.partitions {
		if et != metricsEventType {
			names = append(names, et)
		}
	}
	sort.Strings(names)
	for _, et := range names {
		recs, err := ReadPartitionRecords(ctx, b.dr, b.ds, run.partitions[et])
		if err != nil {
			return nil, err
		}
		for _, rec := range recs {
			if _, ok := lineage(rec); ok {
				return rec, nil
			}
		}
	}
	return nil, nil
}

// lineage reads the lineage fields every event and artifact commit record
// carries.
func lineage(rec map[string]any) (*types.RunMeta, bool) {
	if kind := rec["record_kind"]; kind != RecordKindEvent && kind != RecordKindArtifactEvent {
		return nil, false
	}
	attempt, ok := rec["attempt"].(float64)
	if !ok || attempt < 1 || attempt != float64(int(attempt)) {
		return nil, false
	}
	meta := &types.RunMeta{Attempt: int(attempt), JobID: optString(rec["job_id"]), ParentRunID: optString(rec["parent_run_id"])}
	return meta, true
}

func optString(v any) *string {
	if s, ok := v.(string); ok && s != "" {
		return &s
	}
	return nil
}

func recordTime(rec map[string]any) (time.Time, bool) {
	s, _ := rec["ts"].(string)
	t, err := time.Parse(time.RFC3339Nano, s)
	return t, err == nil
}
