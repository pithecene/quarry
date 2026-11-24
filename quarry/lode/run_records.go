package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

// Run records are small JSON objects Quarry writes outside Lode's partition
// tree, in a namespace it owns inside the dataset (CONTRACT_LODE.md §Run
// Record Namespace). Readers find runs from them instead of enumerating
// partitions. Their schemas are in CONTRACT_RUN.md §Run Records.

const (
	// RecordKindRunStart is the record_kind of a start record.
	RecordKindRunStart = "run_start"
	// RecordKindRunEnd is the record_kind of an end record.
	RecordKindRunEnd = "run_end"
	// RunRecordSchemaVersion is the schema_version written in run records.
	RunRecordSchemaVersion = 1

	// StartRecordName is the object name of a run's start record.
	StartRecordName = "start.json"
	// EndRecordName is the object name of a run's end record.
	EndRecordName = "end.json"
)

// ErrInvalidRunKey is returned when a dataset, source, category, or run ID
// cannot form a safe storage key (CONTRACT_LODE.md §Key Encoding).
var ErrInvalidRunKey = errors.New("invalid run key")

// ValidateRunKey rejects values that cannot form a safe storage key: empty,
// ".", or ".." for any component; for the dataset ID, which is used unencoded
// as Lode's own directory name, any "/" or "\"; and for source, category, and
// run ID, any "." or ".." path segment.
func ValidateRunKey(dataset, source, category, runID string) error {
	for _, c := range []struct{ name, value string }{
		{"dataset", dataset}, {"source", source}, {"category", category}, {"run_id", runID},
	} {
		switch c.value {
		case "", ".", "..":
			return fmt.Errorf("%w: %s must not be %q", ErrInvalidRunKey, c.name, c.value)
		}
	}
	if strings.ContainsAny(dataset, `/\`) {
		return fmt.Errorf("%w: dataset must not contain '/' or '\\': %q", ErrInvalidRunKey, dataset)
	}
	// Sidecar file paths still interpolate these values unencoded, so a "." or ".."
	// path segment could place a run's files under another run's partition.
	for _, c := range []struct{ name, value string }{
		{"source", source}, {"category", category}, {"run_id", runID},
	} {
		if err := ValidateKeyComponent(c.name, c.value); err != nil {
			return err
		}
	}
	return nil
}

// ValidateKeyComponent rejects a run key component (source, category, day,
// or run ID) that is empty, "." or "..", or has a "." or ".." path segment.
// Readers apply it to selectors before building key prefixes, since stores
// normalize such segments and would widen the prefix.
func ValidateKeyComponent(name, value string) error {
	switch value {
	case "", ".", "..":
		return fmt.Errorf("%w: %s must not be %q", ErrInvalidRunKey, name, value)
	}
	for _, seg := range strings.FieldsFunc(value, func(r rune) bool { return r == '/' || r == '\\' }) {
		if seg == "." || seg == ".." {
			return fmt.Errorf("%w: %s must not contain a %q path segment: %q", ErrInvalidRunKey, name, seg, value)
		}
	}
	return nil
}

// encode applies Lode's Hive partition value encoding.
func encode(v string) string { return url.PathEscape(v) }

// RunRecordDir is the run's directory in the run-record namespace:
// datasets/<dataset>/runs/<source>/<category>/<day>/<run_id>/.
func RunRecordDir(cfg Config) string {
	return RunRecordPrefix(cfg.Dataset, cfg.Source, cfg.Category, cfg.Day, cfg.RunID)
}

// RunRecordPrefix is a key prefix in the dataset's run-record namespace for
// the leading components given, in order source, category, day, run_id:
// datasets/<dataset>/runs/<enc>/.../. With no components it is the whole
// namespace.
func RunRecordPrefix(dataset string, components ...string) string {
	return namespacePrefix("datasets/"+dataset+"/runs", components...)
}

// RunArtifactDir is the run's directory in the artifact namespace, which
// holds the run's artifact Volumes (CONTRACT_LODE.md §Artifact Storage):
// artifacts/<dataset>/<source>/<category>/<day>/<run_id>/.
//
// The namespace lies beside datasets/, not inside the dataset's directory:
// a Lode Dataset rejects, as a history written under another layout, any
// manifest it finds in its directory that its layout does not persist,
// and a Volume's manifests are such manifests.
func RunArtifactDir(cfg Config) string {
	return ArtifactPrefix(cfg.Dataset, cfg.Source, cfg.Category, cfg.Day, cfg.RunID)
}

// ArtifactPrefix is a key prefix in the dataset's artifact namespace for
// the leading components given, in order source, category, day, run_id,
// encoded exactly as RunRecordPrefix encodes them:
// artifacts/<dataset>/<enc>/.../. With no components it is the whole
// namespace.
func ArtifactPrefix(dataset string, components ...string) string {
	return namespacePrefix("artifacts/"+dataset, components...)
}

// namespacePrefix is a key prefix under root for the leading components
// given, each Hive-encoded and followed by "/".
func namespacePrefix(root string, components ...string) string {
	var b strings.Builder
	b.WriteString(root + "/")
	for _, c := range components {
		b.WriteString(encode(c) + "/")
	}
	return b.String()
}

// PartitionPrefix is the run's Lode partition prefix relative to the
// dataset's partition root, Hive-encoded exactly as Lode writes it:
// source=<s>/category=<c>/day=<d>/run_id=<r>/.
func PartitionPrefix(cfg Config) string {
	return "source=" + encode(cfg.Source) + "/category=" + encode(cfg.Category) +
		"/day=" + encode(cfg.Day) + "/run_id=" + encode(cfg.RunID) + "/"
}

// StartRecord is a run's start record (CONTRACT_RUN.md §Start Record).
type StartRecord struct {
	RecordKind    string  `json:"record_kind"`
	SchemaVersion int     `json:"schema_version"`
	RunID         string  `json:"run_id"`
	JobID         *string `json:"job_id,omitempty"`
	ParentRunID   *string `json:"parent_run_id,omitempty"`
	Attempt       int     `json:"attempt"`
	Dataset       string  `json:"dataset"`
	Source        string  `json:"source"`
	Category      string  `json:"category"`
	Day           string  `json:"day"`
	Partition     string  `json:"partition"`
	// StartedAt is null on a backfilled record: the stored data does not
	// establish it.
	StartedAt *string `json:"started_at"`
	// DeadlineAt is the instant the run's max duration ends, omitted when
	// the run declares none (CONTRACT_RUN.md §Run Deadline).
	DeadlineAt    *string `json:"deadline_at,omitempty"`
	QuarryVersion string  `json:"quarry_version"`
	Backfilled    bool    `json:"backfilled"`
}

// RunEnd is what the runtime knows when a run ends; WriteEndRecord turns it
// into an end record.
type RunEnd struct {
	EndedAt time.Time
	Outcome types.OutcomeStatus
	// Terminal is the terminal event type received, or "" when none was
	// received.
	Terminal   types.EventType
	Message    string
	EventCount int64
	// Metrics is the run's final snapshot, or nil when the run collects none.
	Metrics *metrics.Snapshot
	// TerminalSummary is the encoded payload of the terminal event received
	// (EncodeTerminalSummary), or nil.
	TerminalSummary json.RawMessage
}

// EndRecord is a run's end record (CONTRACT_RUN.md §End Record).
type EndRecord struct {
	RecordKind    string `json:"record_kind"`
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	EndedAt       string `json:"ended_at"`
	// Outcome and EventCount are null on a backfilled record: the stored data
	// establishes neither the runtime's outcome nor the events it processed.
	Outcome    *string `json:"outcome"`
	Terminal   *string `json:"terminal"`
	Message    string  `json:"message,omitempty"`
	EventCount *int64  `json:"event_count"`
	// MetricsPersisted reports whether the run's metrics were stored, in this
	// record or in a metrics record.
	MetricsPersisted bool `json:"metrics_persisted"`
	// Metrics is omitted on a backfilled record and when the run collects none.
	Metrics map[string]any `json:"metrics,omitempty"`
	// TerminalSummary is omitted without a terminal event, and when JSON cannot
	// represent the payload.
	TerminalSummary json.RawMessage `json:"terminal_summary,omitempty"`
	// SidecarFiles is every sidecar file the run wrote.
	SidecarFiles []SidecarFileRef `json:"sidecar_files,omitempty"`
	Backfilled   bool             `json:"backfilled"`
}

// NewStartRecord builds the start record for the run cfg describes.
func NewStartRecord(cfg Config, meta *types.RunMeta, startedAt time.Time) StartRecord {
	return StartRecord{
		RecordKind:    RecordKindRunStart,
		SchemaVersion: RunRecordSchemaVersion,
		RunID:         cfg.RunID,
		JobID:         meta.JobID,
		ParentRunID:   meta.ParentRunID,
		Attempt:       meta.Attempt,
		Dataset:       cfg.Dataset,
		Source:        cfg.Source,
		Category:      cfg.Category,
		Day:           cfg.Day,
		Partition:     PartitionPrefix(cfg),
		StartedAt:     new(formatRecordTime(startedAt)),
		QuarryVersion: types.Version,
	}
}

// EncodeTerminalSummary encodes a terminal event's payload for RunEnd. It
// refuses one that an end record cannot hold, such as one with a NaN or nested
// too deeply to read back, because the record must be written without it.
func EncodeTerminalSummary(payload map[string]any) (json.RawMessage, error) {
	summary, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	record, err := json.Marshal(EndRecord{TerminalSummary: summary})
	if err != nil {
		return nil, err
	}
	// Encoding checks the payload apart from the record, whose nesting it adds to.
	if !json.Valid(record) {
		return nil, errors.New("an end record with the payload could not be read back")
	}
	return summary, nil
}

// NewEndRecord builds the end record for the run cfg describes.
func NewEndRecord(cfg Config, end RunEnd) EndRecord {
	rec := EndRecord{
		RecordKind:       RecordKindRunEnd,
		SchemaVersion:    RunRecordSchemaVersion,
		RunID:            cfg.RunID,
		EndedAt:          formatRecordTime(end.EndedAt),
		Outcome:          new(string(end.Outcome)),
		Message:          end.Message,
		EventCount:       new(end.EventCount),
		MetricsPersisted: end.Metrics != nil,
	}
	if end.Metrics != nil {
		rec.Metrics = metricsFields(*end.Metrics)
		if len(end.Metrics.FlushTriggers) > 0 {
			rec.Metrics["flush_triggers"] = maps.Clone(end.Metrics.FlushTriggers)
		}
	}
	rec.TerminalSummary = end.TerminalSummary
	if end.Terminal != "" {
		t := string(end.Terminal)
		rec.Terminal = &t
	}
	return rec
}

func formatRecordTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// WriteStartRecord writes the run's start record, with the instant its
// deadline passes when it declares one. Lode's Put is create-only, so Lode
// refuses a run ID that already has a start record.
func (c *LodeClient) WriteStartRecord(ctx context.Context, meta *types.RunMeta, startedAt time.Time, deadlineAt *time.Time) error {
	if day := DeriveDay(startedAt); day != c.config.Day {
		return fmt.Errorf("%w: started_at %s is on %s, not the client's day %s", ErrInvalidRunKey, formatRecordTime(startedAt), day, c.config.Day)
	}
	record := NewStartRecord(c.config, meta, startedAt)
	if deadlineAt != nil {
		record.DeadlineAt = new(formatRecordTime(*deadlineAt))
	}
	return c.putRecord(ctx, StartRecordName, record)
}

// WriteEndRecord writes the run's end record. A repeated call with the same end
// writes the same bytes, so it succeeds when an earlier one persisted.
func (c *LodeClient) WriteEndRecord(ctx context.Context, end RunEnd) error {
	rec := NewEndRecord(c.config, end)
	c.mu.Lock()
	rec.SidecarFiles = slices.Clone(c.files)
	c.mu.Unlock()
	data, err := encodeRunRecord(c.config, rec)
	if err != nil {
		return err
	}
	path := RunRecordDir(c.config) + EndRecordName
	if err := c.putReconciled(ctx, path, data); err != nil {
		return WrapWriteError(err, path)
	}
	return nil
}

func (c *LodeClient) putRecord(ctx context.Context, name string, record any) error {
	return putRunRecord(ctx, c.store, c.config, name, record)
}

// errMalformedRecord marks a run record that was read and is not its JSON form.
var errMalformedRecord = errors.New("malformed run record")

// readEndFields returns the fields of the run's end record, or nil when the run
// has none.
func readEndFields(ctx context.Context, store lode.Store, cfg Config) (map[string]json.RawMessage, error) {
	key := RunRecordDir(cfg) + EndRecordName
	rc, err := store.Get(ctx, key)
	if errors.Is(err, lode.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, WrapReadError(err, key)
	}
	defer iox.DiscardClose(rc)
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, WrapReadError(err, key)
	}
	var rec map[string]json.RawMessage
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("%w %s: %w", errMalformedRecord, key, err)
	}
	return rec, nil
}

// ErrMalformedSidecars marks an end record whose sidecar files cannot be read as
// the run's, as against a record that could not be fetched.
var ErrMalformedSidecars = errors.New("malformed sidecar files in end record")

// ReadEndSidecars returns the sidecar files that the run's end record holds, each
// once, or none when the run has no end record or its record holds none.
func ReadEndSidecars(ctx context.Context, store lode.Store, cfg Config) ([]SidecarFileRef, error) {
	rec, err := readEndFields(ctx, store, cfg)
	if errors.Is(err, errMalformedRecord) {
		return nil, fmt.Errorf("%w: %w", ErrMalformedSidecars, err)
	}
	if err != nil {
		return nil, err
	}
	return endSidecars(rec, cfg)
}

// endSidecars returns the sidecar files of an end record, each once.
func endSidecars(rec map[string]json.RawMessage, cfg Config) ([]SidecarFileRef, error) {
	key := RunRecordDir(cfg) + EndRecordName
	files, err := decodeSidecarFiles(rec)
	if err != nil {
		return nil, fmt.Errorf("%w %s: %w", ErrMalformedSidecars, key, err)
	}
	seen := make(map[SidecarFileRef]bool, len(files))
	unique := files[:0]
	for i, file := range files {
		if file.Path != SidecarFilesPrefix(cfg)+file.Filename {
			return nil, fmt.Errorf("%w %s: entry %d: path %q is not the run's", ErrMalformedSidecars, key, i, file.Path)
		}
		if !seen[file] {
			seen[file] = true
			unique = append(unique, file)
		}
	}
	return unique, nil
}

// validateRecordKey rejects a run whose components cannot form a safe
// record key or whose day is not YYYY-MM-DD.
func validateRecordKey(cfg Config) error {
	if err := ValidateRunKey(cfg.Dataset, cfg.Source, cfg.Category, cfg.RunID); err != nil {
		return err
	}
	if _, err := time.Parse(time.DateOnly, cfg.Day); err != nil {
		return fmt.Errorf("%w: day must be YYYY-MM-DD: %q", ErrInvalidRunKey, cfg.Day)
	}
	return nil
}

// encodeRunRecord encodes a record of the run cfg names.
func encodeRunRecord(cfg Config, record any) ([]byte, error) {
	if err := validateRecordKey(cfg); err != nil {
		return nil, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("run record marshal failed: %w", err)
	}
	return data, nil
}

// putRunRecord writes one record of the run cfg names, create-only.
func putRunRecord(ctx context.Context, store lode.Store, cfg Config, name string, record any) error {
	data, err := encodeRunRecord(cfg, record)
	if err != nil {
		return err
	}
	path := RunRecordDir(cfg) + name
	if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		return WrapWriteError(err, path)
	}
	return nil
}
