package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pithecene-io/lode/lode"

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
	// Sidecar file paths still interpolate these values raw, so a "." or ".."
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
	var b strings.Builder
	b.WriteString("datasets/" + dataset + "/runs/")
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
	// StartedAt is null on a backfilled record: storage does not prove it.
	StartedAt     *string `json:"started_at"`
	QuarryVersion string  `json:"quarry_version"`
	Backfilled    bool    `json:"backfilled"`
}

// RunEnd is what the runtime knows when a run ends; WriteEndRecord turns it
// into an end record.
type RunEnd struct {
	EndedAt time.Time
	Outcome types.OutcomeStatus
	// Terminal is the terminal event type received, or "" when none was.
	Terminal         types.EventType
	Message          string
	EventCount       int64
	MetricsPersisted bool
}

// EndRecord is a run's end record (CONTRACT_RUN.md §End Record).
type EndRecord struct {
	RecordKind    string `json:"record_kind"`
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	EndedAt       string `json:"ended_at"`
	// Outcome and EventCount are null on a backfilled record: storage does
	// not prove the runtime's classification or what it processed.
	Outcome          *string `json:"outcome"`
	Terminal         *string `json:"terminal"`
	Message          string  `json:"message,omitempty"`
	EventCount       *int64  `json:"event_count"`
	MetricsPersisted bool    `json:"metrics_persisted"`
	Backfilled       bool    `json:"backfilled"`
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
		StartedAt:     ptr(formatRecordTime(startedAt)),
		QuarryVersion: types.Version,
	}
}

// NewEndRecord builds the end record for the run cfg describes.
func NewEndRecord(cfg Config, end RunEnd) EndRecord {
	rec := EndRecord{
		RecordKind:       RecordKindRunEnd,
		SchemaVersion:    RunRecordSchemaVersion,
		RunID:            cfg.RunID,
		EndedAt:          formatRecordTime(end.EndedAt),
		Outcome:          ptr(string(end.Outcome)),
		Message:          end.Message,
		EventCount:       ptr(end.EventCount),
		MetricsPersisted: end.MetricsPersisted,
	}
	if end.Terminal != "" {
		t := string(end.Terminal)
		rec.Terminal = &t
	}
	return rec
}

func ptr[T any](v T) *T { return &v }

func formatRecordTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// WriteStartRecord writes the run's start record. Lode's Put is
// create-only, so a run ID that already has a start record is refused.
func (c *LodeClient) WriteStartRecord(ctx context.Context, meta *types.RunMeta, startedAt time.Time) error {
	if day := DeriveDay(startedAt); day != c.config.Day {
		return fmt.Errorf("%w: started_at %s is on %s, not the client's day %s", ErrInvalidRunKey, formatRecordTime(startedAt), day, c.config.Day)
	}
	return c.putRecord(ctx, StartRecordName, NewStartRecord(c.config, meta, startedAt))
}

// WriteEndRecord writes the run's end record.
func (c *LodeClient) WriteEndRecord(ctx context.Context, end RunEnd) error {
	return c.putRecord(ctx, EndRecordName, NewEndRecord(c.config, end))
}

func (c *LodeClient) putRecord(ctx context.Context, name string, record any) error {
	store, err := c.getOrCreateStore()
	if err != nil {
		return fmt.Errorf("run record store init failed: %w", err)
	}
	return putRunRecord(ctx, store, c.config, name, record)
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

// putRunRecord writes one record of the run cfg names, create-only.
func putRunRecord(ctx context.Context, store lode.Store, cfg Config, name string, record any) error {
	if err := validateRecordKey(cfg); err != nil {
		return err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("run record marshal failed: %w", err)
	}
	path := RunRecordDir(cfg) + name
	if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		return WrapWriteError(err, path)
	}
	return nil
}
