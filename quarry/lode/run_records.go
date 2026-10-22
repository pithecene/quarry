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

	startRecordName = "start.json"
	endRecordName   = "end.json"
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
		for _, seg := range strings.FieldsFunc(c.value, func(r rune) bool { return r == '/' || r == '\\' }) {
			if seg == "." || seg == ".." {
				return fmt.Errorf("%w: %s must not contain a %q path segment: %q", ErrInvalidRunKey, c.name, seg, c.value)
			}
		}
	}
	return nil
}

// encode applies Lode's Hive partition value encoding.
func encode(v string) string { return url.PathEscape(v) }

// RunRecordDir is the run's directory in the run-record namespace:
// datasets/<dataset>/runs/<source>/<category>/<day>/<run_id>/.
func RunRecordDir(cfg Config) string {
	return "datasets/" + cfg.Dataset + "/runs/" +
		encode(cfg.Source) + "/" + encode(cfg.Category) + "/" + encode(cfg.Day) + "/" + encode(cfg.RunID) + "/"
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
	StartedAt     string  `json:"started_at"`
	QuarryVersion string  `json:"quarry_version"`
}

// RunEnd is what the runtime knows when a run ends; WriteEndRecord turns it
// into an end record.
type RunEnd struct {
	EndedAt time.Time
	Outcome types.OutcomeStatus
	// Terminal is the terminal event type received, or "" when none was
	// received.
	Terminal         types.EventType
	Message          string
	EventCount       int64
	MetricsPersisted bool
}

// EndRecord is a run's end record (CONTRACT_RUN.md §End Record).
type EndRecord struct {
	RecordKind       string  `json:"record_kind"`
	SchemaVersion    int     `json:"schema_version"`
	RunID            string  `json:"run_id"`
	EndedAt          string  `json:"ended_at"`
	Outcome          string  `json:"outcome"`
	Terminal         *string `json:"terminal"`
	Message          string  `json:"message,omitempty"`
	EventCount       int64   `json:"event_count"`
	MetricsPersisted bool    `json:"metrics_persisted"`
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
		StartedAt:     formatRecordTime(startedAt),
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
		Outcome:          string(end.Outcome),
		Message:          end.Message,
		EventCount:       end.EventCount,
		MetricsPersisted: end.MetricsPersisted,
	}
	if end.Terminal != "" {
		t := string(end.Terminal)
		rec.Terminal = &t
	}
	return rec
}

func formatRecordTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// WriteStartRecord writes the run's start record. Lode's Put is
// create-only, so a run ID that already has a start record is refused.
func (c *LodeClient) WriteStartRecord(ctx context.Context, meta *types.RunMeta, startedAt time.Time) error {
	if day := DeriveDay(startedAt); day != c.config.Day {
		return fmt.Errorf("%w: started_at %s is on %s, not the client's day %s", ErrInvalidRunKey, formatRecordTime(startedAt), day, c.config.Day)
	}
	return c.putRecord(ctx, RunRecordDir(c.config)+startRecordName, NewStartRecord(c.config, meta, startedAt))
}

// WriteEndRecord writes the run's end record.
func (c *LodeClient) WriteEndRecord(ctx context.Context, end RunEnd) error {
	return c.putRecord(ctx, RunRecordDir(c.config)+endRecordName, NewEndRecord(c.config, end))
}

func (c *LodeClient) putRecord(ctx context.Context, path string, record any) error {
	if err := ValidateRunKey(c.config.Dataset, c.config.Source, c.config.Category, c.config.RunID); err != nil {
		return err
	}
	if _, err := time.Parse(time.DateOnly, c.config.Day); err != nil {
		return fmt.Errorf("%w: day must be YYYY-MM-DD: %q", ErrInvalidRunKey, c.config.Day)
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("run record marshal failed: %w", err)
	}
	store, err := c.getOrCreateStore()
	if err != nil {
		return fmt.Errorf("run record store init failed: %w", err)
	}
	if err := store.Put(ctx, path, bytes.NewReader(data)); err != nil {
		return WrapWriteError(err, path)
	}
	return nil
}
