package cmd

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	lodelib "github.com/pithecene/lode/lode"
	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/render"
	"github.com/pithecene/quarry/lode"
	runreader "github.com/pithecene/quarry/reader"
)

// defaultReadTimeout bounds a storage-backed listing unless --read-timeout
// says otherwise. A run's events are one segment each under the strict
// policy, so a listing of many events makes many requests.
const defaultReadTimeout = 5 * time.Minute

// readTimeoutFlag is the bound on the whole read of a listing command.
func readTimeoutFlag() cli.Flag {
	return &cli.DurationFlag{
		Name:  "read-timeout",
		Usage: "Bound on the whole read of storage, e.g. 5m, 30m (default 5m)",
		Value: defaultReadTimeout,
	}
}

// readContext bounds a listing by --read-timeout.
func readContext(c *cli.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.Context, c.Duration("read-timeout"))
}

// storageReadFlags are the storage flags of commands that read Lode
// storage. They have the same meaning as on `run`.
func storageReadFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "storage-dataset", Usage: "Lode dataset ID (default: \"quarry\")", Value: lode.DefaultDataset},
		&cli.StringFlag{Name: "storage-backend", Usage: "Storage backend: fs or s3"},
		&cli.StringFlag{Name: "storage-path", Usage: "Storage path (fs: directory, s3: bucket/prefix)"},
		&cli.StringFlag{Name: "storage-region", Usage: "AWS region for S3 backend"},
		&cli.StringFlag{Name: "storage-endpoint", Usage: "Custom S3 endpoint URL for S3-compatible providers"},
		&cli.BoolFlag{Name: "storage-s3-path-style", Usage: "Force path-style addressing for S3 (required by many S3-compatible providers)"},
		&cli.DurationFlag{Name: "storage-attempt-timeout", Usage: "Longest stall of an S3 request before its attempt fails and the SDK retries it (default 60s)"},
	}
}

// runSelectorFlags select runs by key prefix, in order.
func runSelectorFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "source", Usage: "Source selector"},
		&cli.StringFlag{Name: "category", Usage: "Category selector (requires --source)"},
		&cli.StringFlag{Name: "day", Usage: "Partition day selector, YYYY-MM-DD (requires --category)"},
	}
}

// storageAccess is what a command does to storage, as its errors name it.
type storageAccess string

const (
	readsStorage  storageAccess = "reads"
	writesStorage storageAccess = "writes"
)

// requireStorage returns a cli exit error unless the storage flags name a
// store. The stats, inspect, list, and read commands answer only from what
// storage holds, never from invented data, and index rebuild and prune write
// it.
func requireStorage(c *cli.Context, command string, access storageAccess) error {
	if c.String("storage-backend") != "" && c.String("storage-path") != "" {
		return nil
	}
	prefix := command + " " + string(access) + " storage: "
	if anyStorageFlagSet(c) {
		return cli.Exit(prefix+"both --storage-backend and --storage-path are required", 1)
	}
	return cli.Exit(prefix+"--storage-backend and --storage-path are required", 1)
}

func anyStorageFlagSet(c *cli.Context) bool {
	for _, f := range storageReadFlags() {
		if c.IsSet(f.Names()[0]) {
			return true
		}
	}
	return false
}

// newRunReader builds a run-record reader from the storage flags.
func newRunReader(c *cli.Context, opts ...runreader.Option) (*runreader.Reader, error) {
	factory, err := storeFactoryFrom(c)
	if err != nil {
		return nil, err
	}
	return runreader.New(c.String("storage-dataset"), factory, opts...)
}

// newStoreFactory builds a store factory. A test replaces it to count the
// stores a command builds.
var newStoreFactory = lode.NewStoreFactory

// storeFactoryFrom builds the store factory the storage flags describe.
func storeFactoryFrom(c *cli.Context) (lodelib.Factory[lodelib.RangeStore], error) {
	backend := c.String("storage-backend")
	endpoint, pathStyle := c.String("storage-endpoint"), c.Bool("storage-s3-path-style")
	if backend == "fs" && (endpoint != "" || pathStyle) {
		fmt.Fprintf(os.Stderr, "Warning: --storage-endpoint and --storage-s3-path-style are ignored for fs backend\n")
	}
	return newStoreFactory(backend, c.String("storage-path"), lode.S3Config{
		Region:         c.String("storage-region"),
		Endpoint:       endpoint,
		UsePathStyle:   pathStyle,
		AttemptTimeout: c.Duration("storage-attempt-timeout"),
	})
}

func selectorFrom(c *cli.Context) runreader.Selector {
	return runreader.Selector{Source: c.String("source"), Category: c.String("category"), Day: c.String("day")}
}

// listRunsFromRecords lists runs from run records (CONTRACT_CLI.md §list runs).
func listRunsFromRecords(c *cli.Context, r *render.Renderer) error {
	rr, err := newRunReader(c)
	if err != nil {
		return cli.Exit(fmt.Sprintf("failed to initialize storage reader: %v", err), 1)
	}
	ctx, cancel := readContext(c)
	defer cancel()

	page, err := rr.ListRuns(ctx, selectorFrom(c), runreader.ListOptions{
		State: runreader.State(c.String("state")),
		After: c.String("after"),
		Limit: c.Int("limit"),
	})
	if err != nil {
		return cli.Exit(fmt.Sprintf("list runs: %v", err), 1)
	}
	if r.Format() == render.FormatTable {
		if page.Next != "" {
			fmt.Fprintf(os.Stderr, "More runs: --after %s\n", page.Next)
		}
		return r.Render(page.Runs)
	}
	return r.Render(page)
}

func listEventsCommand() *cli.Command {
	return &cli.Command{
		Name:   "events",
		Usage:  "List a run's events in sequence order (reads storage)",
		Flags:  append(append(ReadOnlyFlags(), runLocatorFlags("Run whose events to list (required)")...), readTimeoutFlag()),
		Action: listEventsAction,
	}
}

// listEventsResponse is the JSON shape of `list events`.
type listEventsResponse struct {
	Run    runreader.Run    `json:"run" yaml:"run"`
	Events []map[string]any `json:"events" yaml:"events"`
}

// locatedRun opens the storage the flags describe and reads the run that
// --source, --category, --day, and --run-id locate. command names the
// command in its errors, which are cli exit errors.
func locatedRun(ctx context.Context, c *cli.Context, command string, opts ...runreader.Option) (*runreader.Reader, runreader.Run, error) {
	return locatedRunID(ctx, c, command, c.String("run-id"), opts...)
}

// locatedRunID is locatedRun for a command that takes the run ID as an
// argument rather than as --run-id.
func locatedRunID(ctx context.Context, c *cli.Context, command, runID string, opts ...runreader.Option) (*runreader.Reader, runreader.Run, error) {
	rr, run, _, err := locateRun(ctx, c, command, runID, opts...)
	return rr, run, err
}

// locateRun is locatedRunID that also returns the store factory, so a command
// that reads more of the run's storage builds the store once.
// sharedStore returns a factory that builds the store on its first call and
// returns that store, or its error, on every later call, so the run reader and
// a command that reads more of the run's storage share one store.
func sharedStore(f lodelib.Factory[lodelib.RangeStore]) lodelib.Factory[lodelib.RangeStore] {
	return sync.OnceValues(f)
}

func locateRun(ctx context.Context, c *cli.Context, command, runID string, opts ...runreader.Option) (*runreader.Reader, runreader.Run, lodelib.Factory[lodelib.RangeStore], error) {
	if err := requireStorage(c, command, readsStorage); err != nil {
		return nil, runreader.Run{}, nil, err
	}
	sel := selectorFrom(c)
	if sel.Source == "" || sel.Category == "" || sel.Day == "" {
		return nil, runreader.Run{}, nil, cli.Exit(command+" requires --source, --category, and --day to locate the run", 1)
	}
	factory, err := storeFactoryFrom(c)
	if err != nil {
		return nil, runreader.Run{}, nil, cli.Exit(fmt.Sprintf("failed to initialize storage reader: %v", err), 1)
	}
	factory = sharedStore(factory)
	rr, err := runreader.New(c.String("storage-dataset"), factory, opts...)
	if err != nil {
		return nil, runreader.Run{}, nil, cli.Exit(fmt.Sprintf("failed to initialize storage reader: %v", err), 1)
	}
	run, err := rr.FindRun(ctx, sel, runID)
	if err != nil {
		return nil, runreader.Run{}, nil, cli.Exit(fmt.Sprintf("%s: %v", command, err), 1)
	}
	return rr, run, factory, nil
}

// runListing is a list command over one located run: read returns the
// run's records, table gives the rows of `--format table`, and response gives
// the JSON and YAML shape. list events and list artifacts differ only in these
// three.
type runListing[T any] struct {
	command  string
	read     func(*runreader.Reader, context.Context, runreader.Run) (T, error)
	table    func(T) any
	response func(runreader.Run, T) any
}

func (l runListing[T]) action(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}
	ctx, cancel := readContext(c)
	defer cancel()
	rr, run, err := locatedRun(ctx, c, l.command)
	if err != nil {
		return err
	}
	records, err := l.read(rr, ctx, run)
	if err != nil {
		return cli.Exit(fmt.Sprintf("%s: %v", l.command, err), 1)
	}
	if r.Format() == render.FormatTable {
		return r.Render(l.table(records))
	}
	return r.Render(l.response(run, records))
}

var listEventsAction = runListing[[]map[string]any]{
	command: "list events",
	read:    (*runreader.Reader).ReadEvents,
	table:   func(events []map[string]any) any { return eventSummaries(events) },
	response: func(run runreader.Run, events []map[string]any) any {
		return listEventsResponse{Run: run, Events: events}
	},
}.action

// runLocatorFlags locate one run's records directly: the storage flags,
// the run's key selectors, and --run-id, whose usage names what the run is
// read for.
func runLocatorFlags(runIDUsage string) []cli.Flag {
	flags := append(storageReadFlags(), runSelectorFlags()...)
	return append(flags, &cli.StringFlag{Name: "run-id", Usage: runIDUsage, Required: true})
}

func listArtifactsCommand() *cli.Command {
	return &cli.Command{
		Name:   "artifacts",
		Usage:  "List a run's artifacts from their commit records (reads storage)",
		Flags:  append(append(ReadOnlyFlags(), runLocatorFlags("Run whose artifacts to list (required)")...), readTimeoutFlag()),
		Action: listArtifactsAction,
	}
}

// listArtifactsResponse is the JSON shape of `list artifacts`.
type listArtifactsResponse struct {
	Run       runreader.Run        `json:"run" yaml:"run"`
	Artifacts []runreader.Artifact `json:"artifacts" yaml:"artifacts"`
}

var listArtifactsAction = runListing[[]runreader.Artifact]{
	command: "list artifacts",
	read:    (*runreader.Reader).ListArtifacts,
	table:   func(artifacts []runreader.Artifact) any { return artifacts },
	response: func(run runreader.Run, artifacts []runreader.Artifact) any {
		return listArtifactsResponse{Run: run, Artifacts: artifacts}
	},
}.action

// eventSummary is one row of `list events --format table`. Records differ in
// shape (events, artifact commits), so the table shows the fields every
// record has; json and yaml carry the full records.
type eventSummary struct {
	Seq        any `json:"seq" yaml:"seq"`
	Type       any `json:"type" yaml:"type"`
	RecordKind any `json:"record_kind" yaml:"record_kind"`
	Ts         any `json:"ts" yaml:"ts"`
	EventID    any `json:"event_id" yaml:"event_id"`
}

// eventType is the record's event type: artifact commit records carry it
// only as their event_type partition key.
func eventType(e map[string]any) any {
	if t, ok := e["type"]; ok {
		return t
	}
	return e["event_type"]
}

func eventSummaries(events []map[string]any) []eventSummary {
	out := make([]eventSummary, 0, len(events))
	for _, e := range events {
		out = append(out, eventSummary{Seq: e["seq"], Type: eventType(e), RecordKind: e["record_kind"], Ts: e["ts"], EventID: e["event_id"]})
	}
	return out
}
