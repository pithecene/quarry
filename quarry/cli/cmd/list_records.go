package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	lodelib "github.com/pithecene-io/lode/lode"
	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/render"
	"github.com/pithecene/quarry/lode"
	runreader "github.com/pithecene/quarry/reader"
)

// readTimeout bounds a storage-backed read command.
const readTimeout = 5 * time.Minute

// storageReadFlags are the storage flags of read commands that read Lode
// storage. They mean the same as on `run`.
func storageReadFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "storage-dataset", Usage: "Lode dataset ID (default: \"quarry\")", Value: lode.DefaultDataset},
		&cli.StringFlag{Name: "storage-backend", Usage: "Storage backend: fs or s3"},
		&cli.StringFlag{Name: "storage-path", Usage: "Storage path (fs: directory, s3: bucket/prefix)"},
		&cli.StringFlag{Name: "storage-region", Usage: "AWS region for S3 backend"},
		&cli.StringFlag{Name: "storage-endpoint", Usage: "Custom S3 endpoint URL for S3-compatible providers (e.g. Cloudflare R2, MinIO)"},
		&cli.BoolFlag{Name: "storage-s3-path-style", Usage: "Force path-style addressing for S3 (required by R2, MinIO)"},
	}
}

// runSelectorFlags select runs by key prefix, in order.
func runSelectorFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "source", Usage: "Source to list (required with storage; cross-source listing omits it)"},
		&cli.StringFlag{Name: "category", Usage: "Category (requires --source)"},
		&cli.StringFlag{Name: "day", Usage: "Partition day YYYY-MM-DD (requires --category)"},
	}
}

// storageRequested reports whether storage flags were given, and errors if
// any was given without the required pair: a storage flag never silently
// falls back to stub data.
func storageRequested(c *cli.Context) (bool, error) {
	backend, path := c.String("storage-backend"), c.String("storage-path")
	if !anyStorageFlagSet(c) {
		return false, nil
	}
	if backend == "" || path == "" {
		return false, errors.New("both --storage-backend and --storage-path are required for Lode reads")
	}
	return true, nil
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
func newRunReader(c *cli.Context) (*runreader.Reader, error) {
	factory, err := storeFactoryFrom(c)
	if err != nil {
		return nil, err
	}
	return runreader.New(c.String("storage-dataset"), factory)
}

// storeFactoryFrom builds the store factory the storage flags describe.
func storeFactoryFrom(c *cli.Context) (lodelib.StoreFactory, error) {
	backend := c.String("storage-backend")
	endpoint, pathStyle := c.String("storage-endpoint"), c.Bool("storage-s3-path-style")
	if backend == "fs" && (endpoint != "" || pathStyle) {
		fmt.Fprintf(os.Stderr, "Warning: --storage-endpoint and --storage-s3-path-style are ignored for fs backend\n")
	}
	return lode.NewStoreFactory(backend, c.String("storage-path"), lode.S3Config{
		Region:       c.String("storage-region"),
		Endpoint:     endpoint,
		UsePathStyle: pathStyle,
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
	ctx, cancel := context.WithTimeout(c.Context, readTimeout)
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
	flags := append(append(ReadOnlyFlags(), storageReadFlags()...), runSelectorFlags()...)
	flags = append(flags, &cli.StringFlag{Name: "run-id", Usage: "Run whose events to list (required)", Required: true})
	return &cli.Command{
		Name:   "events",
		Usage:  "List a run's events in sequence order (reads storage)",
		Flags:  flags,
		Action: listEventsAction,
	}
}

// listEventsResponse is the JSON shape of `list events`.
type listEventsResponse struct {
	Run    runreader.Run    `json:"run"`
	Events []map[string]any `json:"events"`
}

func listEventsAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}
	if c.Bool("tui") {
		return cli.Exit("--tui is not supported for list commands", 1)
	}
	ok, err := storageRequested(c)
	if err != nil {
		return cli.Exit(err.Error(), 1)
	}
	if !ok {
		return cli.Exit("list events reads storage: --storage-backend and --storage-path are required", 1)
	}
	sel := selectorFrom(c)
	if sel.Source == "" || sel.Category == "" || sel.Day == "" {
		return cli.Exit("list events requires --source, --category, and --day to locate the run", 1)
	}
	rr, err := newRunReader(c)
	if err != nil {
		return cli.Exit(fmt.Sprintf("failed to initialize storage reader: %v", err), 1)
	}
	ctx, cancel := context.WithTimeout(c.Context, readTimeout)
	defer cancel()

	run, err := rr.FindRun(ctx, sel, c.String("run-id"))
	if err != nil {
		return cli.Exit(fmt.Sprintf("list events: %v", err), 1)
	}
	events, err := rr.ReadEvents(ctx, run)
	if err != nil {
		return cli.Exit(fmt.Sprintf("list events: %v", err), 1)
	}
	if r.Format() == render.FormatTable {
		return r.Render(eventSummaries(events))
	}
	return r.Render(listEventsResponse{Run: run, Events: events})
}

// eventSummary is one row of `list events --format table`. Records differ in
// shape (events, artifact commits), so the table shows the fields every
// record has; json and yaml carry the full records.
type eventSummary struct {
	Seq        any `json:"seq"`
	Type       any `json:"type"`
	RecordKind any `json:"record_kind"`
	Ts         any `json:"ts"`
	EventID    any `json:"event_id"`
}

func eventSummaries(events []map[string]any) []eventSummary {
	out := make([]eventSummary, 0, len(events))
	for _, e := range events {
		out = append(out, eventSummary{Seq: e["seq"], Type: e["type"], RecordKind: e["record_kind"], Ts: e["ts"], EventID: e["event_id"]})
	}
	return out
}
