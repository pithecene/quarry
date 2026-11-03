// readtrace counts the store requests behind one artifact read, phase by
// phase: FindRun, OpenArtifact, and one ReadAt of up to 8 MiB, which are
// the phases of read artifact. It also runs ListArtifacts, the phase of
// list artifacts, and reports it apart, so the two commands' costs are
// not confused. It wraps the store that the storage flags name in a shim that
// records every call and its duration, so a slow read can be attributed
// to requests rather than to bytes. It is a measurement harness for the
// maintainers, not a command of the CLI: the release builds only
// cmd/quarry, and nothing here is part of the CLI contract. Its flags are
// the storage and run flags of `quarry read artifact`.
//
// Measured on a run of 72 artifacts over Cloudflare R2, one read made
// 298 requests: 2 to find the run, 294 to read the commit records inside
// OpenArtifact, and 2 for the bytes. A list artifacts made 293 more.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/reader"
)

// readTimeout bounds the whole measurement, as the CLI bounds read
// artifact (cli/cmd.artifactReadTimeout).
const readTimeout = 30 * time.Minute

// counting records every store call with its duration.
type counting struct {
	lode.RangeStore
	mu    sync.Mutex
	calls []call
}

type call struct {
	op   string
	key  string
	took time.Duration
}

func (c *counting) record(op, key string, start time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call{op, key, time.Since(start)})
}

// Get times the whole read, not the headers: an object store returns the
// body as a stream that the reader consumes afterward, so the duration is
// recorded when the body is closed. A Get that fails is recorded at once.
func (c *counting) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := c.RangeStore.Get(ctx, key)
	if err != nil {
		c.record("Get", key, start)
		return nil, err
	}
	return &timedBody{ReadCloser: rc, done: func() { c.record("Get", key, start) }}, nil
}

// timedBody records its read once, when it is closed.
type timedBody struct {
	io.ReadCloser
	done func()
	once sync.Once
}

func (b *timedBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.done)
	return err
}

func (c *counting) Exists(ctx context.Context, key string) (bool, error) {
	defer c.record("Exists", key, time.Now())
	return c.RangeStore.Exists(ctx, key)
}

func (c *counting) List(ctx context.Context, prefix string) ([]string, error) {
	defer c.record("List", prefix, time.Now())
	return c.RangeStore.List(ctx, prefix)
}

func (c *counting) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	defer c.record("ReadRange", key, time.Now())
	return c.RangeStore.ReadRange(ctx, key, offset, length)
}

// phase prints the calls made since the last phase, by operation, and
// returns where the next phase starts. A summary that cannot be written
// is a failed measurement, so the harness exits.
func (c *counting) phase(name string, since int, took time.Duration) int {
	next, err := c.phaseTo(os.Stdout, name, since, took)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return next
}

func (c *counting) phaseTo(w io.Writer, name string, since int, took time.Duration) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	byOp := map[string]int{}
	var sum time.Duration
	for _, k := range c.calls[since:] {
		byOp[k.op]++
		sum += k.took
	}
	if _, err := fmt.Fprintf(w, "%-14s %6.2fs wall, %3d requests (%5.2fs in store) %v\n", name, took.Seconds(), len(c.calls)-since, sum.Seconds(), byOp); err != nil {
		return 0, fmt.Errorf("writing the %s summary: %w", name, err)
	}
	return len(c.calls), nil
}

func main() {
	var backend, path, endpoint, region, dataset, source, category, day, runID, artifactID string
	var pathStyle bool
	flag.StringVar(&backend, "storage-backend", "s3", "")
	flag.StringVar(&path, "storage-path", "", "")
	flag.StringVar(&endpoint, "storage-endpoint", "", "")
	flag.StringVar(&region, "storage-region", "", "")
	flag.BoolVar(&pathStyle, "storage-s3-path-style", false, "")
	flag.StringVar(&dataset, "storage-dataset", qlode.DefaultDataset, "")
	flag.StringVar(&source, "source", "", "")
	flag.StringVar(&category, "category", "", "")
	flag.StringVar(&day, "day", "", "")
	flag.StringVar(&runID, "run-id", "", "")
	flag.StringVar(&artifactID, "artifact-id", "", "")
	flag.Parse()

	factory, err := qlode.NewStoreFactory(backend, path, qlode.S3Config{Region: region, Endpoint: endpoint, UsePathStyle: pathStyle})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var c *counting
	counted := func() (lode.RangeStore, error) {
		s, err := factory()
		if err != nil {
			return nil, err
		}
		c = &counting{RangeStore: s}
		return c, nil
	}
	rr, err := reader.New(dataset, counted)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// The measurement is bounded as read artifact is, and Ctrl-C cancels it,
	// so a stalled backend ends the run rather than hanging it.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	n := 0

	t := time.Now()
	run, err := rr.FindRun(ctx, reader.Selector{Source: source, Category: category, Day: day}, runID)
	if err != nil {
		fmt.Fprintln(os.Stderr, "FindRun:", err)
		os.Exit(1)
	}
	n = c.phase("FindRun", n, time.Since(t))

	// read artifact goes from the run straight to OpenArtifact; the
	// listing below is list artifacts, measured apart.
	t = time.Now()
	a, rd, err := rr.OpenArtifact(ctx, run, artifactID)
	if err != nil {
		fmt.Fprintln(os.Stderr, "OpenArtifact:", err)
		os.Exit(1)
	}
	n = c.phase("OpenArtifact", n, time.Since(t))

	t = time.Now()
	buf := make([]byte, 8<<20)
	got, err := rd.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		fmt.Fprintln(os.Stderr, "ReadAt:", err)
		os.Exit(1)
	}
	n = c.phase(fmt.Sprintf("ReadAt(%d/%d)", got, a.SizeBytes), n, time.Since(t))
	readRequests := n

	t = time.Now()
	arts, err := rr.ListArtifacts(ctx, run)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ListArtifacts:", err)
		os.Exit(1)
	}
	c.phase(fmt.Sprintf("ListArtifacts(%d)", len(arts)), n, time.Since(t))

	// The slowest single requests, to see whether one call dominates.
	c.mu.Lock()
	calls := append([]call(nil), c.calls...)
	c.mu.Unlock()
	fmt.Printf("read artifact: %d requests; with list artifacts: %d\n", readRequests, len(calls))
	var slow []call
	for _, k := range calls {
		if k.took > 500*time.Millisecond {
			slow = append(slow, k)
		}
	}
	for _, k := range slow {
		fmt.Printf("  slow %-9s %6.2fs %s\n", k.op, k.took.Seconds(), k.key)
	}
}
