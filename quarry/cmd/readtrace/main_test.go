package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"
)

// The shim counts every call it forwards, by operation, and keeps the
// duration of each, so a phase summary names the requests a read made.
func TestCounting_RecordsEveryCall(t *testing.T) {
	t.Parallel()
	mem := lode.NewMemory()
	ctx := t.Context()
	if err := mem.Put(ctx, "k", strings.NewReader("twelve bytes")); err != nil {
		t.Fatal(err)
	}
	c := &counting{RangeStore: mem}

	if ok, err := c.Exists(ctx, "k"); err != nil || !ok {
		t.Fatalf("Exists = %v, %v", ok, err)
	}
	rc, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(rc)
	_ = rc.Close()
	if _, err := c.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if b, err := c.ReadRange(ctx, "k", 7, 5); err != nil || string(b) != "bytes" {
		t.Fatalf("ReadRange = %q, %v", b, err)
	}

	want := map[string]int{"Exists": 1, "Get": 1, "List": 1, "ReadRange": 1}
	got := map[string]int{}
	for _, k := range c.calls {
		got[k.op]++
		if k.took < 0 {
			t.Fatalf("call %s took %v", k.op, k.took)
		}
	}
	for op, n := range want {
		if got[op] != n {
			t.Fatalf("%s counted %d times, want %d (%v)", op, got[op], n, got)
		}
	}
}

// A phase summary covers only the calls since the previous phase and
// returns the position the next phase starts from.
func TestCounting_PhaseWindows(t *testing.T) {
	t.Parallel()
	c := &counting{}
	c.calls = []call{{op: "Get"}, {op: "Get"}, {op: "List"}}
	var out bytes.Buffer
	n, err := c.phaseTo(&out, "first", 0, time.Second)
	if err != nil || n != 3 || !strings.Contains(out.String(), "3 requests") || !strings.Contains(out.String(), "map[Get:2 List:1]") {
		t.Fatalf("phase = %d, %q", n, out.String())
	}
	c.calls = append(c.calls, call{op: "ReadRange"})
	out.Reset()
	if n, err := c.phaseTo(&out, "second", n, time.Second); err != nil || n != 4 || !strings.Contains(out.String(), "1 requests") {
		t.Fatalf("phase = %d, %q", n, out.String())
	}
}

var _ context.Context = context.Background()

// slowBody is a body whose bytes arrive after Get returns, as an object
// store's do: the read, not the Get, is where the time goes.
type slowBody struct {
	io.Reader
	delay time.Duration
	read  bool
}

func (b *slowBody) Read(p []byte) (int, error) {
	if !b.read {
		b.read = true
		time.Sleep(b.delay)
	}
	return b.Reader.Read(p)
}

func (b *slowBody) Close() error { return nil }

type slowStore struct {
	lode.RangeStore
	delay time.Duration
}

func (s *slowStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := s.RangeStore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return &slowBody{Reader: rc, delay: s.delay}, nil
}

// A Get's duration covers the body, which the caller reads after Get
// returns, and is recorded when the body is closed.
func TestCounting_GetTimesTheBody(t *testing.T) {
	t.Parallel()
	mem := lode.NewMemory()
	ctx := t.Context()
	if err := mem.Put(ctx, "k", strings.NewReader("body")); err != nil {
		t.Fatal(err)
	}
	c := &counting{RangeStore: &slowStore{RangeStore: mem, delay: 50 * time.Millisecond}}
	rc, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(c.calls); n != 0 {
		t.Fatalf("recorded %d calls before the body was read", n)
	}
	if b, _ := io.ReadAll(rc); string(b) != "body" {
		t.Fatalf("body = %q", b)
	}
	_ = rc.Close()
	_ = rc.Close() // a second close records nothing more
	if len(c.calls) != 1 || c.calls[0].took < 50*time.Millisecond {
		t.Fatalf("calls = %+v, want one Get of at least 50ms", c.calls)
	}
}
