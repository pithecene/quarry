package runtime

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/quarry/types"
)

// The capture keeps every byte up to its bound, and past it the last
// maxStderrBytes bytes, after a note of the bytes it dropped.
func TestStderrCapture_KeepsTheTail(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		written int
		note    string
	}{
		{"under the bound", 1000, ""},
		{"at the bound", maxStderrBytes, ""},
		{"past the bound", maxStderrBytes + 5000, "[stderr truncated: the first 5000 bytes were dropped]\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := bytes.Repeat([]byte("a"), tc.written-1)
			stream = append(stream, 'z')
			got := startStderrCapture(bytes.NewReader(stream)).wait()
			tail := stream[max(0, len(stream)-maxStderrBytes):]
			if want := append([]byte(tc.note), tail...); !bytes.Equal(got, want) {
				t.Errorf("kept %d bytes ending %q, want %d bytes with note %q", len(got), got[max(0, len(got)-1):], len(want), tc.note)
			}
		})
	}
}

// An executor that writes more stderr than its pipe holds before it writes
// its stdout must not stall the run: the runtime reads stdout to its end
// before it waits for the executor, so stderr is drained from the start.
func TestExecutorManager_StderrFloodDoesNotStallStdout(t *testing.T) {
	t.Parallel()
	if goruntime.GOOS == "windows" {
		t.Skip("the fake executor is a POSIX shell script")
	}
	script := filepath.Join(t.TempDir(), "flood.sh")
	body := "head -c 2000000 /dev/zero | tr '\\0' e >&2\necho done\n"
	if err := os.WriteFile(script, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	m := NewExecutorManager(&ExecutorConfig{
		ExecutorPath: "/bin/sh",
		ScriptPath:   script,
		RunMeta:      &types.RunMeta{RunID: "run-1", Attempt: 1},
	})
	if err := m.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	stdout := make(chan []byte, 1)
	go func() {
		out, _ := io.ReadAll(m.Stdout())
		stdout <- out
	}()
	select {
	case out := <-stdout:
		if strings.TrimSpace(string(out)) != "done" {
			t.Fatalf("stdout = %q, want done", out)
		}
	case <-time.After(10 * time.Second):
		if err := m.Kill(); err != nil {
			t.Logf("kill: %v", err)
		}
		t.Fatal("stdout did not end: the executor is blocked writing stderr")
	}
	if err := m.Stdin().Close(); err != nil {
		t.Logf("close stdin: %v", err)
	}
	res, err := m.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.ExitCode != 0 {
		t.Errorf("exit code %d, want 0", res.ExitCode)
	}
	if !strings.HasPrefix(string(res.StderrBytes), "[stderr truncated: the first 951424 bytes were dropped]\n") {
		t.Errorf("stderr begins %q, want the truncation note", res.StderrBytes[:min(80, len(res.StderrBytes))])
	}
}
