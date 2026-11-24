package runtime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"syscall"
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

func TestExecutorManager_WaitAfterFailedStart(t *testing.T) {
	t.Parallel()
	m := NewExecutorManager(&ExecutorConfig{
		ExecutorPath: filepath.Join(t.TempDir(), "missing-executor"),
	})
	if err := m.Start(t.Context()); err == nil {
		t.Fatal("Start succeeded with a missing executor")
	}
	if result, err := m.Wait(); result != nil || err == nil {
		t.Fatalf("Wait after a failed Start = (%v, %v), want nil result and an error", result, err)
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
	// Wait for the caller to read the readiness line before flooding stderr.
	body := `read -r input
printf 'ready\n' >&2
read -r ack
head -c 2000000 /dev/zero | tr '\0' e >&2
echo done
`
	if err := os.WriteFile(script, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	m := NewExecutorManager(&ExecutorConfig{
		ExecutorPath: "/bin/sh",
		ScriptPath:   script,
		RunMeta:      &types.RunMeta{RunID: "run-1", Attempt: 1},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ready := make([]byte, len("ready\n"))
	if _, err := io.ReadFull(m.Stderr(), ready); err != nil || string(ready) != "ready\n" {
		t.Fatalf("read live stderr = (%q, %v), want ready line", ready, err)
	}
	if _, err := io.WriteString(m.Stdin(), "continue\n"); err != nil {
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
	case <-ctx.Done():
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
	tail := bytes.Repeat([]byte("e"), maxStderrBytes)
	if want := append([]byte("[stderr truncated: the first 951430 bytes were dropped]\n"), tail...); !bytes.Equal(res.StderrBytes, want) {
		t.Errorf("Wait returned %d stderr bytes starting with %q, want the truncation note and tail", len(res.StderrBytes), res.StderrBytes[:min(80, len(res.StderrBytes))])
	}
	if remaining, err := io.ReadAll(m.Stderr()); err != nil || !bytes.Equal(remaining, tail) {
		t.Errorf("read after falling behind returned %d stderr bytes and error %v, want the tail", len(remaining), err)
	}
}

// A process that the executor starts in its group does not outlive the run,
// whether the run is cancelled or the executor exits first, and one that holds
// the executor's output does not keep the run from ending.
func TestExecutorManager_StopsWhatTheExecutorStarted(t *testing.T) {
	t.Parallel()
	if goruntime.GOOS == "windows" {
		t.Skip("the fake executor is a POSIX shell script")
	}
	quiet := "sleep 300 </dev/null >/dev/null 2>&1 &\n"
	holdsOutput := "(trap '' TERM; exec sleep 300) &\n"
	cases := []struct {
		name, spawn, last string
		cancel            bool
	}{
		{"the run cancelled", quiet, "read -r ack", true},
		{"the executor exiting first", quiet, "exit 0", false},
		{"a child that holds the output, the run cancelled", holdsOutput, "read -r ack", true},
		{"a child that holds the output, the executor exiting first", holdsOutput, "exit 0", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "child.pid")
			script := filepath.Join(dir, "spawn.sh")
			body := "read -r input\n" + tc.spawn + "echo $! > '" + pidFile + "'\n" + tc.last + "\n"
			if err := os.WriteFile(script, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			m := NewExecutorManager(&ExecutorConfig{
				ExecutorPath: "/bin/sh",
				ScriptPath:   script,
				RunMeta:      &types.RunMeta{RunID: "run-1", Attempt: 1},
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := m.Start(ctx); err != nil {
				t.Fatal(err)
			}
			pid := readPID(t, pidFile)
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
			if tc.cancel {
				cancel()
			}
			endsCleanly(t, m)
			deadline := time.Now().Add(5 * time.Second)
			for running(t, pid) && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if running(t, pid) {
				t.Errorf("the executor's child %d is still running after Wait", pid)
			}
			if err := m.Kill(); !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("Kill after the run ended = %v, want os.ErrProcessDone and no signal", err)
			}
			if _, err := m.Wait(); err != nil {
				t.Errorf("a second Wait = %v, want the same result", err)
			}
		})
	}
}

// When the group cannot be stopped, a child can hold both outputs open. What it
// writes during the grace is read, both readers end, and Wait reports the group
// with no cause, although the run ends meanwhile.
func TestExecutorManager_UnstoppableGroup(t *testing.T) {
	t.Parallel()
	if goruntime.GOOS == "windows" {
		t.Skip("the fake executor is a POSIX shell script")
	}
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	script := filepath.Join(dir, "hold.sh")
	body := "read -r input\n(sleep 1; echo late; echo late >&2; exec sleep 300) &\necho $! > '" + pidFile + "'\nexit 0\n"
	if err := os.WriteFile(script, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	m := NewExecutorManager(&ExecutorConfig{
		ExecutorPath: "/bin/sh",
		ScriptPath:   script,
		RunMeta:      &types.RunMeta{RunID: "run-1", Attempt: 1},
	})
	refused := errors.New("signal refused")
	m.signalGroup = func(int) error { return refused }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	pid := readPID(t, pidFile)
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	stdout := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(m.Stdout())
		stdout <- string(data)
	}()
	limit := time.After(outputGrace + 15*time.Second)
	select {
	case <-m.exited:
		cancel() // the run ends while its output is still read
	case <-limit:
		t.Fatal("the executor did not exit")
	}
	// As the runtime does: stdout to its end first, and then Wait.
	select {
	case out := <-stdout:
		if out != "late\n" {
			t.Errorf("stdout %q, want what the child wrote during the grace", out)
		}
	case <-limit:
		t.Fatal("stdout did not end")
	}
	ended := make(chan error, 1)
	go func() {
		_, err := m.Wait()
		ended <- err
	}()
	select {
	case err := <-ended:
		group, ok := errors.AsType[*groupStopError](err)
		if !ok || !errors.Is(err, refused) || group.cause != nil {
			t.Errorf("Wait = %v, want the group's error with no cause", err)
		}
	case <-limit:
		t.Fatal("stderr did not end")
	}
	if text := string(m.stderr.wait()); text != "late\n" {
		t.Errorf("stderr %q, want what the child wrote during the grace", text)
	}

	// A later run on the same manager does not keep the failure.
	m.signalGroup = killGroup
	if err := os.WriteFile(script, []byte("read -r input\nexit 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	endsCleanly(t, m)
}

// endsCleanly reads a started run as the runtime does, stdout to its end first
// and then Wait, and fails the test unless it ends in time without an error.
func endsCleanly(t *testing.T, m *ExecutorManager) {
	t.Helper()
	ended := make(chan error, 1)
	go func() {
		if _, err := io.Copy(io.Discard, m.Stdout()); err != nil {
			ended <- err
			return
		}
		_, err := m.Wait()
		ended <- err
	}()
	select {
	case err := <-ended:
		if err != nil {
			t.Fatalf("the run ended with %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the run did not end")
	}
}

// A manager that starts a second run stops that run too.
func TestExecutorManager_StopsASecondRun(t *testing.T) {
	t.Parallel()
	if goruntime.GOOS == "windows" {
		t.Skip("the fake executor is a POSIX shell script")
	}
	script := filepath.Join(t.TempDir(), "wait.sh")
	if err := os.WriteFile(script, []byte("read -r input\nread -r ack\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	m := NewExecutorManager(&ExecutorConfig{
		ExecutorPath: "/bin/sh",
		ScriptPath:   script,
		RunMeta:      &types.RunMeta{RunID: "run-1", Attempt: 1},
	})
	for range 2 {
		ctx, cancel := context.WithCancel(t.Context())
		if err := m.Start(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		endsCleanly(t, m)
	}
}

// running reports whether pid runs; a zombie that nothing has reaped does not.
func running(t *testing.T, pid int) bool {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "ps", "-o", "stat=", "-p", strconv.Itoa(pid)).Output()
	if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
		return false
	}
	if err != nil {
		t.Logf("ps could not inspect %d: %v", pid, err)
		return true
	}
	return !strings.HasPrefix(strings.TrimSpace(string(out)), "Z")
}

// readPID waits for the fake executor to write its child's PID.
func readPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil && strings.HasSuffix(string(data), "\n") {
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err != nil {
				t.Fatal(err)
			}
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the fake executor never wrote its child's PID")
	return 0
}
