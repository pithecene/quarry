package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/pithecene/quarry/executor"
	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

// ExecutorConfig configures executor execution.
type ExecutorConfig struct {
	// ExecutorPath is the path to the executor binary.
	ExecutorPath string
	// ScriptPath is the path to the script file.
	ScriptPath string
	// Job is the job payload.
	Job any
	// RunMeta is the run metadata.
	RunMeta *types.RunMeta
	// Proxy is the optional resolved proxy endpoint per CONTRACT_PROXY.md.
	// If nil, executor launches without a proxy.
	Proxy *types.ProxyEndpoint
	// BrowserWSEndpoint is the optional WebSocket URL of an externally managed browser.
	// When set, the executor connects instead of launching a new Chromium instance.
	BrowserWSEndpoint string
	// NoBrowser declares a run without a browser: the executor neither
	// launches nor connects to one (CONTRACT_RUN.md §Runs Without a Browser).
	NoBrowser bool
	// ResolveFrom is the optional path to a node_modules directory used for
	// bare-specifier ESM resolution fallback. When set, the executor registers
	// a custom resolve hook via module.register().
	ResolveFrom string
	// Storage is the optional Hive partition metadata for SDK-side key computation.
	// When set, the executor passes this to the SDK so storage.put() can return
	// the resolved storage key without a bidirectional IPC round-trip.
	Storage *StoragePartition
}

// ExecutorResult represents the result of executor execution.
type ExecutorResult struct {
	// ExitCode is the process exit code.
	ExitCode int
	// StderrBytes is the captured stderr output.
	StderrBytes []byte
}

// ExecutorManager manages executor process lifecycle.
type ExecutorManager struct {
	config *ExecutorConfig
	cmd    *executor.Process
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr *stderrCapture
	// browsers reports the per-run browsers that the executor launched.
	browsers *browserReports
	// escalation kills the executor's group when a cancelled run outlasts
	// its grace period. Only Cancel arms it.
	escalation *time.Timer
}

// NewExecutorManager creates a new executor manager.
func NewExecutorManager(config *ExecutorConfig) *ExecutorManager {
	return &ExecutorManager{
		config: config,
	}
}

// StoragePartition describes the Hive partition metadata passed to the executor.
// The executor uses this to compute deterministic storage keys client-side,
// avoiding a bidirectional IPC round-trip.
type StoragePartition struct {
	Dataset  string `json:"dataset"`
	Source   string `json:"source"`
	Category string `json:"category"`
	Day      string `json:"day"`
	RunID    string `json:"run_id"`
}

// executorInput is the JSON structure written to executor stdin.
type executorInput struct {
	RunID             string               `json:"run_id"`
	Attempt           int                  `json:"attempt"`
	JobID             *string              `json:"job_id,omitempty"`
	ParentRunID       *string              `json:"parent_run_id,omitempty"`
	Job               any                  `json:"job"`
	Proxy             *types.ProxyEndpoint `json:"proxy,omitempty"`
	BrowserWSEndpoint string               `json:"browser_ws_endpoint,omitempty"`
	NoBrowser         bool                 `json:"no_browser,omitempty"`
	Storage           *StoragePartition    `json:"storage,omitempty"`
}

// Start starts the executor process.
// The process reads run metadata and job from stdin (JSON).
// Stdout is used for IPC frames.
// Stderr is captured for diagnostics.
func (m *ExecutorManager) Start(ctx context.Context) error {
	// Build command: quarry-executor <script-path>
	m.cmd = executor.Command(ctx, m.config.ExecutorPath, m.config.ScriptPath)
	m.stderr = nil
	// The executor leads its own process group, so Quarry can stop the group.
	// A child can hold a pipe that ingestion reads to its end, so the whole
	// group is killed when the grace period ends, before Wait.
	m.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	m.escalation = time.AfterFunc(time.Hour, func() { _ = m.Kill() })
	m.escalation.Stop()
	m.cmd.Cancel = func() error {
		m.escalation.Reset(executorStopGrace)
		return signalGroup(m.cmd.Process, syscall.SIGTERM)
	}

	m.cmd.Env = withResolveFrom(os.Environ(), m.config.ResolveFrom)

	// Set up pipes
	stdin, err := m.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}
	m.stdin = stdin

	stdout, err := m.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	m.stdout = stdout

	stderr, err := m.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start process
	reports, reportPipe, err := newBrowserReports()
	if err != nil {
		return err
	}
	// Descriptor 3 belongs to the embedded bundle, so reports use 4.
	m.cmd.ExtraFiles = []*os.File{nil, reportPipe}
	m.cmd.Env = append(m.cmd.Env, browserReportEnv)
	err = m.cmd.Start()
	iox.DiscardClose(reportPipe) // the child holds its own descriptor
	if err != nil {
		reports.close()
		return fmt.Errorf("failed to start executor: %w", err)
	}
	m.browsers = reports
	// Drain stderr now, not at Wait: Wait runs only after ingestion ends, and
	// an executor blocked on a full stderr pipe would never end it.
	m.stderr = startStderrCapture(stderr)

	// Write run metadata and job to stdin
	input := executorInput{
		RunID:             m.config.RunMeta.RunID,
		Attempt:           m.config.RunMeta.Attempt,
		JobID:             m.config.RunMeta.JobID,
		ParentRunID:       m.config.RunMeta.ParentRunID,
		Job:               m.config.Job,
		Proxy:             m.config.Proxy,
		BrowserWSEndpoint: m.config.BrowserWSEndpoint,
		NoBrowser:         m.config.NoBrowser,
		Storage:           m.config.Storage,
	}

	if err := json.NewEncoder(stdin).Encode(input); err != nil {
		_ = m.Kill()
		return fmt.Errorf("failed to write input: %w", err)
	}

	// Stdin remains open for ack frames (file_write_ack).
	// Caller closes via Stdin().Close() after ingestion completes.

	return nil
}

// Stdout returns the stdout reader for IPC frame reading.
func (m *ExecutorManager) Stdout() io.Reader {
	return m.stdout
}

// Stderr returns the live stderr reader for diagnostic capture, or nil if the
// process did not start. Reads advance a shared position. If that position
// falls behind the retained tail, reading resumes at the oldest retained byte.
// Wait reports any truncation and includes the tail even if it was read here.
func (m *ExecutorManager) Stderr() io.Reader {
	if m.stderr == nil {
		return nil
	}
	return stderrReader{capture: m.stderr}
}

// Stdin returns the stdin writer for sending ack frames to the executor.
// The caller must close it after ingestion completes to signal EOF.
func (m *ExecutorManager) Stdin() io.WriteCloser {
	return m.stdin
}

// Wait waits for the executor to exit and returns the result.
// Must be called after Start.
func (m *ExecutorManager) Wait() (*ExecutorResult, error) {
	if m.stderr == nil {
		return nil, errors.New("executor not started")
	}

	// The capture ends when the executor closes stderr, and the pipe must be
	// drained before cmd.Wait closes it.
	stderrBytes := m.stderr.wait()

	// Wait for exit
	err := m.cmd.Wait()
	m.escalation.Stop()
	// The executor has ended: what remains of its group, and every browser it
	// reported launched and not closed, belongs to this run alone.
	_ = m.Kill()
	m.browsers.killOpen()
	m.browsers.close()

	result := &ExecutorResult{
		StderrBytes: stderrBytes,
	}

	// Determine exit code
	if err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				result.ExitCode = status.ExitStatus()
			} else {
				result.ExitCode = -1
			}
		} else {
			return nil, fmt.Errorf("executor wait failed: %w", err)
		}
	} else {
		result.ExitCode = 0
	}

	return result, nil
}

// Kill terminates the executor and every process in its group. Wait then
// terminates the browsers that the executor launched.
func (m *ExecutorManager) Kill() error {
	if m.cmd != nil && m.cmd.Process != nil {
		return signalGroup(m.cmd.Process, syscall.SIGKILL)
	}
	return nil
}

// executorStopGrace is how long a cancelled executor has to exit after
// SIGTERM before its group is killed.
const executorStopGrace = 5 * time.Second

// signalGroup sends sig to the process group that p leads. A group with no
// process left is os.ErrProcessDone, as exec.Cmd.Cancel expects.
func signalGroup(p *os.Process, sig syscall.Signal) error {
	err := syscall.Kill(-p.Pid, sig)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	if err != nil {
		return fmt.Errorf("signal the executor's process group: %w", err)
	}
	return nil
}

// ScriptExports describes the exports found in a validated script module.
type ScriptExports struct {
	Default bool     `json:"default"`
	Hooks   []string `json:"hooks"`
}

// ScriptValidation is the result of executor --validate.
type ScriptValidation struct {
	Valid   bool           `json:"valid"`
	Exports *ScriptExports `json:"exports,omitempty"`
	Error   string         `json:"error,omitempty"`
}

// ValidateScript spawns the executor in --validate mode and returns the
// script validation result. This loads the script module and checks its
// shape without launching a browser or setting up IPC.
func ValidateScript(ctx context.Context, executorPath, scriptPath, resolveFrom string) (*ScriptValidation, error) {
	cmd := executor.Command(ctx, executorPath, "--validate", scriptPath)

	cmd.Env = withResolveFrom(os.Environ(), resolveFrom)

	stdout, err := cmd.Output()
	if err != nil {
		// If the process exited with code 1 and produced stdout, parse it
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && len(stdout) > 0 {
			var result ScriptValidation
			if jsonErr := json.Unmarshal(stdout, &result); jsonErr == nil {
				return &result, nil
			}
		}
		// If process produced stderr, include it
		if errors.As(err, &exitErr) && len(exitErr.Stderr) > 0 {
			return nil, fmt.Errorf("executor validate failed: %w\nstderr: %s", err, string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("executor validate failed: %w", err)
	}

	var result ScriptValidation
	if err := json.Unmarshal(stdout, &result); err != nil {
		return nil, fmt.Errorf("failed to parse validation result: %w", err)
	}

	return &result, nil
}

// withResolveFrom returns env with the settings that make the executor load
// modules from resolveFrom: QUARRY_RESOLVE_FROM for its ESM hook, and the same
// directory ahead of any inherited NODE_PATH for require. Every executor launch
// uses it, so a shared browser loads Puppeteer as its runs do. An empty
// resolveFrom returns env unchanged.
func withResolveFrom(env []string, resolveFrom string) []string {
	if resolveFrom == "" {
		return env
	}
	nodePath := resolveFrom
	if existing := os.Getenv("NODE_PATH"); existing != "" {
		nodePath += string(os.PathListSeparator) + existing
	}
	return deduplicateEnv(append(slices.Clone(env), "QUARRY_RESOLVE_FROM="+resolveFrom, "NODE_PATH="+nodePath))
}

// deduplicateEnv keeps the last occurrence of each env var key.
// This ensures our appended values (NODE_PATH, QUARRY_RESOLVE_FROM) win
// over inherited duplicates from os.Environ().
func deduplicateEnv(env []string) []string {
	seen := make(map[string]int, len(env))
	for i, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		seen[key] = i
	}
	result := make([]string, 0, len(seen))
	for i, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if seen[key] == i {
			result = append(result, entry)
		}
	}
	return result
}
