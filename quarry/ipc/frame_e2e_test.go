// Package ipc provides E2E tests for IPC framing per CONTRACT_IPC.md.
//
// These tests spawn the actual Node executor and validate that the Go
// FrameDecoder correctly handles real executor output.
//
// Test gating:
//   - Live E2E tests require QUARRY_E2E=1 (slow, requires Node + Puppeteer)
//   - Fixture regeneration requires QUARRY_REGEN_FIXTURES=1
//
// Fixture tests always run using pre-generated fixtures.
package ipc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/pithecene/quarry/types"
)

// testPaths holds resolved paths for E2E tests.
type testPaths struct {
	repoRoot    string
	executorBin string
	fixtureDir  string
	testdataDir string
}

// resolveTestPaths finds the repository root and executor binary.
func resolveTestPaths(t *testing.T) testPaths {
	t.Helper()

	// Find repo root by walking up from this file
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get caller info")
	}

	// This file is at quarry/ipc/frame_e2e_test.go
	// Repo root is two levels up
	ipcDir := filepath.Dir(thisFile)
	quarryDir := filepath.Dir(ipcDir)
	repoRoot := filepath.Dir(quarryDir)

	executorBin := filepath.Join(repoRoot, "executor-node", "dist", "bin", "executor.js")
	fixtureDir := filepath.Join(repoRoot, "executor-node", "testdata")
	testdataDir := filepath.Join(ipcDir, "testdata")

	return testPaths{
		repoRoot:    repoRoot,
		executorBin: executorBin,
		fixtureDir:  fixtureDir,
		testdataDir: testdataDir,
	}
}

// checkNodeAvailable verifies Node.js is available.
func checkNodeAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not available, skipping E2E test")
	}
}

// checkExecutorBuilt verifies the executor is built.
func checkExecutorBuilt(t *testing.T, paths testPaths) {
	t.Helper()
	if _, err := os.Stat(paths.executorBin); os.IsNotExist(err) {
		t.Skipf("executor not built at %s, run 'pnpm build' in executor-node/", paths.executorBin)
	}
}

// runInput is the JSON input structure for the executor.
type runInput struct {
	RunID       string `json:"run_id"`
	Attempt     int    `json:"attempt"`
	JobID       string `json:"job_id,omitempty"`
	ParentRunID string `json:"parent_run_id,omitempty"`
	Job         any    `json:"job"`
}

// spawnExecutor spawns the Node executor with the given script and input.
// The context controls the lifetime of the spawned process.
// Returns stdout bytes and any error.
func spawnExecutor(t *testing.T, ctx context.Context, paths testPaths, scriptPath string, input runInput) ([]byte, error) {
	t.Helper()

	inputJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("failed to marshal input: %v", err)
	}

	cmd := exec.CommandContext(ctx, "node", paths.executorBin, scriptPath)
	cmd.Stdin = bytes.NewReader(inputJSON)
	cmd.Env = append(os.Environ(), "QUARRY_NO_SANDBOX=1")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if stderr.Len() > 0 {
		t.Logf("executor stderr: %s", stderr.String())
	}

	return stdout.Bytes(), err
}

// TestE2E_WireHarness is Milestone 1: spawn executor and capture raw stdout.
// This test validates the wire harness works without asserting decode correctness.
func TestE2E_WireHarness(t *testing.T) {
	if os.Getenv("QUARRY_E2E") != "1" {
		t.Skip("QUARRY_E2E=1 not set, skipping live E2E test")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	paths := resolveTestPaths(t)
	checkNodeAvailable(t)
	checkExecutorBuilt(t, paths)

	scriptPath := filepath.Join(paths.fixtureDir, "e2e-fixture-script.js")
	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		t.Skipf("fixture script not found at %s", scriptPath)
	}

	input := runInput{
		RunID:   "run-e2e-test-001",
		Attempt: 1,
		Job:     map[string]string{"test": "wire_harness"},
	}

	stdout, err := spawnExecutor(t, ctx, paths, scriptPath, input)
	if err != nil {
		// Exit code 1 is ok (run_error), 2+ is crash
		exitErr := &exec.ExitError{}
		if errors.As(err, &exitErr) {
			t.Fatalf("executor crashed (exit %d): %v", exitErr.ExitCode(), err)
		}
	}

	if len(stdout) == 0 {
		t.Fatal("executor produced no output")
	}

	// Pipe through decoder to validate framing
	decoder := NewFrameDecoder(bytes.NewReader(stdout))
	frameCount := 0

	for {
		payload, err := decoder.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame failed at frame %d: %v", frameCount, err)
		}
		if len(payload) == 0 {
			t.Errorf("frame %d has empty payload", frameCount)
		}
		frameCount++
	}

	if frameCount == 0 {
		t.Error("no frames decoded from executor output")
	}

	t.Logf("wire harness: captured %d bytes, decoded %d frames", len(stdout), frameCount)
}

// TestE2E_LiveDecode is Milestone 5: decode live stdout stream with assertions.
// This test validates the full decode path with ordering and terminal checks.
func TestE2E_LiveDecode(t *testing.T) {
	if os.Getenv("QUARRY_E2E") != "1" {
		t.Skip("QUARRY_E2E=1 not set, skipping live E2E test")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	paths := resolveTestPaths(t)
	checkNodeAvailable(t)
	checkExecutorBuilt(t, paths)

	scriptPath := filepath.Join(paths.fixtureDir, "e2e-fixture-script.js")
	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		t.Skipf("fixture script not found at %s", scriptPath)
	}

	input := runInput{
		RunID:   "run-e2e-live-001",
		Attempt: 1,
		Job:     map[string]string{"test": "live_decode"},
	}

	stdout, err := spawnExecutor(t, ctx, paths, scriptPath, input)
	if err != nil {
		exitErr := &exec.ExitError{}
		if errors.As(err, &exitErr) {
			t.Fatalf("executor crashed (exit %d): %v", exitErr.ExitCode(), err)
		}
	}

	// Decode all frames
	decoder := NewFrameDecoder(bytes.NewReader(stdout))
	var frames []any
	terminalSeenAt := -1 // Index where terminal event was seen
	var terminalType string

	for {
		payload, err := decoder.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}

		frame, err := DecodeFrame(payload)
		if err != nil {
			t.Fatalf("DecodeFrame failed: %v", err)
		}

		// Per CONTRACT_IPC.md: no frames should occur after terminal event
		if terminalSeenAt >= 0 {
			t.Errorf("frame received after terminal event at index %d: %T", terminalSeenAt, frame)
		}

		frames = append(frames, frame)

		// Track terminal event
		if env, ok := frame.(*types.EventEnvelope); ok {
			if env.Type.IsTerminal() {
				terminalSeenAt = len(frames) - 1
				terminalType = string(env.Type)
			}
		}
	}

	// Assertions
	if len(frames) == 0 {
		t.Fatal("no frames decoded")
	}

	// Terminal event must exist
	if terminalSeenAt < 0 {
		t.Fatal("no terminal event found in stream")
	}

	// Last frame must be the terminal event (no chunks after terminal)
	if terminalSeenAt != len(frames)-1 {
		t.Errorf("terminal event at index %d, but %d frames total (frames after terminal)", terminalSeenAt, len(frames))
	}

	// Last frame must be an event envelope (not a chunk)
	lastFrame := frames[len(frames)-1]
	if _, ok := lastFrame.(*types.EventEnvelope); !ok {
		t.Errorf("last frame is %T, want *types.EventEnvelope", lastFrame)
	}

	t.Logf("live decode: %d frames, terminal = %s at index %d", len(frames), terminalType, terminalSeenAt)
}

// TestE2E_FixtureDrift is Milestone 4: detect fixture drift.
// When QUARRY_REGEN_FIXTURES=1, regenerates the fixture and compares structure.
// Fails if the new fixture differs structurally from the existing one.
//
// Note: We compare semantic structure (event types, sequences, payload keys)
// rather than raw bytes because some fields are non-deterministic:
//   - event_id (UUIDs generated at runtime)
//   - ts (timestamps)
//
// This test is slow (requires spawning executor) and only runs when
// QUARRY_REGEN_FIXTURES=1 is set.
func TestE2E_FixtureDrift(t *testing.T) {
	if os.Getenv("QUARRY_REGEN_FIXTURES") != "1" {
		t.Skip("QUARRY_REGEN_FIXTURES=1 not set, skipping drift check")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	paths := resolveTestPaths(t)
	checkNodeAvailable(t)
	checkExecutorBuilt(t, paths)

	scriptPath := filepath.Join(paths.fixtureDir, "e2e-fixture-script.js")
	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		t.Skipf("fixture script not found at %s", scriptPath)
	}

	// Load existing fixture
	existingFixturePath := filepath.Join(paths.testdataDir, "e2e_fixture.bin")
	existingFixture, err := os.ReadFile(existingFixturePath)
	if err != nil {
		t.Skipf("existing fixture not found at %s", existingFixturePath)
	}

	// Regenerate fixture with same input as generator script
	input := runInput{
		RunID:   "run-e2e-fixture-001",
		Attempt: 1,
		Job: map[string]any{
			"fixture": true,
			"version": "1.0.0",
		},
	}

	newFixture, err := spawnExecutor(t, ctx, paths, scriptPath, input)
	if err != nil {
		exitErr := &exec.ExitError{}
		if errors.As(err, &exitErr) {
			t.Fatalf("executor crashed (exit %d): %v", exitErr.ExitCode(), err)
		}
	}

	// Compare frame counts
	existingFrameCount := countFrames(t, existingFixture)
	newFrameCount := countFrames(t, newFixture)
	if existingFrameCount != newFrameCount {
		t.Errorf("frame count drift: existing=%d, new=%d", existingFrameCount, newFrameCount)
	}

	// Parse both fixtures into semantic structures
	existingEvents, existingChunks := parseFixture(t, existingFixture)
	newEvents, newChunks := parseFixture(t, newFixture)

	// Compare event count
	if len(existingEvents) != len(newEvents) {
		t.Errorf("event count drift: existing=%d, new=%d", len(existingEvents), len(newEvents))
	}

	// Compare chunk count
	if len(existingChunks) != len(newChunks) {
		t.Errorf("chunk count drift: existing=%d, new=%d", len(existingChunks), len(newChunks))
	}

	// Compare each event's semantic structure
	minEvents := len(existingEvents)
	if len(newEvents) < minEvents {
		minEvents = len(newEvents)
	}
	for i := 0; i < minEvents; i++ {
		existing := existingEvents[i]
		current := newEvents[i]

		// Compare type
		if existing.Type != current.Type {
			t.Errorf("event[%d] type drift: existing=%q, new=%q", i, existing.Type, current.Type)
		}

		// Compare seq
		if existing.Seq != current.Seq {
			t.Errorf("event[%d] seq drift: existing=%d, new=%d", i, existing.Seq, current.Seq)
		}

		// Compare run_id
		if existing.RunID != current.RunID {
			t.Errorf("event[%d] run_id drift: existing=%q, new=%q", i, existing.RunID, current.RunID)
		}

		// Compare attempt
		if existing.Attempt != current.Attempt {
			t.Errorf("event[%d] attempt drift: existing=%d, new=%d", i, existing.Attempt, current.Attempt)
		}

		// Compare payload keys (not values, as some may be generated)
		existingKeys := payloadKeys(existing.Payload)
		currentKeys := payloadKeys(current.Payload)
		if !equalStringSlices(existingKeys, currentKeys) {
			t.Errorf("event[%d] payload keys drift: existing=%v, new=%v", i, existingKeys, currentKeys)
		}
	}

	// Compare each chunk's structure
	minChunks := len(existingChunks)
	if len(newChunks) < minChunks {
		minChunks = len(newChunks)
	}
	for i := 0; i < minChunks; i++ {
		existing := existingChunks[i]
		current := newChunks[i]

		// Compare seq
		if existing.Seq != current.Seq {
			t.Errorf("chunk[%d] seq drift: existing=%d, new=%d", i, existing.Seq, current.Seq)
		}

		// Compare is_last
		if existing.IsLast != current.IsLast {
			t.Errorf("chunk[%d] is_last drift: existing=%v, new=%v", i, existing.IsLast, current.IsLast)
		}

		// Compare data size
		if len(existing.Data) != len(current.Data) {
			t.Errorf("chunk[%d] data size drift: existing=%d, new=%d", i, len(existing.Data), len(current.Data))
		}
	}

	t.Logf("fixture drift check passed: %d events, %d chunks", len(existingEvents), len(existingChunks))
}

// parseFixture parses raw fixture bytes into events and chunks.
func parseFixture(t *testing.T, data []byte) ([]*types.EventEnvelope, []*types.ArtifactChunkFrame) {
	t.Helper()
	decoder := NewFrameDecoder(bytes.NewReader(data))
	var events []*types.EventEnvelope
	var chunks []*types.ArtifactChunkFrame

	for {
		payload, err := decoder.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}

		frame, err := DecodeFrame(payload)
		if err != nil {
			t.Fatalf("DecodeFrame failed: %v", err)
		}

		switch f := frame.(type) {
		case *types.EventEnvelope:
			events = append(events, f)
		case *types.ArtifactChunkFrame:
			chunks = append(chunks, f)
		}
	}
	return events, chunks
}

// payloadKeys returns sorted keys from a payload map.
func payloadKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Sort for consistent comparison
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

// equalStringSlices compares two string slices for equality.
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// countFrames counts frames in raw fixture bytes.
func countFrames(t *testing.T, data []byte) int {
	t.Helper()
	decoder := NewFrameDecoder(bytes.NewReader(data))
	count := 0
	for {
		_, err := decoder.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}
		count++
	}
	return count
}
