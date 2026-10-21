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
	"sync"
	"testing"
	"time"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

// TestE2E_FileWriteAck_Roundtrip spawns the real Node executor with a script
// that calls ctx.storage.put(). The test exercises the full bidirectional IPC:
//
//  1. Writes JSON metadata to stdin (phase 1)
//  2. Concurrently reads file_write frames from stdout
//  3. Writes file_write_ack frames back on stdin (phase 2)
//  4. Validates the run completes with terminal event
//
// This is the first test that exercises the two-phase stdin protocol with
// real subprocess pipes per CONTRACT_IPC.md.
func TestE2E_FileWriteAck_Roundtrip(t *testing.T) {
	if os.Getenv("QUARRY_E2E") != "1" {
		t.Skip("QUARRY_E2E=1 not set, skipping live E2E test")
	}

	// Explicit timeout — a hanging test is a deadlock, not a slow test.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	paths := resolveTestPaths(t)
	checkNodeAvailable(t)
	checkExecutorBuilt(t, paths)

	scriptPath := filepath.Join(paths.fixtureDir, "e2e-storage-put-script.js")
	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		t.Skipf("fixture script not found at %s", scriptPath)
	}

	// Phase 1: prepare JSON metadata for stdin.
	// Go's json.NewEncoder appends \n — the executor's readStdinMetadata
	// reads until the first newline, then keeps stdin open for phase 2.
	input := map[string]any{
		"run_id":  "run-e2e-ack-001",
		"attempt": 1,
		"job":     map[string]any{"test": "file_write_ack"},
		"storage": map[string]any{
			"dataset":  "test-dataset",
			"source":   "e2e",
			"category": "default",
			"day":      "2024-01-15",
			"run_id":   "run-e2e-ack-001",
		},
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	inputJSON = append(inputJSON, '\n')

	// Bidirectional pipes. We write to stdinWriter; executor reads stdinReader.
	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe (stdin): %v", err)
	}

	// Stdout pipe. Executor writes to stdoutWriter; we read stdoutReader.
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe (stdout): %v", err)
	}

	cmd := exec.CommandContext(ctx, "node", paths.executorBin, scriptPath)
	cmd.Stdin = stdinReader
	cmd.Stdout = stdoutWriter
	cmd.Env = append(os.Environ(), "QUARRY_NO_SANDBOX=1")

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start executor: %v", err)
	}
	// Close our copies of the child's pipe ends.
	iox.DiscardClose(stdinReader)
	iox.DiscardClose(stdoutWriter)

	// Write phase 1: JSON metadata
	if _, err := stdinWriter.Write(inputJSON); err != nil {
		t.Fatalf("write metadata: %v", err)
	}

	// Concurrently read frames from stdout and send acks on stdin.
	var (
		mu         sync.Mutex
		events     []*types.EventEnvelope
		fileWrites []*types.FileWriteFrame
		runResults []*types.RunResultFrame
	)

	// Frame reader goroutine: reads stdout, sends acks for file_write frames.
	readerDone := make(chan error, 1)
	go func() {
		defer close(readerDone)
		decoder := NewFrameDecoder(stdoutReader)

		for {
			payload, err := decoder.ReadFrame()
			if errors.Is(err, io.EOF) {
				readerDone <- nil
				return
			}
			if err != nil {
				readerDone <- err
				return
			}

			frame, err := DecodeFrame(payload)
			if err != nil {
				readerDone <- err
				return
			}

			mu.Lock()
			switch f := frame.(type) {
			case *types.EventEnvelope:
				events = append(events, f)
			case *types.FileWriteFrame:
				fileWrites = append(fileWrites, f)

				// Send success ack back on stdin (phase 2)
				ack := &types.FileWriteAckFrame{
					Type:    FileWriteAckType,
					WriteID: f.WriteID,
					OK:      true,
				}
				ackFrame, encErr := EncodeFileWriteAck(ack)
				if encErr != nil {
					mu.Unlock()
					readerDone <- encErr
					return
				}
				if _, writeErr := stdinWriter.Write(ackFrame); writeErr != nil {
					// EPIPE is expected if executor already exited
					t.Logf("ack write (write_id=%d): %v", f.WriteID, writeErr)
				}
			case *types.RunResultFrame:
				runResults = append(runResults, f)
			}
			mu.Unlock()
		}
	}()

	// Wait for the frame reader to finish (executor closes stdout on exit)
	if err := <-readerDone; err != nil {
		t.Fatalf("frame reader: %v", err)
	}

	// Close stdin writer — signals EOF to AckReader
	iox.DiscardClose(stdinWriter)

	// Wait for process to exit
	cmdErr := cmd.Wait()
	if stderr.Len() > 0 {
		t.Logf("executor stderr:\n%s", stderr.String())
	}

	if cmdErr != nil {
		var exitErr *exec.ExitError
		if errors.As(cmdErr, &exitErr) && exitErr.ExitCode() >= 2 {
			t.Fatalf("executor crashed (exit %d)", exitErr.ExitCode())
		}
	}

	// --- Assertions ---

	mu.Lock()
	defer mu.Unlock()

	// Must have at least one file_write frame (from storage.put())
	if len(fileWrites) == 0 {
		t.Fatal("expected at least one file_write frame from storage.put()")
	}

	// file_write must have write_id > 0 (new executor with ack support)
	for i, fw := range fileWrites {
		if fw.WriteID == 0 {
			t.Errorf("fileWrites[%d] has write_id=0, expected > 0", i)
		}
		if fw.Filename != "report.json" {
			t.Errorf("fileWrites[%d] filename=%q, want %q", i, fw.Filename, "report.json")
		}
	}

	// Must have events
	if len(events) == 0 {
		t.Fatal("expected at least one event")
	}

	// Must have a terminal event (run_complete or run_error)
	hasTerminal := false
	for _, env := range events {
		if env.Type.IsTerminal() {
			hasTerminal = true
		}
	}
	if !hasTerminal {
		t.Error("no terminal event found — storage.put() may have hung waiting for ack")
	}

	// Should have a run_result control frame
	if len(runResults) == 0 {
		t.Error("expected a run_result control frame")
	} else if runResults[0].Outcome.Status != types.RunResultStatusCompleted {
		t.Errorf("run_result status=%q, want %q",
			runResults[0].Outcome.Status, types.RunResultStatusCompleted)
	}

	t.Logf("roundtrip OK: %d events, %d file_writes (acked), %d run_results",
		len(events), len(fileWrites), len(runResults))
}
