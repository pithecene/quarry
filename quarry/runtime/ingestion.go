package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/pithecene/quarry/ipc"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/log"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// IngestionError classifies ingestion errors for outcome determination.
type IngestionError struct {
	// Kind indicates whether this is a stream/frame error or a policy error.
	Kind IngestionErrorKind
	// Err is the underlying error.
	Err error
}

// IngestionErrorKind classifies ingestion errors.
type IngestionErrorKind int

const (
	// IngestionErrorStream indicates a frame/stream error (executor crash outcome).
	IngestionErrorStream IngestionErrorKind = iota
	// IngestionErrorPolicy indicates a policy failure (policy failure outcome).
	IngestionErrorPolicy
	// IngestionErrorCanceled indicates context cancellation (executor crash outcome).
	IngestionErrorCanceled
	// IngestionErrorVersionMismatch indicates a contract version mismatch (SDK/CLI version skew).
	IngestionErrorVersionMismatch
)

func (e *IngestionError) Error() string {
	return e.Err.Error()
}

func (e *IngestionError) Unwrap() error {
	return e.Err
}

// IsPolicyError returns true if the error is a policy failure.
func IsPolicyError(err error) bool {
	var ingErr *IngestionError
	if errors.As(err, &ingErr) {
		return ingErr.Kind == IngestionErrorPolicy
	}
	return false
}

// IsCanceledError returns true if the error is due to context cancellation.
func IsCanceledError(err error) bool {
	var ingErr *IngestionError
	if errors.As(err, &ingErr) {
		return ingErr.Kind == IngestionErrorCanceled
	}
	return false
}

// IsVersionMismatchError returns true if the error is a contract version mismatch.
func IsVersionMismatchError(err error) bool {
	var ingErr *IngestionError
	if errors.As(err, &ingErr) {
		return ingErr.Kind == IngestionErrorVersionMismatch
	}
	return false
}

// IsStreamError returns true if the error is a stream/frame error.
func IsStreamError(err error) bool {
	var ingErr *IngestionError
	if errors.As(err, &ingErr) {
		return ingErr.Kind == IngestionErrorStream
	}
	return false
}

// errContractVersionMismatch is a sentinel error for contract version mismatches.
// Used to distinguish version skew from other envelope validation failures
// (run_id mismatch, attempt mismatch) which remain stream errors.
var errContractVersionMismatch = errors.New("contract version mismatch")

// EnqueueObserver is a callback invoked when an enqueue event is received.
// Called synchronously between artifact handling and policy dispatch.
// Implementations must not perform blocking I/O; brief mutex acquisition
// for dedup bookkeeping is acceptable.
type EnqueueObserver func(*types.EventEnvelope)

// IngestionEngine handles IPC frame ingestion.
// Per CONTRACT_IPC.md and CONTRACT_EMIT.md:
//   - Frames are read in order
//   - Sequence numbers must be strictly monotonic (1, 2, 3...)
//   - First terminal event wins; subsequent terminals ignored
//   - Invalid framing is fatal (no resync)
//   - Policy failure on non-droppable events terminates run
//   - run_result control frames do not affect seq ordering
type IngestionEngine struct {
	decoder          *ipc.FrameDecoder
	policy           policy.Policy
	artifacts        *ArtifactManager
	fileWriter       lode.FileWriter // sidecar file writes, may be nil
	logger           *log.Logger
	runMeta          *types.RunMeta // for envelope validation
	collector        *metrics.Collector
	enqueueObserver  EnqueueObserver // optional fan-out observer, may be nil
	ackWriter        io.Writer       // stdin pipe for file_write_ack frames, may be nil
	currentSeq       int64
	terminalSeen     bool
	terminalEvent    *types.EventEnvelope
	runResult        *types.RunResultFrame // control frame, not counted in seq
}

// NewIngestionEngine creates a new ingestion engine.
// The fileWriter parameter may be nil if sidecar file writes are not supported.
// The observer parameter may be nil if fan-out is not enabled.
// The ackWriter parameter may be nil for backward compatibility (no ack frames sent).
func NewIngestionEngine(
	reader io.Reader,
	pol policy.Policy,
	artifacts *ArtifactManager,
	fileWriter lode.FileWriter,
	logger *log.Logger,
	runMeta *types.RunMeta,
	collector *metrics.Collector,
	observer EnqueueObserver,
	ackWriter io.Writer,
) *IngestionEngine {
	return &IngestionEngine{
		decoder:         ipc.NewFrameDecoder(reader),
		policy:          pol,
		artifacts:       artifacts,
		fileWriter:      fileWriter,
		logger:          logger,
		runMeta:         runMeta,
		collector:       collector,
		enqueueObserver: observer,
		ackWriter:       ackWriter,
		currentSeq:      0,
	}
}

// Run runs the ingestion loop until EOF or fatal error.
// Returns:
//   - nil: stream ended cleanly (EOF)
//   - *IngestionError with Kind=IngestionErrorStream: frame/stream error
//   - *IngestionError with Kind=IngestionErrorPolicy: policy failure
//   - *IngestionError with Kind=IngestionErrorCanceled: context canceled
func (e *IngestionEngine) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return &IngestionError{
				Kind: IngestionErrorCanceled,
				Err:  ctx.Err(),
			}
		default:
		}

		// Read frame
		payload, err := e.decoder.ReadFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Stream ended cleanly
				return nil
			}

			// If terminal event was already received and persisted,
			// treat pipe closure errors as acceptable completion.
			// Per CONTRACT_IPC.md: outcome is determined by terminal event,
			// and pipe closure after terminal is normal executor exit behavior.
			if e.terminalSeen {
				e.logger.Debug("pipe closed after terminal event (expected)", map[string]any{
					"error": err.Error(),
				})
				return nil
			}

			// Frame errors before terminal are stream errors (executor crash outcome)
			e.logger.Error("frame error", map[string]any{
				"error": err.Error(),
			})
			e.collector.IncExecutorCrash()
			return &IngestionError{
				Kind: IngestionErrorStream,
				Err:  fmt.Errorf("frame error: %w", err),
			}
		}

		// Decode and process frame
		if err := e.processFrame(ctx, payload); err != nil {
			// Count stream errors as executor crashes — decode failures,
			// envelope validation, sequence violations all indicate executor
			// misbehavior and produce crash outcomes.
			// Version mismatch is NOT an executor crash — it's a configuration error.
			if IsStreamError(err) {
				e.collector.IncExecutorCrash()
			}
			return err
		}
	}
}

// processFrame decodes and processes a single frame.
func (e *IngestionEngine) processFrame(ctx context.Context, payload []byte) error {
	// Decode frame - discriminates by type field
	decoded, err := ipc.DecodeFrame(payload)
	if err != nil {
		e.logger.Error("frame decode error", map[string]any{
			"error": err.Error(),
		})
		// ipc_decode_errors counts the failure type; executor_crash is counted
		// separately at the stream-error return site in Run(). Both incrementing
		// for the same event is intentional — different dimensions.
		e.collector.IncIPCDecodeErrors()
		// Decode errors are stream errors (executor crash outcome)
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  fmt.Errorf("frame decode error: %w", err),
		}
	}

	// Handle based on frame type
	switch frame := decoded.(type) {
	case *types.ArtifactChunkFrame:
		return e.processArtifactChunk(ctx, frame)
	case *types.EventEnvelope:
		return e.processEvent(ctx, frame)
	case *types.RunResultFrame:
		return e.processRunResult(frame)
	case *types.FileWriteFrame:
		return e.processFileWrite(ctx, frame)
	default:
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  fmt.Errorf("unexpected frame type: %T", decoded),
		}
	}
}

// processEvent processes an event envelope.
func (e *IngestionEngine) processEvent(ctx context.Context, envelope *types.EventEnvelope) error {
	// Validate envelope against run metadata
	if err := e.validateEnvelope(envelope); err != nil {
		e.logger.Error("envelope validation failed", map[string]any{
			"error": err.Error(),
			"type":  envelope.Type,
			"seq":   envelope.Seq,
		})
		// Contract version mismatch is a distinct error kind (SDK/CLI version skew)
		if errors.Is(err, errContractVersionMismatch) {
			return &IngestionError{
				Kind: IngestionErrorVersionMismatch,
				Err:  fmt.Errorf("envelope validation failed: %w", err),
			}
		}
		// Other envelope validation errors are stream errors (executor misbehavior)
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  fmt.Errorf("envelope validation failed: %w", err),
		}
	}

	// Validate sequence ordering per CONTRACT_EMIT.md
	expectedSeq := e.currentSeq + 1
	if envelope.Seq != expectedSeq {
		e.logger.Error("sequence violation", map[string]any{
			"expected": expectedSeq,
			"got":      envelope.Seq,
			"type":     envelope.Type,
		})
		// Sequence violation is a stream error (executor misbehavior)
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  fmt.Errorf("sequence violation: expected %d, got %d", expectedSeq, envelope.Seq),
		}
	}
	e.currentSeq = envelope.Seq

	// Check for terminal events
	if envelope.Type.IsTerminal() {
		if e.terminalSeen {
			// Per CONTRACT_EMIT.md, first terminal wins, subsequent ignored
			e.logger.Warn("ignoring duplicate terminal event", map[string]any{
				"type": envelope.Type,
				"seq":  envelope.Seq,
			})
			return nil
		}

		e.terminalSeen = true
		e.terminalEvent = envelope

		e.logger.Info("terminal event received", map[string]any{
			"type": envelope.Type,
			"seq":  envelope.Seq,
		})
	}

	// Handle artifact commit
	if envelope.Type == types.EventTypeArtifact {
		if err := e.handleArtifactCommit(envelope); err != nil {
			// Artifact errors are stream errors (executor/data misbehavior)
			return &IngestionError{
				Kind: IngestionErrorStream,
				Err:  err,
			}
		}
	}

	// Notify fan-out observer before policy dispatch.
	// Scheduling is independent of whether the policy drops the enqueue event.
	if envelope.Type == types.EventTypeEnqueue && e.enqueueObserver != nil {
		e.enqueueObserver(envelope)
	}

	// Delegate to policy
	if err := e.policy.IngestEvent(ctx, envelope); err != nil {
		// Policy failure terminates run per CONTRACT_POLICY.md
		e.logger.Error("policy ingestion failed", map[string]any{
			"event_type": envelope.Type,
			"seq":        envelope.Seq,
			"error":      err.Error(),
		})
		return &IngestionError{
			Kind: IngestionErrorPolicy,
			Err:  fmt.Errorf("policy failure: %w", err),
		}
	}

	return nil
}

// validateEnvelope validates envelope fields against run metadata.
func (e *IngestionEngine) validateEnvelope(envelope *types.EventEnvelope) error {
	// Validate contract version
	if envelope.ContractVersion != types.ContractVersion {
		return fmt.Errorf("%w: expected %s, got %s",
			errContractVersionMismatch, types.ContractVersion, envelope.ContractVersion)
	}

	// Validate run_id matches
	if envelope.RunID != e.runMeta.RunID {
		return fmt.Errorf("run_id mismatch: expected %s, got %s",
			e.runMeta.RunID, envelope.RunID)
	}

	// Validate attempt matches
	if envelope.Attempt != e.runMeta.Attempt {
		return fmt.Errorf("attempt mismatch: expected %d, got %d",
			e.runMeta.Attempt, envelope.Attempt)
	}

	return nil
}

// handleArtifactCommit processes an artifact event (the commit record).
func (e *IngestionEngine) handleArtifactCommit(envelope *types.EventEnvelope) error {
	// Extract artifact metadata from payload
	artifactID, _ := envelope.Payload["artifact_id"].(string)
	if artifactID == "" {
		return errors.New("artifact event missing artifact_id")
	}

	// size_bytes may come as various integer types from msgpack encoding
	// msgpack uses the smallest encoding that fits the value
	var sizeBytes int64
	switch v := envelope.Payload["size_bytes"].(type) {
	case int64:
		sizeBytes = v
	case int:
		sizeBytes = int64(v)
	case int8:
		sizeBytes = int64(v)
	case int16:
		sizeBytes = int64(v)
	case int32:
		sizeBytes = int64(v)
	case uint:
		sizeBytes = int64(v)
	case uint8:
		sizeBytes = int64(v)
	case uint16:
		sizeBytes = int64(v)
	case uint32:
		sizeBytes = int64(v)
	case uint64:
		sizeBytes = int64(v)
	case float64:
		sizeBytes = int64(v)
	default:
		return fmt.Errorf("artifact event has invalid size_bytes type: %T", envelope.Payload["size_bytes"])
	}

	if err := e.artifacts.CommitArtifact(artifactID, sizeBytes); err != nil {
		e.logger.Error("artifact commit failed", map[string]any{
			"artifact_id": artifactID,
			"size_bytes":  sizeBytes,
			"error":       err.Error(),
		})
		return fmt.Errorf("artifact commit failed: %w", err)
	}

	e.logger.Debug("artifact committed", map[string]any{
		"artifact_id": artifactID,
		"size_bytes":  sizeBytes,
	})

	return nil
}

// processArtifactChunk processes an artifact chunk frame.
func (e *IngestionEngine) processArtifactChunk(ctx context.Context, frame *types.ArtifactChunkFrame) error {
	// Validate chunk per CONTRACT_IPC.md
	if frame.Seq < 1 {
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  fmt.Errorf("invalid chunk seq: %d", frame.Seq),
		}
	}

	if len(frame.Data) > ipc.MaxChunkSize {
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  fmt.Errorf("chunk data exceeds max size: %d > %d", len(frame.Data), ipc.MaxChunkSize),
		}
	}

	// Convert to internal chunk type
	chunk := &types.ArtifactChunk{
		ArtifactID: frame.ArtifactID,
		Seq:        frame.Seq,
		IsLast:     frame.IsLast,
		Data:       frame.Data,
	}

	// Add to artifact manager
	if err := e.artifacts.AddChunk(chunk); err != nil {
		e.logger.Error("artifact chunk rejected", map[string]any{
			"artifact_id": chunk.ArtifactID,
			"seq":         chunk.Seq,
			"is_last":     chunk.IsLast,
			"error":       err.Error(),
		})
		// Artifact errors are stream errors (executor/data misbehavior)
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  fmt.Errorf("artifact chunk failed: %w", err),
		}
	}

	// Delegate to policy
	if err := e.policy.IngestArtifactChunk(ctx, chunk); err != nil {
		e.logger.Error("policy chunk ingestion failed", map[string]any{
			"artifact_id": chunk.ArtifactID,
			"seq":         chunk.Seq,
			"error":       err.Error(),
		})
		return &IngestionError{
			Kind: IngestionErrorPolicy,
			Err:  fmt.Errorf("policy chunk failure: %w", err),
		}
	}

	return nil
}

// GetTerminalEvent returns the terminal event if seen.
func (e *IngestionEngine) GetTerminalEvent() (*types.EventEnvelope, bool) {
	return e.terminalEvent, e.terminalSeen
}

// HasTerminal returns true if a terminal event has been seen.
func (e *IngestionEngine) HasTerminal() bool {
	return e.terminalSeen
}

// CurrentSeq returns the current sequence number.
func (e *IngestionEngine) CurrentSeq() int64 {
	return e.currentSeq
}

// processRunResult processes a run result control frame.
// Per CONTRACT_IPC.md, run_result is a control frame that:
//   - Does NOT affect seq ordering (not counted as an event)
//   - Is emitted once after terminal event emission
//   - Contains outcome and optional proxy_used (redacted)
func (e *IngestionEngine) processRunResult(frame *types.RunResultFrame) error {
	// Only accept the first run_result frame
	if e.runResult != nil {
		e.logger.Warn("ignoring duplicate run_result frame", nil)
		return nil
	}

	e.runResult = frame
	e.logger.Debug("run_result frame received", map[string]any{
		"status":    frame.Outcome.Status,
		"has_proxy": frame.ProxyUsed != nil,
	})

	return nil
}

// processFileWrite processes a file_write frame.
// File writes bypass seq numbering and the policy pipeline.
//
// Error model: PutFile failures send an error ack but do NOT terminate the
// ingestion loop. The executor's storage.put() promise rejects, and the
// script decides how to handle it. Validation errors (empty filename, path
// traversal) remain fatal stream errors.
func (e *IngestionEngine) processFileWrite(ctx context.Context, frame *types.FileWriteFrame) error {
	// Reject file writes after terminal event — send error ack to guarantee
	// promise settlement on the executor side, then discard.
	if e.terminalSeen {
		e.logger.Warn("rejecting file_write after terminal event", map[string]any{
			"filename": frame.Filename,
			"write_id": frame.WriteID,
		})
		e.sendFileWriteAck(frame.WriteID, false, "run already terminated")
		return nil
	}

	// Validate filename: no path separators, no ".."
	if frame.Filename == "" {
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  errors.New("file_write: empty filename"),
		}
	}
	if strings.Contains(frame.Filename, "/") || strings.Contains(frame.Filename, "\\") {
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  fmt.Errorf("file_write: filename contains path separator: %s", frame.Filename),
		}
	}
	if strings.Contains(frame.Filename, "..") {
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  fmt.Errorf("file_write: filename contains '..': %s", frame.Filename),
		}
	}

	// Validate data size (same 8 MiB limit as artifact chunks)
	if len(frame.Data) > ipc.MaxChunkSize {
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  fmt.Errorf("file_write: data size %d exceeds max %d", len(frame.Data), ipc.MaxChunkSize),
		}
	}

	if e.fileWriter == nil {
		return &IngestionError{
			Kind: IngestionErrorStream,
			Err:  errors.New("file_write received but no FileWriter configured; ensure storage is properly configured for sidecar file support"),
		}
	}

	if err := e.fileWriter.PutFile(ctx, frame.Filename, frame.ContentType, frame.Data); err != nil {
		e.logger.Error("file_write failed", map[string]any{
			"filename": frame.Filename,
			"error":    err.Error(),
			"write_id": frame.WriteID,
		})
		e.collector.IncLodeWriteFailure()

		// Send error ack — recoverable, ingestion continues
		e.sendFileWriteAck(frame.WriteID, false, err.Error())
		return nil
	}

	e.logger.Debug("file written", map[string]any{
		"filename":     frame.Filename,
		"content_type": frame.ContentType,
		"size_bytes":   len(frame.Data),
		"write_id":     frame.WriteID,
	})

	// Send success ack
	e.sendFileWriteAck(frame.WriteID, true, "")
	return nil
}

// sendFileWriteAck writes a file_write_ack frame to the executor's stdin.
// No-op if ackWriter is nil (backward compat) or writeId is 0 (legacy frame).
// Ack write failures are logged but non-fatal (executor may have exited).
func (e *IngestionEngine) sendFileWriteAck(writeID uint32, ok bool, errMsg string) {
	if e.ackWriter == nil || writeID == 0 {
		return
	}

	ack := &types.FileWriteAckFrame{
		Type:    ipc.FileWriteAckType,
		WriteID: writeID,
		OK:      ok,
	}
	if errMsg != "" {
		ack.Error = &errMsg
	}

	frame, err := ipc.EncodeFileWriteAck(ack)
	if err != nil {
		e.logger.Warn("failed to encode file_write_ack", map[string]any{
			"write_id": writeID,
			"error":    err.Error(),
		})
		return
	}

	if _, err := e.ackWriter.Write(frame); err != nil {
		// EPIPE or similar — executor may have exited. Non-fatal.
		e.logger.Warn("failed to write file_write_ack (executor may have exited)", map[string]any{
			"write_id": writeID,
			"error":    err.Error(),
		})
	}
}

// GetRunResult returns the run result frame if received.
func (e *IngestionEngine) GetRunResult() *types.RunResultFrame {
	return e.runResult
}
