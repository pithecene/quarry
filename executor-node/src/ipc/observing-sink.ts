/**
 * ObservingSink: Wraps an EmitSink to observe terminal event emissions.
 *
 * This allows the executor to determine outcome based on actually
 * written terminal events, not just emission attempts.
 *
 * Invariants:
 * - First terminal event wins: subsequent terminal events are ignored
 * - First sink failure wins: preserved even if later errors occur
 * - Fail-fast: after sink failure, subsequent writes throw immediately
 * - Single-writer assumption: NOT thread-safe, must be serialized externally
 *
 * @module
 */
import type { ArtifactId, EmitSink, EventEnvelope } from '@pithecene/quarry-sdk'

/**
 * Terminal event types.
 */
export type TerminalType = 'run_complete' | 'run_error'

/**
 * Captured terminal event state.
 * Type alone is authoritative; payload fields are best-effort extraction.
 */
export type TerminalState =
  | { readonly type: 'run_complete'; readonly summary?: Record<string, unknown> }
  | { readonly type: 'run_error'; readonly errorType?: string; readonly message?: string }

/**
 * Error thrown when attempting to write after a sink failure.
 */
export class SinkAlreadyFailedError extends Error {
  constructor(originalCause: unknown) {
    const causeMsg = originalCause instanceof Error ? originalCause.message : String(originalCause)
    super(`Sink has already failed: ${causeMsg}`)
    this.name = 'SinkAlreadyFailedError'
    this.cause = originalCause
  }
}

/**
 * Observable state for the sink.
 *
 * Precedence rules for executor:
 * 1. If isSinkFailed() → outcome is crash (regardless of terminal state)
 * 2. If getTerminalState() exists → outcome matches terminal event
 * 3. Otherwise → script completed without emitting terminal
 */
export type SinkState = {
  /**
   * Returns the first successfully written terminal event state.
   * Returns null if no terminal event has been written yet.
   *
   * Note: Even if this returns a value, the executor MUST check
   * isSinkFailed() first. A sink failure after terminal write
   * still means crash.
   */
  getTerminalState(): TerminalState | null

  /**
   * Returns true if the sink has failed at any point.
   * After a sink failure, the run should be treated as a crash.
   */
  isSinkFailed(): boolean

  /**
   * Returns the first sink failure cause.
   * Subsequent failures do not overwrite this.
   */
  getSinkFailure(): unknown
}

/**
 * Check if an event type is terminal.
 */
function isTerminalType(type: string): type is TerminalType {
  return type === 'run_complete' || type === 'run_error'
}

/**
 * Check if a value is a plain object (not null, array, or primitive).
 */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/**
 * Wraps an EmitSink to observe terminal event emissions.
 *
 * @remarks
 * **Single-writer assumption**: This sink is NOT thread-safe. Concurrent
 * calls will produce undefined behavior. The SDK's EmitAPI serializes calls
 * via promise chain, so this is safe when used through createEmitAPI.
 *
 * **First terminal wins**: Only the first successfully written terminal
 * event is recorded. Subsequent terminal events are written through but
 * do not update the observed state.
 *
 * **First failure wins**: The first sink failure is preserved as the root
 * cause. Subsequent failures (including SinkAlreadyFailedError) do not
 * overwrite it.
 *
 * **Fail-fast**: After the first sink failure, all subsequent writes
 * throw SinkAlreadyFailedError without attempting to write.
 *
 * **Failure precedence**: If the sink fails at any point (even after a
 * terminal event is written), the executor should treat the run as a crash.
 */
export class ObservingSink implements EmitSink, SinkState {
  private terminalState: TerminalState | null = null
  private sinkFailure: unknown = null

  constructor(private readonly inner: EmitSink) {}

  /**
   * Write an event envelope, tracking the first terminal event on success.
   * @throws SinkAlreadyFailedError if the sink has previously failed
   */
  async writeEvent(envelope: EventEnvelope): Promise<void> {
    // Fail-fast: don't attempt writes after failure
    if (this.sinkFailure !== null) {
      throw new SinkAlreadyFailedError(this.sinkFailure)
    }

    try {
      await this.inner.writeEvent(envelope)

      // Only track terminal state if not already set (first terminal wins)
      // Terminal type alone is authoritative
      if (this.terminalState === null && isTerminalType(envelope.type)) {
        this.terminalState = this.extractTerminalState(envelope.type, envelope.payload)
      }
    } catch (err) {
      // First failure wins: only set if not already set
      if (this.sinkFailure === null) {
        this.sinkFailure = err
      }
      throw err
    }
  }

  /**
   * Write artifact data, tracking failures.
   * @throws SinkAlreadyFailedError if the sink has previously failed
   */
  async writeArtifactData(artifactId: ArtifactId, data: Buffer | Uint8Array): Promise<void> {
    // Fail-fast: don't attempt writes after failure
    if (this.sinkFailure !== null) {
      throw new SinkAlreadyFailedError(this.sinkFailure)
    }

    try {
      await this.inner.writeArtifactData(artifactId, data)
    } catch (err) {
      // First failure wins: only set if not already set
      if (this.sinkFailure === null) {
        this.sinkFailure = err
      }
      throw err
    }
  }

  /**
   * Write a sidecar file, tracking failures.
   * @throws SinkAlreadyFailedError if the sink has previously failed
   */
  async writeFile(filename: string, contentType: string, data: Buffer | Uint8Array): Promise<void> {
    // Fail-fast: don't attempt writes after failure
    if (this.sinkFailure !== null) {
      throw new SinkAlreadyFailedError(this.sinkFailure)
    }

    try {
      await this.inner.writeFile(filename, contentType, data)
    } catch (err) {
      // First failure wins: only set if not already set
      if (this.sinkFailure === null) {
        this.sinkFailure = err
      }
      throw err
    }
  }

  /**
   * Extract terminal state from type and payload.
   * Type alone is authoritative; payload fields are best-effort.
   */
  private extractTerminalState(type: TerminalType, payload: unknown): TerminalState {
    if (type === 'run_error') {
      // Best-effort extraction of error fields
      let errorType: string | undefined
      let message: string | undefined

      if (isPlainObject(payload)) {
        if ('error_type' in payload && typeof payload.error_type === 'string') {
          errorType = payload.error_type
        }
        if ('message' in payload && typeof payload.message === 'string') {
          message = payload.message
        }
      }

      return { type: 'run_error', errorType, message }
    }

    // run_complete
    let summary: Record<string, unknown> | undefined
    if (isPlainObject(payload) && 'summary' in payload && isPlainObject(payload.summary)) {
      summary = payload.summary
    }

    return { type: 'run_complete', summary }
  }

  // SinkState implementation

  getTerminalState(): TerminalState | null {
    return this.terminalState
  }

  isSinkFailed(): boolean {
    return this.sinkFailure !== null
  }

  getSinkFailure(): unknown {
    return this.sinkFailure
  }
}
