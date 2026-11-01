import type { ArtifactId, CheckpointId, EventEnvelope, LogLevel } from './types/events'

/**
 * Options for emitting an item.
 */
export type EmitItemOptions = {
  /** Caller-defined type label for the item */
  item_type: string
  /** The record payload */
  data: Record<string, unknown>
}

/**
 * Options for emitting an artifact.
 * Binary data is passed separately from metadata.
 */
export type EmitArtifactOptions = {
  /** Human-readable name for the artifact */
  name: string
  /** MIME content type */
  content_type: string
  /** The binary data (Buffer or Uint8Array) */
  data: Buffer | Uint8Array
}

/**
 * Options for emitting a checkpoint.
 */
export type EmitCheckpointOptions = {
  /** Unique identifier for the checkpoint */
  checkpoint_id: CheckpointId
  /** Optional human-readable note */
  note?: string
}

/**
 * Options for emitting an enqueue advisory.
 */
export type EmitEnqueueOptions = {
  /** Target identifier for the work */
  target: string
  /** Parameters for the work */
  params: Record<string, unknown>
  /** Optional source partition override for the child run */
  source?: string
  /** Optional category partition override for the child run */
  category?: string
}

/**
 * Options for emitting a rotate_proxy advisory.
 */
export type EmitRotateProxyOptions = {
  /** Optional reason for the rotation request */
  reason?: string
}

/**
 * Options for emitting a log message.
 */
export type EmitLogOptions = {
  /** Log level */
  level: LogLevel
  /** Log message */
  message: string
  /** Optional structured fields */
  fields?: Record<string, unknown>
}

/**
 * Options for emitting a run error.
 */
export type EmitRunErrorOptions = {
  /** Error type/category */
  error_type: string
  /** Error message */
  message: string
  /** Optional stack trace */
  stack?: string
}

/**
 * Options for emitting run completion.
 */
export type EmitRunCompleteOptions = {
  /** Optional summary object */
  summary?: Record<string, unknown>
}

/**
 * The stable emit interface for extraction scripts.
 * This is the sole output mechanism for scripts.
 *
 * All methods are async because they may block on backpressure
 * (per CONTRACT_IPC.md).
 */
export type EmitAPI = {
  /**
   * Emit a structured item record.
   * This is the primary output mechanism for extracted data.
   */
  readonly item: (options: EmitItemOptions) => Promise<void>

  /**
   * Emit a binary artifact (screenshot, PDF, file download, etc.)
   * The SDK handles artifact_id generation and chunking coordination.
   * @returns The generated artifact_id for reference.
   */
  readonly artifact: (options: EmitArtifactOptions) => Promise<ArtifactId>

  /**
   * Emit a checkpoint to mark progress.
   * Useful for resumable scripts or progress tracking.
   */
  readonly checkpoint: (options: EmitCheckpointOptions) => Promise<void>

  /**
   * Advisory: suggest enqueueing additional work.
   * Not guaranteed to be acted upon.
   */
  readonly enqueue: (options: EmitEnqueueOptions) => Promise<void>

  /**
   * Advisory: suggest rotating proxy/session.
   * Not guaranteed to be acted upon.
   */
  readonly rotateProxy: (options?: EmitRotateProxyOptions) => Promise<void>

  /**
   * Emit structured log messages.
   */
  readonly log: (options: EmitLogOptions) => Promise<void>

  /** Convenience: emit a debug log */
  readonly debug: (message: string, fields?: Record<string, unknown>) => Promise<void>

  /** Convenience: emit an info log */
  readonly info: (message: string, fields?: Record<string, unknown>) => Promise<void>

  /** Convenience: emit a warning log */
  readonly warn: (message: string, fields?: Record<string, unknown>) => Promise<void>

  /** Convenience: emit an error log */
  readonly error: (message: string, fields?: Record<string, unknown>) => Promise<void>

  /**
   * Emit a fatal error and signal script termination.
   * The script should terminate shortly after calling this.
   * Further emit calls after runError are undefined behavior.
   */
  readonly runError: (options: EmitRunErrorOptions) => Promise<void>

  /**
   * Emit normal completion of the script.
   * Should be called at the end of successful execution.
   * Note: The executor may call this automatically if the script
   * returns without error and hasn't already emitted run_complete.
   */
  readonly runComplete: (options?: EmitRunCompleteOptions) => Promise<void>
}

/**
 * Options for writing a sidecar file via ctx.storage.put().
 */
export type StoragePutOptions = {
  /** Target filename (no path separators, no "..") */
  filename: string
  /** MIME content type */
  content_type: string
  /** The binary data (Buffer or Uint8Array, max 8 MiB) */
  data: Buffer | Uint8Array
}

/**
 * Result of a storage put operation.
 * Contains the resolved Hive-partitioned storage key.
 */
export type StoragePutResult = {
  /** The resolved Hive-partitioned storage key (e.g. "datasets/.../files/filename") */
  readonly key: string
}

/**
 * Storage API for sidecar file uploads via Lode Store.
 * Files land at Hive-partitioned paths under files/, bypassing
 * Dataset segment/manifest machinery entirely.
 */
export type StorageAPI = {
  /**
   * Write a file to Hive-partitioned storage.
   * Shares the emit serialization chain for ordering and fail-fast.
   * Returns the resolved storage key for downstream correlation.
   */
  readonly put: (options: StoragePutOptions) => Promise<StoragePutResult>
}

/**
 * Storage partition metadata passed from the runtime via executor stdin.
 * Used by the SDK to compute deterministic storage keys without
 * a bidirectional IPC round-trip.
 *
 * @internal
 */
export type StoragePartitionMeta = {
  readonly dataset: string
  readonly source: string
  readonly category: string
  readonly day: string
  readonly run_id: string
}

/**
 * Internal interface for emit sink.
 * The executor-node will provide an implementation of this.
 * This is NOT part of the public SDK API.
 *
 * @internal
 */
export type EmitSink = {
  /**
   * Write a complete event envelope.
   * May block on backpressure.
   */
  writeEvent(envelope: EventEnvelope): Promise<void>

  /**
   * Write artifact binary data: an artifact_open frame with the data's
   * size, then its bytes as artifact_range frames.
   * Per CONTRACT_IPC.md, bytes are written BEFORE the artifact event.
   * The artifact event is the commit record.
   * The sink splits the data into ranges per CONTRACT_IPC.md, and resolves
   * once the runtime has committed every range.
   */
  writeArtifactData(artifact_id: ArtifactId, data: Buffer | Uint8Array): Promise<void>

  /**
   * Write a sidecar file via file_write frame.
   * Bypasses seq numbering and the policy pipeline.
   */
  writeFile(filename: string, contentType: string, data: Buffer | Uint8Array): Promise<void>
}

/**
 * Error with which `emit.artifact()` rejects when the runtime refused one
 * of the artifact's ranges (CONTRACT_IPC.md §Artifact Range
 * Acknowledgement). The refusal is recoverable: the script may catch it
 * and continue emitting.
 */
export class ArtifactRangeRefusedError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ArtifactRangeRefusedError'
  }
}
