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
 * Options for opening an artifact writer.
 * The artifact's length is declared before its first byte.
 */
export type EmitArtifactWriterOptions = {
  /** Human-readable name for the artifact */
  name: string
  /** MIME content type */
  content_type: string
  /** The artifact's length in bytes, at most 1 GiB */
  size_bytes: number
  /**
   * Optional hex SHA-256 the script expects of the bytes. The runtime
   * refuses the artifact event when its own digest differs.
   */
  sha256?: string
}

/**
 * A writer that sends one artifact's bytes in ranges, in any order, and
 * commits the artifact once its ranges cover it.
 */
export type ArtifactWriter = {
  /** The artifact's generated ID */
  readonly artifact_id: ArtifactId

  /**
   * Write `data` at `offset`. The range must lie inside the artifact and
   * must not overlap a range already written. Resolves once the runtime
   * has committed every byte of it.
   */
  writeAt(offset: number, data: Buffer | Uint8Array): Promise<void>

  /**
   * Emit the artifact event once the written ranges cover the artifact.
   * @returns The artifact's ID.
   */
  commit(): Promise<ArtifactId>
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
   * The SDK handles artifact_id generation and splits the data into ranges.
   * Equivalent to an artifact writer with one `writeAt` at offset 0.
   * @returns The generated artifact_id for reference.
   */
  readonly artifact: (options: EmitArtifactOptions) => Promise<ArtifactId>

  /**
   * Open an artifact writer for an artifact of known length, whose bytes
   * the script sends in ranges, so that it never holds the whole artifact.
   */
  readonly artifactWriter: (options: EmitArtifactWriterOptions) => Promise<ArtifactWriter>

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
   * Write an artifact_open frame, which declares the artifact's size and
   * any expected SHA-256 before its first range.
   * Per CONTRACT_IPC.md, bytes are written BEFORE the artifact event.
   * The artifact event is the commit record.
   */
  openArtifact(artifact_id: ArtifactId, size_bytes: number, expected_sha256?: string): Promise<void>

  /**
   * Write one artifact_range frame of at most ARTIFACT_RANGE_MAX_BYTES,
   * and resolve once the runtime has committed it.
   * A range the runtime refused rejects with ArtifactRangeRefusedError.
   */
  writeArtifactRange(
    artifact_id: ArtifactId,
    offset: number,
    data: Buffer | Uint8Array
  ): Promise<void>

  /**
   * Write a sidecar file via file_write frame.
   * Bypasses seq numbering and the policy pipeline.
   */
  writeFile(filename: string, contentType: string, data: Buffer | Uint8Array): Promise<void>
}

/**
 * Maximum bytes in one artifact range frame (8 MiB, CONTRACT_IPC.md
 * §Artifact Frames). The SDK splits longer writes into ranges of this size.
 *
 * @internal
 */
export const ARTIFACT_RANGE_MAX_BYTES = 8 * 1024 * 1024

/**
 * Maximum artifact size (1 GiB, CONTRACT_IPC.md §Size Constraints).
 *
 * @internal
 */
export const ARTIFACT_MAX_BYTES = 1024 * 1024 * 1024

/**
 * Brands of the errors a script may catch. The executor bundles its own
 * copy of the SDK, so an error it constructs is not an instance of the
 * class a script imports. Each class matches `instanceof` by its brand, a
 * symbol from the global registry that every copy shares.
 */
const RANGE_REFUSED_BRAND = Symbol.for('@pithecene/quarry-sdk/ArtifactRangeRefusedError')
const WRITER_ERROR_BRAND = Symbol.for('@pithecene/quarry-sdk/ArtifactWriterError')

/** Reports whether value carries brand, whichever SDK copy branded it. */
function hasBrand(value: unknown, brand: symbol): boolean {
  return typeof value === 'object' && value !== null && Reflect.get(value, brand) === true
}

/**
 * Error with which `emit.artifact()` and `ArtifactWriter.writeAt()` reject
 * when the runtime refused one of the artifact's ranges (CONTRACT_IPC.md
 * §Artifact Range Acknowledgement). The refusal is recoverable: the script
 * may catch it and continue emitting.
 */
export class ArtifactRangeRefusedError extends Error {
  /**
   * The offset of the refused range, when the SDK knows it. A rejected
   * `writeAt(offset, data)` committed the bytes from `offset` up to this
   * offset, and none from it on.
   */
  readonly offset: number | undefined

  readonly [RANGE_REFUSED_BRAND] = true

  constructor(message: string, offset?: number, options?: ErrorOptions) {
    super(message, options)
    this.name = 'ArtifactRangeRefusedError'
    this.offset = offset
  }

  static [Symbol.hasInstance](value: unknown): boolean {
    return hasBrand(value, RANGE_REFUSED_BRAND)
  }
}

/**
 * Error with which an artifact writer refuses a call before it sends
 * anything: invalid options, a range outside the artifact or overlapping
 * a written one, a commit before the ranges cover the artifact, or a call
 * after the commit. The refusal is recoverable: the script may catch it
 * and continue emitting.
 */
export class ArtifactWriterError extends Error {
  readonly [WRITER_ERROR_BRAND] = true

  constructor(message: string) {
    super(message)
    this.name = 'ArtifactWriterError'
  }

  static [Symbol.hasInstance](value: unknown): boolean {
    return hasBrand(value, WRITER_ERROR_BRAND)
  }
}
