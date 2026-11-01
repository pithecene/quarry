import { randomUUID } from 'node:crypto'
import type {
  ArtifactWriter,
  EmitAPI,
  EmitArtifactOptions,
  EmitArtifactWriterOptions,
  EmitCheckpointOptions,
  EmitEnqueueOptions,
  EmitItemOptions,
  EmitLogOptions,
  EmitRotateProxyOptions,
  EmitRunCompleteOptions,
  EmitRunErrorOptions,
  EmitSink,
  StorageAPI,
  StoragePartitionMeta,
  StoragePutOptions,
  StoragePutResult
} from './emit'
import {
  ARTIFACT_MAX_BYTES,
  ARTIFACT_RANGE_MAX_BYTES,
  ArtifactRangeRefusedError,
  ArtifactWriterError
} from './emit'
import { RangeSet } from './range-set'
import type { RunMeta } from './types/context'
import type { ArtifactId, EventEnvelope, EventId, EventType, PayloadMap } from './types/events'
import { CONTRACT_VERSION } from './types/events'

/**
 * Error thrown when attempting to emit after a terminal event.
 */
export class TerminalEventError extends Error {
  constructor() {
    super('Cannot emit: a terminal event (run_error or run_complete) has already been emitted')
    this.name = 'TerminalEventError'
  }
}

/**
 * Error thrown when attempting to emit after a sink failure.
 */
export class SinkFailedError extends Error {
  constructor(cause: unknown) {
    super('Cannot emit: sink has previously failed')
    this.name = 'SinkFailedError'
    this.cause = cause
  }
}

/**
 * Error thrown when a storage filename is invalid.
 */
export class StorageFilenameError extends Error {
  constructor(filename: string, reason: string) {
    super(`Invalid storage filename "${filename}": ${reason}`)
    this.name = 'StorageFilenameError'
  }
}

/**
 * Compute the Hive-partitioned storage key for a sidecar file.
 * Must exactly match Go's buildFilePath() in quarry/lode/file_writer.go.
 *
 * Format: datasets/{dataset}/partitions/source={source}/category={category}/day={day}/run_id={runID}/files/{filename}
 */
export function buildStorageKey(partition: StoragePartitionMeta, filename: string): string {
  return `datasets/${partition.dataset}/partitions/source=${partition.source}/category=${partition.category}/day=${partition.day}/run_id=${partition.run_id}/files/${filename}`
}

/** Hex SHA-256: 64 hex digits, as the runtime compares it (any case). */
const SHA256_HEX = /^[0-9a-f]{64}$/i

/**
 * Errors after which the script may emit on: a range the runtime refused,
 * and a call an artifact writer refused before sending anything.
 */
function isRecoverable(err: unknown): boolean {
  return err instanceof ArtifactRangeRefusedError || err instanceof ArtifactWriterError
}

/**
 * Refuse writer options the runtime would refuse: a size that is not a
 * whole number of bytes up to ARTIFACT_MAX_BYTES, and an expected digest
 * that is no hex SHA-256.
 */
function validateWriterOptions(options: EmitArtifactWriterOptions): void {
  const size = options.size_bytes
  if (!Number.isSafeInteger(size) || size < 0 || size > ARTIFACT_MAX_BYTES) {
    throw new ArtifactWriterError(
      `artifact size_bytes ${size} is not a whole number from 0 to ${ARTIFACT_MAX_BYTES}`
    )
  }
  if (options.sha256 !== undefined && !SHA256_HEX.test(options.sha256)) {
    throw new ArtifactWriterError(`artifact sha256 "${options.sha256}" is not a hex SHA-256`)
  }
}

/**
 * Split a write at offset into ranges of at most ARTIFACT_RANGE_MAX_BYTES,
 * each with its own offset.
 */
function* splitRanges(
  offset: number,
  data: Buffer | Uint8Array
): Generator<readonly [number, Buffer | Uint8Array]> {
  for (let at = 0; at < data.byteLength; at += ARTIFACT_RANGE_MAX_BYTES) {
    yield [offset + at, data.subarray(at, at + ARTIFACT_RANGE_MAX_BYTES)]
  }
}

/**
 * One open artifact: its declared metadata and the ranges the runtime
 * has committed. It checks each call before anything is sent, so the
 * runtime sees no range or artifact event it would refuse for its shape.
 */
class OpenArtifact {
  private readonly written = new RangeSet()
  private committed = false

  constructor(
    readonly artifact_id: ArtifactId,
    readonly options: EmitArtifactWriterOptions
  ) {}

  /**
   * Refuse a write after the commit, an empty range, or a range outside
   * the artifact or overlapping a written one.
   */
  checkWrite(offset: number, length: number): void {
    this.checkOpen()
    const size = this.options.size_bytes
    if (!Number.isSafeInteger(offset) || offset < 0 || length === 0 || length > size - offset) {
      throw new ArtifactWriterError(
        `artifact ${this.artifact_id}: range of ${length} bytes at ${offset} is empty or outside [0, ${size})`
      )
    }
    if (this.written.overlaps(offset, offset + length)) {
      throw new ArtifactWriterError(
        `artifact ${this.artifact_id}: range of ${length} bytes at ${offset} overlaps a written range`
      )
    }
  }

  /** Record a range the runtime committed. */
  wrote(offset: number, length: number): void {
    this.written.add(offset, offset + length)
  }

  /** Refuse a commit after the commit, or before the ranges cover the artifact. */
  checkCommit(): void {
    this.checkOpen()
    if (!this.written.covers(this.options.size_bytes)) {
      throw new ArtifactWriterError(
        `artifact ${this.artifact_id}: written ranges do not cover its ${this.options.size_bytes} bytes`
      )
    }
  }

  /** Mark the artifact committed: its artifact event is written. */
  commit(): void {
    this.committed = true
  }

  private checkOpen(): void {
    if (this.committed) {
      throw new ArtifactWriterError(`artifact ${this.artifact_id} is committed`)
    }
  }
}

/**
 * Create both EmitAPI and StorageAPI backed by a shared EmitSink.
 *
 * Both APIs share a single promise chain for ordering and fail-fast.
 * Emission is serialized to ensure strict ordering and correct
 * terminal state handling.
 *
 * @param run - Run metadata
 * @param sink - Emit sink for writing events/artifacts/files
 * @param storagePartition - Optional storage partition metadata for key computation.
 *   When provided, storage.put() returns the resolved storage key.
 *   When absent, storage.put() returns an empty key (pre-v1.0 behavior).
 *
 * @internal
 */
export function createAPIs(
  run: RunMeta,
  sink: EmitSink,
  storagePartition?: StoragePartitionMeta
): { emit: EmitAPI; storage: StorageAPI } {
  let seq = 0
  let terminalEmitted = false
  let sinkFailed: unknown = null
  let chain = Promise.resolve()

  /**
   * Serialize an operation through the promise chain.
   * Ensures strict ordering and prevents concurrent interleaving.
   * Fail-fast: after first sink failure, all subsequent operations fail.
   */
  function serialize<T>(fn: () => Promise<T>): Promise<T> {
    const result = chain.then(async () => {
      if (sinkFailed !== null) {
        throw new SinkFailedError(sinkFailed)
      }
      try {
        return await fn()
      } catch (err) {
        // A refused artifact range is recoverable (CONTRACT_IPC.md §Artifact
        // Range Acknowledgement), as is a call an artifact writer refused
        // before sending anything: the script may catch it and emit on.
        if (!isRecoverable(err)) {
          sinkFailed = err
        }
        throw err
      }
    })
    // Chain continues regardless to maintain serialization
    chain = result.then(
      () => {},
      () => {}
    )
    return result
  }

  /**
   * Create an event envelope without seq (assigned after successful write).
   * Sequence numbers represent persisted order.
   */
  function createEnvelope<T extends EventType>(
    type: T,
    payload: PayloadMap[T]
  ): Readonly<Omit<EventEnvelope<T>, 'seq'>> {
    return {
      contract_version: CONTRACT_VERSION,
      event_id: randomUUID() as EventId,
      run_id: run.run_id,
      type,
      ts: new Date().toISOString(),
      payload,
      attempt: run.attempt,
      ...(run.job_id !== undefined && { job_id: run.job_id }),
      ...(run.parent_run_id !== undefined && { parent_run_id: run.parent_run_id })
    }
  }

  /**
   * Assign seq and write envelope. Seq represents persisted order.
   */
  async function writeEnvelope<T extends EventType>(
    envelope: Readonly<Omit<EventEnvelope<T>, 'seq'>>
  ): Promise<void> {
    seq += 1
    const complete: Readonly<EventEnvelope<T>> = { ...envelope, seq } as Readonly<EventEnvelope<T>>
    await sink.writeEvent(complete)
  }

  function assertNotTerminal(): void {
    if (terminalEmitted) {
      throw new TerminalEventError()
    }
  }

  /**
   * Check writer options and send the artifact's open frame.
   * Artifact bytes are written before the artifact event, which is the
   * commit record: bytes without an event are removed when the run ends.
   */
  async function openArtifact(options: EmitArtifactWriterOptions): Promise<OpenArtifact> {
    // Copy the caller's options, so a later change to them cannot make the
    // writer disagree with the open frame it sent.
    const declared: EmitArtifactWriterOptions = Object.freeze({
      name: options.name,
      content_type: options.content_type,
      size_bytes: options.size_bytes,
      ...(options.sha256 !== undefined && { sha256: options.sha256 })
    })
    validateWriterOptions(declared)
    const artifact = new OpenArtifact(randomUUID() as ArtifactId, declared)
    await sink.openArtifact(artifact.artifact_id, declared.size_bytes, declared.sha256)
    return artifact
  }

  /**
   * Check a write and send it as ranges, recording each range the runtime
   * commits. A refused range rejects with its offset, so the script knows
   * that the ranges before it are committed.
   */
  async function writeRanges(
    artifact: OpenArtifact,
    offset: number,
    data: Buffer | Uint8Array
  ): Promise<void> {
    artifact.checkWrite(offset, data.byteLength)
    for (const [at, range] of splitRanges(offset, data)) {
      try {
        await sink.writeArtifactRange(artifact.artifact_id, at, range)
      } catch (err) {
        if (err instanceof ArtifactRangeRefusedError) {
          throw new ArtifactRangeRefusedError(err.message, at, { cause: err })
        }
        throw err
      }
      artifact.wrote(at, range.byteLength)
    }
  }

  /** Check that the ranges cover the artifact and write its artifact event. */
  async function commitArtifact(artifact: OpenArtifact): Promise<ArtifactId> {
    artifact.checkCommit()
    await writeEnvelope(
      createEnvelope('artifact', {
        artifact_id: artifact.artifact_id,
        name: artifact.options.name,
        content_type: artifact.options.content_type,
        size_bytes: artifact.options.size_bytes
      })
    )
    artifact.commit()
    return artifact.artifact_id
  }

  /** Expose an open artifact as a writer whose calls share the emit chain. */
  function writerOf(artifact: OpenArtifact): ArtifactWriter {
    return {
      artifact_id: artifact.artifact_id,
      writeAt: (offset, data) =>
        serialize(async () => {
          assertNotTerminal()
          await writeRanges(artifact, offset, data)
        }),
      commit: () =>
        serialize(async () => {
          assertNotTerminal()
          return commitArtifact(artifact)
        })
    }
  }

  /**
   * Emit a non-terminal event: serialize → assert → envelope → write.
   */
  function emitEvent<T extends EventType>(type: T, payload: PayloadMap[T]): Promise<void> {
    return serialize(async () => {
      assertNotTerminal()
      const envelope = createEnvelope(type, payload)
      await writeEnvelope(envelope)
    })
  }

  const emit: EmitAPI = {
    item(options: EmitItemOptions): Promise<void> {
      return emitEvent('item', {
        item_type: options.item_type,
        data: options.data
      })
    },

    artifact(options: EmitArtifactOptions): Promise<ArtifactId> {
      return serialize(async () => {
        assertNotTerminal()
        const artifact = await openArtifact({
          name: options.name,
          content_type: options.content_type,
          size_bytes: options.data.byteLength
        })
        if (options.data.byteLength > 0) {
          await writeRanges(artifact, 0, options.data)
        }
        return commitArtifact(artifact)
      })
    },

    artifactWriter(options: EmitArtifactWriterOptions): Promise<ArtifactWriter> {
      return serialize(async () => {
        assertNotTerminal()
        return writerOf(await openArtifact(options))
      })
    },

    checkpoint(options: EmitCheckpointOptions): Promise<void> {
      return emitEvent('checkpoint', {
        checkpoint_id: options.checkpoint_id,
        ...(options.note !== undefined && { note: options.note })
      })
    },

    enqueue(options: EmitEnqueueOptions): Promise<void> {
      return emitEvent('enqueue', {
        target: options.target,
        params: options.params,
        ...(options.source !== undefined && { source: options.source }),
        ...(options.category !== undefined && { category: options.category })
      })
    },

    rotateProxy(options?: EmitRotateProxyOptions): Promise<void> {
      return emitEvent('rotate_proxy', {
        ...(options?.reason !== undefined && { reason: options.reason })
      })
    },

    log(options: EmitLogOptions): Promise<void> {
      return emitEvent('log', {
        level: options.level,
        message: options.message,
        ...(options.fields !== undefined && { fields: options.fields })
      })
    },

    async debug(message: string, fields?: Record<string, unknown>): Promise<void> {
      await emit.log({ level: 'debug', message, fields })
    },

    async info(message: string, fields?: Record<string, unknown>): Promise<void> {
      await emit.log({ level: 'info', message, fields })
    },

    async warn(message: string, fields?: Record<string, unknown>): Promise<void> {
      await emit.log({ level: 'warn', message, fields })
    },

    async error(message: string, fields?: Record<string, unknown>): Promise<void> {
      await emit.log({ level: 'error', message, fields })
    },

    runError(options: EmitRunErrorOptions): Promise<void> {
      return serialize(async () => {
        assertNotTerminal()
        const envelope = createEnvelope('run_error', {
          error_type: options.error_type,
          message: options.message,
          ...(options.stack !== undefined && { stack: options.stack })
        })
        await writeEnvelope(envelope)
        terminalEmitted = true
      })
    },

    runComplete(options?: EmitRunCompleteOptions): Promise<void> {
      return serialize(async () => {
        assertNotTerminal()
        const envelope = createEnvelope('run_complete', {
          ...(options?.summary !== undefined && { summary: options.summary })
        })
        await writeEnvelope(envelope)
        terminalEmitted = true
      })
    }
  }

  /**
   * Validate storage filename: no path separators, no "..".
   */
  function validateFilename(filename: string): void {
    if (!filename) {
      throw new StorageFilenameError(filename, 'filename must not be empty')
    }
    if (filename.includes('/') || filename.includes('\\')) {
      throw new StorageFilenameError(filename, 'filename must not contain path separators')
    }
    if (filename.includes('..')) {
      throw new StorageFilenameError(filename, 'filename must not contain ".."')
    }
  }

  const storage: StorageAPI = {
    put(options: StoragePutOptions): Promise<StoragePutResult> {
      return serialize(async () => {
        assertNotTerminal()
        validateFilename(options.filename)
        await sink.writeFile(options.filename, options.content_type, options.data)
        const key = storagePartition ? buildStorageKey(storagePartition, options.filename) : ''
        return { key }
      })
    }
  }

  return { emit, storage }
}

/**
 * Create an EmitAPI implementation backed by an EmitSink.
 *
 * Convenience wrapper around createAPIs for callers that only need EmitAPI.
 *
 * @internal
 */
export function createEmitAPI(run: RunMeta, sink: EmitSink): EmitAPI {
  return createAPIs(run, sink).emit
}
