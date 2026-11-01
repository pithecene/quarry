import { randomUUID } from 'node:crypto'
import type {
  EmitAPI,
  EmitArtifactOptions,
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
import { ArtifactRangeRefusedError } from './emit'
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
        // Range Acknowledgement): the script may catch it and emit on.
        if (!(err instanceof ArtifactRangeRefusedError)) {
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
        const artifact_id = randomUUID() as ArtifactId
        const size_bytes = options.data.byteLength

        // Artifact bytes may be written before the artifact event.
        // The artifact event is the commit record.
        //
        // Error cases:
        // - Bytes fail → no event emitted, no orphan.
        // - Event fails after bytes → orphaned blob, eligible for GC.
        await sink.writeArtifactData(artifact_id, options.data)

        const envelope = createEnvelope('artifact', {
          artifact_id,
          name: options.name,
          content_type: options.content_type,
          size_bytes
        })
        await writeEnvelope(envelope)

        return artifact_id
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
