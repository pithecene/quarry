/**
 * AckReader: reads length-prefixed msgpack frames from stdin (phase 2).
 *
 * After the executor reads JSON metadata (phase 1), stdin remains open for
 * the runtime to send file_write_ack frames back. AckReader attaches to
 * stdin and matches incoming acks to pending promises by write_id.
 *
 * @module
 */
import type { Readable } from 'node:stream'
import { ArtifactRangeRefusedError } from '@pithecene/quarry-sdk'
import { decodeArtifactRangeAck, decodeFileWriteAck } from './frame.js'

/** Minimum frame size: 4-byte length prefix + at least 1 byte payload. */
const LENGTH_PREFIX_SIZE = 4

/** The two acknowledgements the runtime sends, each with its own ID space. */
type AckKind = 'file_write' | 'artifact_range'

type PendingAck = { resolve: () => void; reject: (err: Error) => void }

/**
 * AckReader reads file_write_ack and artifact_range_ack frames from stdin
 * and resolves pending promises.
 *
 * Lifecycle:
 * 1. Construct with a readable stream (stdin)
 * 2. Call start() to begin reading
 * 3. Call waitForAck(writeId) before sending a file_write frame, or
 *    waitForRangeAck(rangeId) before sending an artifact_range frame
 * 4. Call stop() after execution completes
 *
 * On EOF or error, all pending promises are rejected.
 */
export class AckReader {
  private readonly pending = new Map<string, PendingAck>()
  private buffer = Buffer.alloc(0)
  private stopped = false
  /** True when the runtime does not support ack frames (fire-and-forget fallback). */
  private noAckSupport = false
  /** True after at least one ack frame has been successfully dispatched. */
  private receivedAnyAck = false
  private readonly stream: Readable

  constructor(stream: Readable) {
    this.stream = stream
  }

  /**
   * Register a pending ack for the given writeId.
   * Returns a promise that resolves on success ack or rejects on error ack/EOF.
   */
  waitForAck(writeId: number): Promise<void> {
    // Old runtime closed stdin immediately → fire-and-forget fallback
    if (this.noAckSupport) {
      return Promise.resolve()
    }
    return this.register('file_write', writeId)
  }

  /**
   * Register a pending ack for the given rangeId. A range has no
   * fire-and-forget fallback (CONTRACT_IPC.md §Artifact Range
   * Acknowledgement): the promise rejects when stdin ends without its ack.
   */
  waitForRangeAck(rangeId: number): Promise<void> {
    if (this.noAckSupport) {
      return Promise.reject(new Error('runtime sent no artifact_range_ack before stdin closed'))
    }
    return this.register('artifact_range', rangeId)
  }

  private register(kind: AckKind, id: number): Promise<void> {
    if (this.stopped) {
      return Promise.reject(new Error('AckReader is stopped'))
    }
    return new Promise<void>((resolve, reject) => {
      this.pending.set(pendingKey(kind, id), { resolve, reject })
    })
  }

  /**
   * Returns true if the runtime does not support ack frames.
   * Detected when stdin EOF arrives without having received any ack frames.
   */
  get hasAckSupport(): boolean {
    return !this.noAckSupport
  }

  /**
   * Start reading frames from the stream.
   * Attaches data/end/error listeners.
   */
  start(): void {
    this.stream.on('data', this.onData)
    this.stream.on('end', this.onEnd)
    this.stream.on('error', this.onError)
  }

  /**
   * Stop the reader and reject all pending promises.
   */
  stop(): void {
    if (this.stopped) return
    this.stopped = true
    this.stream.removeListener('data', this.onData)
    this.stream.removeListener('end', this.onEnd)
    this.stream.removeListener('error', this.onError)
    this.rejectAll(new Error('AckReader stopped'))
  }

  /**
   * Returns true if there are no pending ack promises.
   */
  get idle(): boolean {
    return this.pending.size === 0
  }

  private readonly onData = (chunk: Buffer): void => {
    this.buffer = Buffer.concat([this.buffer, chunk])
    this.drainBuffer()
  }

  private readonly onEnd = (): void => {
    this.stopped = true
    if (!this.receivedAnyAck) {
      // No ack was ever received → runtime does not support ack frames.
      // Resolve pending file writes (fire-and-forget fallback) per
      // CONTRACT_IPC.md §Backward Compatibility, and reject pending ranges,
      // which have no such fallback. This is deterministic: regardless of
      // whether waitForAck() was called before or after EOF, the outcome is
      // the same.
      this.noAckSupport = true
      this.settleAll(new Error('runtime sent no artifact_range_ack before stdin closed'))
      return
    }
    // Runtime supports acks but closed stdin with pending writes → real failure.
    this.rejectAll(new Error('stdin closed (EOF)'))
  }

  private readonly onError = (err: Error): void => {
    this.stopped = true
    this.rejectAll(new Error(`stdin error: ${err.message}`))
  }

  /** Consume complete frames from the internal buffer. */
  private drainBuffer(): void {
    while (this.buffer.length >= LENGTH_PREFIX_SIZE) {
      const payloadLen = this.buffer.readUInt32BE(0)
      const totalLen = LENGTH_PREFIX_SIZE + payloadLen

      if (this.buffer.length < totalLen) {
        // Incomplete frame — wait for more data
        return
      }

      const payload = this.buffer.subarray(LENGTH_PREFIX_SIZE, totalLen)
      this.buffer = this.buffer.subarray(totalLen)

      this.processPayload(payload)
    }
  }

  /** Decode and dispatch a single ack frame. */
  private processPayload(payload: Uint8Array): void {
    const ack = decodeAck(payload)
    if (ack === undefined) {
      // Unknown or malformed frame — ignore (future frame types may appear)
      return
    }

    const key = pendingKey(ack.kind, ack.id)
    const entry = this.pending.get(key)
    if (!entry) {
      // No pending promise for this ID — stale or duplicate ack
      return
    }

    this.pending.delete(key)
    this.receivedAnyAck = true

    if (ack.ok) {
      entry.resolve()
    } else if (ack.kind === 'artifact_range') {
      // A refused range is recoverable (CONTRACT_IPC.md §Artifact Range
      // Acknowledgement): the error names it so the sinks do not latch it.
      entry.reject(new ArtifactRangeRefusedError(ack.error ?? 'artifact range refused'))
    } else {
      entry.reject(new Error(ack.error ?? 'file write failed'))
    }
  }

  /**
   * Settle all pending promises when the runtime sent no ack at all:
   * file writes resolve (fire-and-forget fallback), and ranges reject.
   */
  private settleAll(rangeError: Error): void {
    for (const [key, entry] of this.pending) {
      if (key.startsWith('artifact_range:')) {
        entry.reject(rangeError)
      } else {
        entry.resolve()
      }
    }
    this.pending.clear()
  }

  /** Reject all pending promises with the given error. */
  private rejectAll(err: Error): void {
    for (const [, entry] of this.pending) {
      entry.reject(err)
    }
    this.pending.clear()
  }
}

/** One decoded acknowledgement of either kind. */
type Ack = {
  readonly kind: AckKind
  readonly id: number
  readonly ok: boolean
  readonly error?: string
}

/** Decode an ack frame of either kind, or undefined for any other frame. */
function decodeAck(payload: Uint8Array): Ack | undefined {
  try {
    const ack = decodeFileWriteAck(payload)
    return {
      kind: 'file_write',
      id: ack.write_id,
      ok: ack.ok,
      ...(ack.error !== undefined && { error: ack.error })
    }
  } catch {
    // Not a file_write_ack; try the other kind.
  }
  try {
    const ack = decodeArtifactRangeAck(payload)
    return {
      kind: 'artifact_range',
      id: ack.range_id,
      ok: ack.ok,
      ...(ack.error !== undefined && { error: ack.error })
    }
  } catch {
    return undefined
  }
}

function pendingKey(kind: AckKind, id: number): string {
  return `${kind}:${id}`
}
