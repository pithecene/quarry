/**
 * StdioSink: EmitSink implementation that writes frames to a stream.
 *
 * Per CONTRACT_IPC.md:
 * - Emit calls must block on backpressure
 * - Executor must not buffer unboundedly
 * - Executor must not drop frames
 *
 * @remarks
 * **Single-writer assumption**: This sink is NOT thread-safe. Concurrent
 * calls to writeEvent/writeArtifactData from multiple async contexts will
 * interleave frames. The SDK's EmitAPI serializes calls via promise chain,
 * so this is safe when used through createEmitAPI. Direct usage must
 * serialize externally.
 *
 * @module
 */
import type { Writable } from 'node:stream'
import type { ArtifactId, EmitSink, EventEnvelope } from '@pithecene/quarry-sdk'
import type { AckReader } from './ack-reader.js'
import {
  encodeArtifactChunks,
  encodeEventFrame,
  encodeFileWriteFrame,
  encodeRunResultFrame,
  type ProxyEndpointRedactedFrame,
  type RunResultOutcome
} from './frame.js'

/**
 * Error thrown when the output stream is closed or finished unexpectedly.
 */
export class StreamClosedError extends Error {
  constructor(reason: 'destroyed' | 'ended' | 'close' | 'finish') {
    super(`Output stream unavailable: ${reason}`)
    this.name = 'StreamClosedError'
  }
}

/**
 * Write a buffer to a stream with backpressure handling.
 * Resolves only from a single code path to avoid double-resolution.
 *
 * @param stream - The writable stream (used for state checks and event listening)
 * @param data - The data to write
 * @param writeFn - Function that performs the actual write, returning false on backpressure.
 *   Separated from `stream` so callers can bypass a patched `stream.write`
 *   (e.g. the stdout guard) while still listening for drain events on the real stream.
 * @returns Promise that resolves when data is accepted by the stream
 * @throws StreamClosedError if the stream is closed/finished
 * @throws Error if the stream emits an error
 */
function writeWithBackpressure(
  stream: Writable,
  data: Buffer,
  writeFn: (data: Buffer) => boolean
): Promise<void> {
  return new Promise((resolve, reject) => {
    // Pre-check stream state
    if (stream.destroyed) {
      reject(new StreamClosedError('destroyed'))
      return
    }
    if (stream.writableEnded || stream.writableFinished) {
      reject(new StreamClosedError('ended'))
      return
    }

    let settled = false

    const settle = (fn: () => void) => {
      if (settled) return
      settled = true
      cleanup()
      fn()
    }

    const onError = (err: Error) => settle(() => reject(err))
    const onClose = () => settle(() => reject(new StreamClosedError('close')))
    const onFinish = () => settle(() => reject(new StreamClosedError('finish')))
    const onDrain = () => settle(() => resolve())

    const cleanup = () => {
      stream.off('error', onError)
      stream.off('close', onClose)
      stream.off('finish', onFinish)
      stream.off('drain', onDrain)
    }

    // Attach listeners before write to catch synchronous errors
    stream.on('error', onError)
    stream.on('close', onClose)
    stream.on('finish', onFinish)

    let canContinue: boolean
    try {
      canContinue = writeFn(data)
    } catch (err) {
      // writeFn threw synchronously — clean up listeners to prevent leaks.
      // The Promise constructor would catch this, but cleanup() would never
      // run because settle() is only called by event handlers.
      cleanup()
      reject(err instanceof Error ? err : new Error(String(err)))
      return
    }

    if (canContinue) {
      // Buffer accepted, resolve on next tick to ensure
      // any synchronous error from write() is caught first
      setImmediate(() => settle(() => resolve()))
    } else {
      // Backpressure: wait for drain
      stream.on('drain', onDrain)
    }
  })
}

/**
 * Drain process.stdout, ensuring all buffered data reaches the OS pipe.
 *
 * `stream.end()` flushes buffered data and invokes the callback once the OS
 * has accepted everything. Without this, `process.exit()` can discard data
 * still sitting in Node's internal buffer — causing the runtime to see EOF
 * without a terminal event and classify the run as `executor_crash`.
 */
export function drainStdout(): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (process.stdout.writableFinished) {
      resolve()
      return
    }
    const onError = (err: Error): void => {
      cleanup()
      reject(err)
    }
    const cleanup = (): void => {
      process.stdout.off('error', onError)
    }
    process.stdout.on('error', onError)
    process.stdout.end(() => {
      cleanup()
      resolve()
    })
  })
}

/**
 * EmitSink implementation that writes frames to a writable stream.
 *
 * Conforms to CONTRACT_IPC.md:
 * - Frames are length-prefixed msgpack
 * - Artifact data is chunked (max 8 MiB per chunk)
 * - Writes block on backpressure
 */
export class StdioSink implements EmitSink {
  private readonly writeFn: (data: Buffer) => boolean
  private readonly ackReader: AckReader | undefined
  private writeIdCounter = 0

  /**
   * @param output - The writable stream (used for state checks and event listening)
   * @param writeFn - Optional function for actual writes. When omitted, defaults to
   *   `output.write()`. When provided, allows the caller to bypass a patched
   *   `output.write` (e.g. the stdout guard) while still using `output` for
   *   backpressure events and stream state.
   * @param ackReader - Optional AckReader for file_write_ack correlation.
   *   When provided, writeFile blocks until the runtime sends an ack.
   *   When omitted, writeFile is fire-and-forget (backward compat).
   */
  constructor(
    private readonly output: Writable,
    writeFn?: (data: Buffer) => boolean,
    ackReader?: AckReader
  ) {
    this.writeFn = writeFn ?? ((data) => output.write(data))
    this.ackReader = ackReader
  }

  /**
   * Write an event envelope as a framed message.
   * Blocks on backpressure per CONTRACT_IPC.md.
   */
  async writeEvent(envelope: EventEnvelope): Promise<void> {
    const frame = encodeEventFrame(envelope)
    await writeWithBackpressure(this.output, frame, this.writeFn)
  }

  /**
   * Write artifact binary data as chunked frames.
   * Per CONTRACT_IPC.md, bytes are written BEFORE the artifact event.
   * Blocks on backpressure per CONTRACT_IPC.md.
   */
  async writeArtifactData(artifactId: ArtifactId, data: Buffer | Uint8Array): Promise<void> {
    for (const frame of encodeArtifactChunks(artifactId, data)) {
      await writeWithBackpressure(this.output, frame, this.writeFn)
    }
  }

  /**
   * Write a run result control frame.
   * Per CONTRACT_IPC.md, this is a control frame emitted once after terminal event.
   * It does NOT affect seq ordering.
   *
   * @param outcome - The run outcome
   * @param proxyUsed - Optional redacted proxy endpoint (no password)
   */
  async writeRunResult(
    outcome: RunResultOutcome,
    proxyUsed?: ProxyEndpointRedactedFrame
  ): Promise<void> {
    const frame = encodeRunResultFrame(outcome, proxyUsed)
    await writeWithBackpressure(this.output, frame, this.writeFn)
  }

  /**
   * Write a sidecar file via file_write frame.
   * Bypasses seq numbering and the policy pipeline.
   * Blocks on backpressure per CONTRACT_IPC.md.
   *
   * When an AckReader is configured, this method blocks until the runtime
   * sends a file_write_ack. On error ack, the returned promise rejects.
   * Without an AckReader, this is fire-and-forget (backward compat).
   *
   * @param filename - Target filename (no path separators, no "..")
   * @param contentType - MIME content type
   * @param data - Raw binary data (max 8 MiB)
   */
  async writeFile(filename: string, contentType: string, data: Buffer | Uint8Array): Promise<void> {
    if (this.ackReader) {
      const writeId = ++this.writeIdCounter
      const ackPromise = this.ackReader.waitForAck(writeId)
      const frame = encodeFileWriteFrame(filename, contentType, data, writeId)
      await writeWithBackpressure(this.output, frame, this.writeFn)
      await ackPromise
    } else {
      const frame = encodeFileWriteFrame(filename, contentType, data)
      await writeWithBackpressure(this.output, frame, this.writeFn)
    }
  }
}
