import { PassThrough, Writable } from 'node:stream'
import { decode as msgpackDecode, encode as msgpackEncode } from '@msgpack/msgpack'
import type { ArtifactId, EventEnvelope, EventId, RunId } from '@pithecene/quarry-sdk'
import { beforeEach, describe, expect, it } from 'vitest'
import { AckReader } from '../../src/ipc/ack-reader.js'
import {
  type ArtifactChunkFrame,
  encodeFrame,
  type FileWriteFrame,
  LENGTH_PREFIX_SIZE
} from '../../src/ipc/frame.js'
import { StdioSink, StreamClosedError } from '../../src/ipc/sink.js'

/**
 * Helper to create a minimal valid event envelope for testing.
 */
function makeEnvelope(overrides: Partial<EventEnvelope<'item'>> = {}): EventEnvelope<'item'> {
  return {
    contract_version: '0.1.0',
    event_id: 'evt-123' as EventId,
    run_id: 'run-456' as RunId,
    seq: 1,
    type: 'item',
    ts: '2024-01-01T00:00:00.000Z',
    payload: { item_type: 'test', data: {} },
    attempt: 1,
    ...overrides
  }
}

/**
 * Helper to read a frame from collected buffer data.
 */
function readFrame(buffer: Buffer): { frame: unknown; remaining: Buffer } {
  if (buffer.length < LENGTH_PREFIX_SIZE) {
    throw new Error('Buffer too short for frame header')
  }
  const payloadLength = buffer.readUInt32BE(0)
  const totalLength = LENGTH_PREFIX_SIZE + payloadLength
  if (buffer.length < totalLength) {
    throw new Error('Buffer too short for frame payload')
  }

  const payload = buffer.subarray(LENGTH_PREFIX_SIZE, totalLength)
  const frame = msgpackDecode(payload)
  const remaining = buffer.subarray(totalLength)

  return { frame, remaining }
}

/**
 * Collect all data written to a stream into a single buffer.
 */
class BufferCollector extends Writable {
  private chunks: Buffer[] = []

  _write(chunk: Buffer, _encoding: string, callback: (error?: Error | null) => void): void {
    this.chunks.push(chunk)
    callback()
  }

  get buffer(): Buffer {
    return Buffer.concat(this.chunks)
  }

  get frames(): unknown[] {
    const frames: unknown[] = []
    let buf = this.buffer
    while (buf.length >= LENGTH_PREFIX_SIZE) {
      const { frame, remaining } = readFrame(buf)
      frames.push(frame)
      buf = remaining
    }
    return frames
  }
}

describe('StdioSink', () => {
  let collector: BufferCollector
  let sink: StdioSink

  beforeEach(() => {
    collector = new BufferCollector()
    sink = new StdioSink(collector)
  })

  describe('writeEvent', () => {
    it('writes event as framed message (raw envelope per CONTRACT_IPC)', async () => {
      const envelope = makeEnvelope()
      await sink.writeEvent(envelope)

      const frames = collector.frames
      expect(frames).toHaveLength(1)

      // Per CONTRACT_IPC, event frames contain the envelope directly (no wrapper)
      const decoded = frames[0] as EventEnvelope
      expect(decoded.type).toBe('item')
      expect(decoded.event_id).toBe(envelope.event_id)
      expect(decoded.run_id).toBe(envelope.run_id)
    })

    it('preserves event ordering', async () => {
      const envelopes = [
        makeEnvelope({ seq: 1, event_id: 'evt-1' as EventId }),
        makeEnvelope({ seq: 2, event_id: 'evt-2' as EventId }),
        makeEnvelope({ seq: 3, event_id: 'evt-3' as EventId })
      ]

      for (const envelope of envelopes) {
        await sink.writeEvent(envelope)
      }

      const frames = collector.frames as EventEnvelope[]
      expect(frames).toHaveLength(3)
      expect(frames[0].event_id).toBe('evt-1')
      expect(frames[1].event_id).toBe('evt-2')
      expect(frames[2].event_id).toBe('evt-3')
    })

    it('writes events of different types', async () => {
      const itemEnvelope: EventEnvelope<'item'> = {
        contract_version: '0.1.0',
        event_id: 'evt-item' as EventId,
        run_id: 'run-456' as RunId,
        seq: 1,
        type: 'item',
        ts: '2024-01-01T00:00:00.000Z',
        payload: { item_type: 'product', data: { name: 'Widget' } },
        attempt: 1
      }

      const logEnvelope: EventEnvelope<'log'> = {
        contract_version: '0.1.0',
        event_id: 'evt-log' as EventId,
        run_id: 'run-456' as RunId,
        seq: 2,
        type: 'log',
        ts: '2024-01-01T00:00:00.000Z',
        payload: { level: 'info', message: 'Processing...' },
        attempt: 1
      }

      await sink.writeEvent(itemEnvelope)
      await sink.writeEvent(logEnvelope)

      const frames = collector.frames as EventEnvelope[]
      expect(frames).toHaveLength(2)
      expect(frames[0].type).toBe('item')
      expect(frames[1].type).toBe('log')
    })
  })

  describe('writeArtifactData', () => {
    it('writes small artifact as single chunk', async () => {
      const artifactId = 'artifact-small' as ArtifactId
      const data = Buffer.from([1, 2, 3, 4, 5])

      await sink.writeArtifactData(artifactId, data)

      const frames = collector.frames as ArtifactChunkFrame[]
      expect(frames).toHaveLength(1)
      expect(frames[0].type).toBe('artifact_chunk')
      expect(frames[0].artifact_id).toBe('artifact-small')
      expect(frames[0].seq).toBe(1)
      expect(frames[0].is_last).toBe(true)
      expect(new Uint8Array(frames[0].data)).toEqual(new Uint8Array([1, 2, 3, 4, 5]))
    })

    it('writes empty artifact as single chunk', async () => {
      const artifactId = 'artifact-empty' as ArtifactId
      const data = Buffer.alloc(0)

      await sink.writeArtifactData(artifactId, data)

      const frames = collector.frames as ArtifactChunkFrame[]
      expect(frames).toHaveLength(1)
      expect(frames[0].is_last).toBe(true)
      expect(frames[0].data.length).toBe(0)
    })

    it('works with Uint8Array input', async () => {
      const artifactId = 'artifact-uint8' as ArtifactId
      const data = new Uint8Array([10, 20, 30])

      await sink.writeArtifactData(artifactId, data)

      const frames = collector.frames as ArtifactChunkFrame[]
      expect(frames).toHaveLength(1)
      expect(new Uint8Array(frames[0].data)).toEqual(data)
    })
  })

  describe('interleaved writes', () => {
    it('preserves order of events and artifact chunks', async () => {
      const artifactId = 'artifact-1' as ArtifactId

      await sink.writeEvent(makeEnvelope({ seq: 1 }))
      await sink.writeArtifactData(artifactId, Buffer.from([1, 2, 3]))
      await sink.writeEvent(makeEnvelope({ seq: 2 }))

      const frames = collector.frames
      expect(frames).toHaveLength(3)
      // Event envelopes have type like 'item', not 'event' (raw envelope per CONTRACT_IPC)
      expect((frames[0] as EventEnvelope).type).toBe('item')
      expect((frames[1] as ArtifactChunkFrame).type).toBe('artifact_chunk')
      expect((frames[2] as EventEnvelope).type).toBe('item')
    })
  })

  describe('error handling', () => {
    it('throws StreamClosedError on destroyed stream', async () => {
      const stream = new PassThrough()
      const sink = new StdioSink(stream)

      stream.destroy()

      await expect(sink.writeEvent(makeEnvelope())).rejects.toThrow(StreamClosedError)
    })

    it('throws StreamClosedError on ended stream', async () => {
      const stream = new PassThrough()
      const sink = new StdioSink(stream)

      stream.end()
      // Wait for end to propagate
      await new Promise((resolve) => stream.once('finish', resolve))

      await expect(sink.writeEvent(makeEnvelope())).rejects.toThrow(StreamClosedError)
    })

    it('propagates stream errors during backpressure', async () => {
      // Create a stream that will trigger backpressure
      const stream = new PassThrough({ highWaterMark: 1 })
      const sink = new StdioSink(stream)

      // Write large data to trigger backpressure
      const largeEnvelope = makeEnvelope({
        payload: { item_type: 'test', data: { padding: 'x'.repeat(1000) } }
      })

      // Don't read from stream - it will backpressure
      const writePromise = sink.writeEvent(largeEnvelope)

      // Destroy stream while write is blocked on backpressure
      await new Promise((resolve) => setTimeout(resolve, 10))
      stream.destroy(new Error('Test error'))

      await expect(writePromise).rejects.toThrow()
    })
  })

  describe('backpressure', () => {
    it('blocks on backpressure and resumes on drain', async () => {
      // Create a stream with very small highWaterMark to trigger backpressure
      const stream = new PassThrough({ highWaterMark: 16 })
      const sink = new StdioSink(stream)

      // Write enough data to fill buffer
      const largeEnvelope = makeEnvelope({
        payload: { item_type: 'test', data: { padding: 'x'.repeat(100) } }
      })

      // Start write (may block)
      const writePromise = sink.writeEvent(largeEnvelope)

      // Read data to trigger drain
      setImmediate(() => {
        stream.read()
        stream.resume()
      })

      // Write should eventually complete
      await expect(writePromise).resolves.toBeUndefined()
    })

    it('waits for drain on stream while writing via separate writeFn', async () => {
      // Verifies the split contract: events (drain) are listened on `stream`,
      // while actual bytes go through `writeFn`. This is the pattern the
      // stdout guard relies on to avoid the drain deadlock.
      const stream = new PassThrough({ highWaterMark: 16 })

      const writeCalls: Buffer[] = []
      const writeFn = (data: Buffer): boolean => {
        writeCalls.push(data)
        // Delegate to the real stream so drain fires on `stream`
        return stream.write(data)
      }

      const sink = new StdioSink(stream, writeFn)

      const largeEnvelope = makeEnvelope({
        payload: { item_type: 'test', data: { padding: 'x'.repeat(100) } }
      })

      const writePromise = sink.writeEvent(largeEnvelope)

      // Read data to trigger drain on stream
      setImmediate(() => {
        stream.read()
        stream.resume()
      })

      await expect(writePromise).resolves.toBeUndefined()

      // writeFn was called (not stream.write directly by StdioSink)
      expect(writeCalls).toHaveLength(1)
    })

    it('cleans up listeners if writeFn throws synchronously', async () => {
      const stream = new PassThrough({ highWaterMark: 16 })

      const writeFn = (_data: Buffer): boolean => {
        throw new Error('writeFn exploded')
      }

      const sink = new StdioSink(stream, writeFn)

      await expect(sink.writeEvent(makeEnvelope())).rejects.toThrow('writeFn exploded')

      // Listeners must be cleaned up — no accumulation on the stream
      expect(stream.listenerCount('error')).toBe(0)
      expect(stream.listenerCount('close')).toBe(0)
      expect(stream.listenerCount('finish')).toBe(0)
      expect(stream.listenerCount('drain')).toBe(0)
    })
  })
})

/** Encode a file_write_ack payload as a length-prefixed msgpack frame. */
function encodeAckFrame(writeId: number, ok: boolean, error?: string): Buffer {
  const payload = msgpackEncode({
    type: 'file_write_ack',
    write_id: writeId,
    ok,
    ...(error != null && { error })
  })
  return Buffer.from(encodeFrame(payload))
}

describe('StdioSink writeFile with AckReader', () => {
  it('blocks on ack and resolves on success', async () => {
    const output = new PassThrough()
    const ackStream = new PassThrough()
    const ackReader = new AckReader(ackStream)
    ackReader.start()
    const sink = new StdioSink(output, undefined, ackReader)

    // Capture written frames
    const chunks: Buffer[] = []
    output.on('data', (chunk: Buffer) => chunks.push(chunk))

    // Start writeFile — it will block waiting for ack
    const writePromise = sink.writeFile('test.png', 'image/png', Buffer.from('data'))

    // Let the frame be written
    await new Promise((r) => setTimeout(r, 10))

    // Verify frame was sent with write_id
    const buf = Buffer.concat(chunks)
    const payloadLen = buf.readUInt32BE(0)
    const decoded = msgpackDecode(buf.subarray(4, 4 + payloadLen)) as FileWriteFrame
    expect(decoded.write_id).toBe(1)

    // Send success ack
    ackStream.write(encodeAckFrame(1, true))

    await expect(writePromise).resolves.toBeUndefined()
    ackReader.stop()
  })

  it('rejects on error ack', async () => {
    const output = new PassThrough()
    const ackStream = new PassThrough()
    const ackReader = new AckReader(ackStream)
    ackReader.start()
    const sink = new StdioSink(output, undefined, ackReader)

    const writePromise = sink.writeFile('test.png', 'image/png', Buffer.from('data'))

    await new Promise((r) => setTimeout(r, 10))

    ackStream.write(encodeAckFrame(1, false, 'S3 PutObject failed'))

    await expect(writePromise).rejects.toThrow('S3 PutObject failed')
    ackReader.stop()
  })

  it('fire-and-forget without ack reader', async () => {
    const collector = new BufferCollector()
    const sink = new StdioSink(collector)

    // No ack reader — should resolve immediately after write
    await sink.writeFile('test.png', 'image/png', Buffer.from('data'))

    // Verify frame was sent with write_id 0 (default)
    const frames = collector.frames as FileWriteFrame[]
    expect(frames).toHaveLength(1)
    expect(frames[0].write_id).toBe(0)
  })

  it('increments write_id for each call', async () => {
    const output = new PassThrough()
    const ackStream = new PassThrough()
    const ackReader = new AckReader(ackStream)
    ackReader.start()
    const sink = new StdioSink(output, undefined, ackReader)

    const chunks: Buffer[] = []
    output.on('data', (chunk: Buffer) => chunks.push(chunk))

    // Start two writes
    const p1 = sink.writeFile('a.png', 'image/png', Buffer.from('a'))
    await new Promise((r) => setTimeout(r, 10))
    ackStream.write(encodeAckFrame(1, true))
    await p1

    const p2 = sink.writeFile('b.png', 'image/png', Buffer.from('b'))
    await new Promise((r) => setTimeout(r, 10))
    ackStream.write(encodeAckFrame(2, true))
    await p2

    // Verify write_ids
    const buf = Buffer.concat(chunks)
    let offset = 0

    const len1 = buf.readUInt32BE(offset)
    const frame1 = msgpackDecode(buf.subarray(offset + 4, offset + 4 + len1)) as FileWriteFrame
    expect(frame1.write_id).toBe(1)

    offset += 4 + len1
    const len2 = buf.readUInt32BE(offset)
    const frame2 = msgpackDecode(buf.subarray(offset + 4, offset + 4 + len2)) as FileWriteFrame
    expect(frame2.write_id).toBe(2)

    ackReader.stop()
  })

  it('falls back to fire-and-forget when AckReader detects no ack support (CONTRACT_IPC.md §Backward Compatibility)', async () => {
    const output = new PassThrough()
    const ackStream = new PassThrough()
    const ackReader = new AckReader(ackStream)
    ackReader.start()

    // Old runtime closes stdin immediately — trigger no-ack-support detection
    ackStream.end()
    await new Promise((r) => setTimeout(r, 10))

    const sink = new StdioSink(output, undefined, ackReader)

    // Collect written frames
    const chunks: Buffer[] = []
    output.on('data', (chunk: Buffer) => chunks.push(chunk))

    // writeFile should resolve without waiting for an ack
    await sink.writeFile('test.png', 'image/png', Buffer.from('data'))

    // Verify frame was written (write_id > 0 because ackReader was present)
    const buf = Buffer.concat(chunks)
    const payloadLen = buf.readUInt32BE(0)
    const decoded = msgpackDecode(buf.subarray(4, 4 + payloadLen)) as FileWriteFrame
    expect(decoded.write_id).toBe(1)
  })
})
