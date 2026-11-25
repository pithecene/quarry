import { PassThrough } from 'node:stream'
import { encode as msgpackEncode } from '@msgpack/msgpack'
import { describe, expect, it } from 'vitest'
import { AckReader } from '../../src/ipc/ack-reader.js'
import { encodeFrame } from '../../src/ipc/frame.js'

/** Encode a file_write_ack payload as a length-prefixed msgpack frame. */
function encodeAck(writeId: number, ok: boolean, error?: string): Buffer {
  const payload = msgpackEncode({
    type: 'file_write_ack',
    write_id: writeId,
    ok,
    ...(error != null && { error })
  })
  return Buffer.from(encodeFrame(payload))
}

describe('AckReader', () => {
  it('resolves on success ack', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()

    const promise = reader.waitForAck(1)
    stream.write(encodeAck(1, true))

    await expect(promise).resolves.toBeUndefined()
    reader.stop()
  })

  it('rejects on error ack', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()

    const promise = reader.waitForAck(1)
    stream.write(encodeAck(1, false, 'S3 PutObject failed'))

    await expect(promise).rejects.toThrow('S3 PutObject failed')
    reader.stop()
  })

  it('resolves all pending on EOF when no ack was ever received (no-ack-support)', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()

    const p1 = reader.waitForAck(1)
    const p2 = reader.waitForAck(2)

    stream.end()

    // No ack was ever received → fire-and-forget fallback
    await expect(p1).resolves.toBeUndefined()
    await expect(p2).resolves.toBeUndefined()
  })

  it('rejects all pending on EOF when acks were previously received', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()

    // Receive one ack — runtime supports acks
    const p0 = reader.waitForAck(1)
    stream.write(encodeAck(1, true))
    await expect(p0).resolves.toBeUndefined()

    // Now two pending with no acks coming
    const p1 = reader.waitForAck(2)
    const p2 = reader.waitForAck(3)

    stream.end()

    await expect(p1).rejects.toThrow('stdin closed (EOF)')
    await expect(p2).rejects.toThrow('stdin closed (EOF)')
  })

  it('handles partial frame buffering', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()

    const promise = reader.waitForAck(1)
    const fullFrame = encodeAck(1, true)

    // Write first half, then second half
    const mid = Math.floor(fullFrame.length / 2)
    stream.write(fullFrame.subarray(0, mid))

    // Small delay to let the first chunk be processed
    await new Promise((r) => setTimeout(r, 10))

    stream.write(fullFrame.subarray(mid))

    await expect(promise).resolves.toBeUndefined()
    reader.stop()
  })

  it('stop() rejects pending promises', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()

    const promise = reader.waitForAck(1)
    reader.stop()

    await expect(promise).rejects.toThrow('AckReader stopped')
  })

  it('ignores unknown frame types', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()

    const promise = reader.waitForAck(1)

    // Send an unknown frame type
    const unknownPayload = msgpackEncode({ type: 'unknown_frame', data: 'hello' })
    stream.write(Buffer.from(encodeFrame(unknownPayload)))

    // Then send the real ack
    stream.write(encodeAck(1, true))

    await expect(promise).resolves.toBeUndefined()
    reader.stop()
  })

  it('handles multiple acks in order', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()

    const p1 = reader.waitForAck(1)
    const p2 = reader.waitForAck(2)
    const p3 = reader.waitForAck(3)

    // Send all three acks
    stream.write(encodeAck(1, true))
    stream.write(encodeAck(2, false, 'disk full'))
    stream.write(encodeAck(3, true))

    await expect(p1).resolves.toBeUndefined()
    await expect(p2).rejects.toThrow('disk full')
    await expect(p3).resolves.toBeUndefined()
    reader.stop()
  })

  it('rejects waitForAck after stop', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()
    reader.stop()

    await expect(reader.waitForAck(1)).rejects.toThrow('AckReader is stopped')
  })

  it('reports idle when no pending acks', () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    expect(reader.idle).toBe(true)
  })

  it('reports non-idle when pending acks', () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()
    reader.waitForAck(1).catch(() => {
      /* expected rejection on cleanup */
    })
    expect(reader.idle).toBe(false)
    reader.stop()
  })

  describe('backward compatibility (CONTRACT_IPC.md §Backward Compatibility)', () => {
    it('EOF before any waitForAck enables fire-and-forget fallback', async () => {
      const stream = new PassThrough()
      const reader = new AckReader(stream)
      reader.start()

      // Old runtime closes stdin immediately after metadata — no acks
      stream.end()

      // Let EOF propagate
      await new Promise((r) => setTimeout(r, 10))

      // Reader should detect no-ack-support, not reject
      expect(reader.hasAckSupport).toBe(false)

      // Subsequent waitForAck calls should resolve immediately (fire-and-forget)
      await expect(reader.waitForAck(1)).resolves.toBeUndefined()
      await expect(reader.waitForAck(2)).resolves.toBeUndefined()
    })

    it('EOF races with waitForAck — resolves when no ack ever received (same tick)', async () => {
      // This is the critical race path: waitForAck() is called before the
      // EOF event fires. Because no ack was ever received, the pending
      // promise must resolve (fire-and-forget), not reject.
      const stream = new PassThrough()
      const reader = new AckReader(stream)
      reader.start()

      // End stream AND register pending ack on the same tick
      stream.end()
      const p1 = reader.waitForAck(1)

      // Must resolve (fire-and-forget), not reject
      await expect(p1).resolves.toBeUndefined()
      expect(reader.hasAckSupport).toBe(false)
    })

    it('EOF with pending rejects when acks were previously received (real failure)', async () => {
      const stream = new PassThrough()
      const reader = new AckReader(stream)
      reader.start()

      // First write succeeds — runtime supports acks
      const p1 = reader.waitForAck(1)
      stream.write(encodeAck(1, true))
      await expect(p1).resolves.toBeUndefined()

      // Second write is pending when EOF arrives — real failure
      const p2 = reader.waitForAck(2)
      stream.end()

      await expect(p2).rejects.toThrow('stdin closed (EOF)')
      expect(reader.hasAckSupport).toBe(true)
    })

    it('hasAckSupport is true before EOF', () => {
      const stream = new PassThrough()
      const reader = new AckReader(stream)
      reader.start()

      expect(reader.hasAckSupport).toBe(true)
    })
  })
})

/** Encode an artifact_range_ack payload as a length-prefixed msgpack frame. */
function encodeRangeAck(rangeId: number, ok: boolean, error?: string): Buffer {
  const payload = msgpackEncode({
    type: 'artifact_range_ack',
    range_id: rangeId,
    ok,
    ...(error != null && { error })
  })
  return Buffer.from(encodeFrame(payload))
}

describe('AckReader artifact range acks (CONTRACT_IPC.md §Artifact Range Acknowledgement)', () => {
  it('resolves a range on its ack and keeps file write ids apart', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()

    const file = reader.waitForAck(1)
    const range = reader.waitForRangeAck(1)
    stream.write(encodeRangeAck(1, true))
    await expect(range).resolves.toBeUndefined()
    expect(reader.idle).toBe(false)
    stream.write(encodeAck(1, true))
    await expect(file).resolves.toBeUndefined()
    reader.stop()
  })

  it('rejects a range on a refusal with the runtime error', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()

    const range = reader.waitForRangeAck(2)
    stream.write(encodeRangeAck(2, false, 'artifact range refused: range past size'))
    await expect(range).rejects.toThrow('range past size')
    reader.stop()
  })

  it('rejects a pending range on EOF without any ack, while file writes fall back', async () => {
    const stream = new PassThrough()
    const reader = new AckReader(stream)
    reader.start()

    const file = reader.waitForAck(1)
    const range = reader.waitForRangeAck(1)
    stream.end()
    await expect(file).resolves.toBeUndefined()
    await expect(range).rejects.toThrow('no artifact_range_ack')
    await expect(reader.waitForRangeAck(2)).rejects.toThrow('no artifact_range_ack')
  })
})
