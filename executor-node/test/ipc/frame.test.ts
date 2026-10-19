import { decode as msgpackDecode, encode as msgpackEncode } from '@msgpack/msgpack'
import type { ArtifactId, EventEnvelope, EventId, JobId, RunId } from '@pithecene/quarry-sdk'
import { describe, expect, it } from 'vitest'
import {
  type ArtifactChunkFrame,
  ChunkValidationError,
  calculateChunks,
  decodeFileWriteAck,
  encodeArtifactChunkFrame,
  encodeArtifactChunks,
  encodeEventFrame,
  encodeFileWriteFrame,
  encodeFrame,
  encodeRunResultFrame,
  type FileWriteAckFrame,
  type FileWriteFrame,
  FrameSizeError,
  LENGTH_PREFIX_SIZE,
  MAX_CHUNK_SIZE,
  MAX_FRAME_SIZE,
  MAX_PAYLOAD_SIZE,
  type RunResultFrame,
  type RunResultOutcome
} from '../../src/ipc/frame.js'

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

describe('IPC Frame Constants', () => {
  it('MAX_FRAME_SIZE is 16 MiB', () => {
    expect(MAX_FRAME_SIZE).toBe(16 * 1024 * 1024)
  })

  it('MAX_PAYLOAD_SIZE is MAX_FRAME_SIZE - LENGTH_PREFIX_SIZE', () => {
    expect(MAX_PAYLOAD_SIZE).toBe(MAX_FRAME_SIZE - LENGTH_PREFIX_SIZE)
  })

  it('MAX_CHUNK_SIZE is 8 MiB', () => {
    expect(MAX_CHUNK_SIZE).toBe(8 * 1024 * 1024)
  })

  it('LENGTH_PREFIX_SIZE is 4 bytes', () => {
    expect(LENGTH_PREFIX_SIZE).toBe(4)
  })
})

describe('encodeFrame', () => {
  it('encodes empty payload correctly', () => {
    const payload = new Uint8Array(0)
    const frame = encodeFrame(payload)

    expect(frame.length).toBe(LENGTH_PREFIX_SIZE)
    expect(frame.readUInt32BE(0)).toBe(0)
  })

  it('encodes payload with correct length prefix', () => {
    const payload = new Uint8Array([1, 2, 3, 4, 5])
    const frame = encodeFrame(payload)

    expect(frame.length).toBe(LENGTH_PREFIX_SIZE + 5)
    expect(frame.readUInt32BE(0)).toBe(5)
    expect(frame.subarray(LENGTH_PREFIX_SIZE)).toEqual(Buffer.from([1, 2, 3, 4, 5]))
  })

  it('handles large payloads up to MAX_PAYLOAD_SIZE', () => {
    // Test with a reasonably large payload (1 MiB)
    const payload = new Uint8Array(1024 * 1024)
    const frame = encodeFrame(payload)

    expect(frame.length).toBe(LENGTH_PREFIX_SIZE + payload.length)
    expect(frame.readUInt32BE(0)).toBe(payload.length)
  })

  it('throws FrameSizeError when payload exceeds MAX_PAYLOAD_SIZE', () => {
    const payload = new Uint8Array(MAX_PAYLOAD_SIZE + 1)

    expect(() => encodeFrame(payload)).toThrow(FrameSizeError)
    expect(() => encodeFrame(payload)).toThrow(/exceeds maximum/)
  })

  it('FrameSizeError contains size information', () => {
    const payload = new Uint8Array(MAX_PAYLOAD_SIZE + 100)

    try {
      encodeFrame(payload)
      expect.fail('Expected FrameSizeError')
    } catch (err) {
      expect(err).toBeInstanceOf(FrameSizeError)
      const frameErr = err as FrameSizeError
      expect(frameErr.payloadSize).toBe(MAX_PAYLOAD_SIZE + 100)
      expect(frameErr.maxPayloadSize).toBe(MAX_PAYLOAD_SIZE)
    }
  })
})

describe('encodeEventFrame', () => {
  it('encodes event envelope directly (no wrapper)', () => {
    const envelope = makeEnvelope()
    const frame = encodeEventFrame(envelope)

    // Decode the frame - should be raw envelope per CONTRACT_IPC
    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as EventEnvelope

    // Envelope is encoded directly, not wrapped
    expect(decoded.type).toBe('item')
    expect(decoded.event_id).toBe(envelope.event_id)
    expect(decoded.run_id).toBe(envelope.run_id)
    expect(decoded.seq).toBe(envelope.seq)
  })

  it('preserves all envelope fields', () => {
    const envelope = makeEnvelope({
      job_id: 'job-789' as JobId,
      parent_run_id: 'parent-run' as RunId,
      attempt: 3
    })
    const frame = encodeEventFrame(envelope)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as EventEnvelope

    expect(decoded.job_id).toBe('job-789')
    expect(decoded.parent_run_id).toBe('parent-run')
    expect(decoded.attempt).toBe(3)
  })
})

describe('encodeArtifactChunkFrame', () => {
  it('encodes chunk with correct structure', () => {
    const artifactId = 'artifact-123' as ArtifactId
    const data = new Uint8Array([10, 20, 30])
    const frame = encodeArtifactChunkFrame(artifactId, 1, false, data)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as ArtifactChunkFrame

    expect(decoded.type).toBe('artifact_chunk')
    expect(decoded.artifact_id).toBe('artifact-123')
    expect(decoded.seq).toBe(1)
    expect(decoded.is_last).toBe(false)
    expect(new Uint8Array(decoded.data)).toEqual(data)
  })

  it('encodes is_last=true correctly', () => {
    const artifactId = 'artifact-123' as ArtifactId
    const data = new Uint8Array([1, 2, 3])
    const frame = encodeArtifactChunkFrame(artifactId, 5, true, data)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as ArtifactChunkFrame

    expect(decoded.seq).toBe(5)
    expect(decoded.is_last).toBe(true)
  })

  it('encodes binary data as msgpack bin type', () => {
    const artifactId = 'artifact-123' as ArtifactId
    const data = new Uint8Array(256)
    for (let i = 0; i < 256; i++) {
      data[i] = i
    }
    const frame = encodeArtifactChunkFrame(artifactId, 1, true, data)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as ArtifactChunkFrame

    // Verify data is decoded as Uint8Array (bin type)
    expect(decoded.data).toBeInstanceOf(Uint8Array)
    expect(decoded.data.length).toBe(256)
  })

  describe('CONTRACT_IPC validation', () => {
    it('throws ChunkValidationError when seq < 1', () => {
      const artifactId = 'artifact-123' as ArtifactId
      const data = new Uint8Array([1, 2, 3])

      expect(() => encodeArtifactChunkFrame(artifactId, 0, false, data)).toThrow(
        ChunkValidationError
      )
      expect(() => encodeArtifactChunkFrame(artifactId, 0, false, data)).toThrow('seq must be >= 1')

      expect(() => encodeArtifactChunkFrame(artifactId, -1, false, data)).toThrow(
        ChunkValidationError
      )
    })

    it('throws ChunkValidationError when data exceeds MAX_CHUNK_SIZE', () => {
      const artifactId = 'artifact-123' as ArtifactId
      const data = new Uint8Array(MAX_CHUNK_SIZE + 1)

      expect(() => encodeArtifactChunkFrame(artifactId, 1, false, data)).toThrow(
        ChunkValidationError
      )
      expect(() => encodeArtifactChunkFrame(artifactId, 1, false, data)).toThrow(
        'exceeds MAX_CHUNK_SIZE'
      )
    })

    it('accepts data exactly at MAX_CHUNK_SIZE', () => {
      const artifactId = 'artifact-123' as ArtifactId
      const data = new Uint8Array(MAX_CHUNK_SIZE)

      // Should not throw
      expect(() => encodeArtifactChunkFrame(artifactId, 1, true, data)).not.toThrow()
    })

    it('accepts seq = 1 (minimum valid)', () => {
      const artifactId = 'artifact-123' as ArtifactId
      const data = new Uint8Array([1, 2, 3])

      // Should not throw
      expect(() => encodeArtifactChunkFrame(artifactId, 1, true, data)).not.toThrow()
    })
  })
})

describe('calculateChunks', () => {
  it('returns single chunk for empty data', () => {
    const chunks = calculateChunks(0)

    expect(chunks).toHaveLength(1)
    expect(chunks[0]).toEqual({
      seq: 1,
      isLast: true,
      offset: 0,
      length: 0
    })
  })

  it('returns single chunk for data smaller than MAX_CHUNK_SIZE', () => {
    const chunks = calculateChunks(1000)

    expect(chunks).toHaveLength(1)
    expect(chunks[0]).toEqual({
      seq: 1,
      isLast: true,
      offset: 0,
      length: 1000
    })
  })

  it('returns single chunk for data exactly MAX_CHUNK_SIZE', () => {
    const chunks = calculateChunks(MAX_CHUNK_SIZE)

    expect(chunks).toHaveLength(1)
    expect(chunks[0]).toEqual({
      seq: 1,
      isLast: true,
      offset: 0,
      length: MAX_CHUNK_SIZE
    })
  })

  it('returns multiple chunks for data larger than MAX_CHUNK_SIZE', () => {
    const totalSize = MAX_CHUNK_SIZE + 1000
    const chunks = calculateChunks(totalSize)

    expect(chunks).toHaveLength(2)
    expect(chunks[0]).toEqual({
      seq: 1,
      isLast: false,
      offset: 0,
      length: MAX_CHUNK_SIZE
    })
    expect(chunks[1]).toEqual({
      seq: 2,
      isLast: true,
      offset: MAX_CHUNK_SIZE,
      length: 1000
    })
  })

  it('handles exact multiple of MAX_CHUNK_SIZE', () => {
    const totalSize = MAX_CHUNK_SIZE * 3
    const chunks = calculateChunks(totalSize)

    expect(chunks).toHaveLength(3)
    expect(chunks.every((c, i) => c.seq === i + 1)).toBe(true)
    expect(chunks[0].isLast).toBe(false)
    expect(chunks[1].isLast).toBe(false)
    expect(chunks[2].isLast).toBe(true)
  })

  it('seq numbers are monotonically increasing from 1', () => {
    const chunks = calculateChunks(MAX_CHUNK_SIZE * 5 + 1)

    expect(chunks).toHaveLength(6)
    for (let i = 0; i < chunks.length; i++) {
      expect(chunks[i].seq).toBe(i + 1)
    }
  })

  it('only last chunk has isLast=true', () => {
    const chunks = calculateChunks(MAX_CHUNK_SIZE * 4)

    for (let i = 0; i < chunks.length - 1; i++) {
      expect(chunks[i].isLast).toBe(false)
    }
    expect(chunks[chunks.length - 1].isLast).toBe(true)
  })
})

describe('encodeArtifactChunks', () => {
  it('yields single frame for small data', () => {
    const artifactId = 'artifact-123' as ArtifactId
    const data = new Uint8Array([1, 2, 3, 4, 5])

    const frames = [...encodeArtifactChunks(artifactId, data)]

    expect(frames).toHaveLength(1)

    // Decode and verify
    const payloadLength = frames[0].readUInt32BE(0)
    const payload = frames[0].subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as ArtifactChunkFrame

    expect(decoded.artifact_id).toBe('artifact-123')
    expect(decoded.seq).toBe(1)
    expect(decoded.is_last).toBe(true)
    expect(new Uint8Array(decoded.data)).toEqual(data)
  })

  it('yields single frame for empty data', () => {
    const artifactId = 'artifact-empty' as ArtifactId
    const data = new Uint8Array(0)

    const frames = [...encodeArtifactChunks(artifactId, data)]

    expect(frames).toHaveLength(1)

    const payloadLength = frames[0].readUInt32BE(0)
    const payload = frames[0].subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as ArtifactChunkFrame

    expect(decoded.is_last).toBe(true)
    expect(decoded.data.length).toBe(0)
  })

  it('yields multiple frames for large data', () => {
    const artifactId = 'artifact-large' as ArtifactId
    // Create data that spans exactly 2 chunks
    const data = new Uint8Array(MAX_CHUNK_SIZE + 100)
    for (let i = 0; i < data.length; i++) {
      data[i] = i % 256
    }

    const frames = [...encodeArtifactChunks(artifactId, data)]

    expect(frames).toHaveLength(2)

    // Verify first chunk
    let payloadLength = frames[0].readUInt32BE(0)
    let payload = frames[0].subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    let decoded = msgpackDecode(payload) as ArtifactChunkFrame

    expect(decoded.seq).toBe(1)
    expect(decoded.is_last).toBe(false)
    expect(decoded.data.length).toBe(MAX_CHUNK_SIZE)

    // Verify second chunk
    payloadLength = frames[1].readUInt32BE(0)
    payload = frames[1].subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    decoded = msgpackDecode(payload) as ArtifactChunkFrame

    expect(decoded.seq).toBe(2)
    expect(decoded.is_last).toBe(true)
    expect(decoded.data.length).toBe(100)
  })

  it('preserves data integrity across chunks', () => {
    const artifactId = 'artifact-integrity' as ArtifactId
    // Use smaller data to avoid stack overflow from spread operator
    const data = new Uint8Array(1024 * 100) // 100 KB, will be 1 chunk but tests integrity
    for (let i = 0; i < data.length; i++) {
      data[i] = i % 256
    }

    const frames = [...encodeArtifactChunks(artifactId, data)]
    const chunks: Uint8Array[] = []

    for (const frame of frames) {
      const payloadLength = frame.readUInt32BE(0)
      const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
      const decoded = msgpackDecode(payload) as ArtifactChunkFrame
      chunks.push(new Uint8Array(decoded.data))
    }

    // Concatenate chunks without spread operator
    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
    const reassembled = new Uint8Array(totalLength)
    let offset = 0
    for (const chunk of chunks) {
      reassembled.set(chunk, offset)
      offset += chunk.length
    }

    expect(reassembled).toEqual(data)
  })

  it('works with Buffer input', () => {
    const artifactId = 'artifact-buffer' as ArtifactId
    const data = Buffer.from([1, 2, 3, 4, 5])

    const frames = [...encodeArtifactChunks(artifactId, data)]

    expect(frames).toHaveLength(1)

    const payloadLength = frames[0].readUInt32BE(0)
    const payload = frames[0].subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as ArtifactChunkFrame

    expect(new Uint8Array(decoded.data)).toEqual(new Uint8Array([1, 2, 3, 4, 5]))
  })
})

describe('encodeRunResultFrame', () => {
  it('encodes completed outcome correctly', () => {
    const outcome: RunResultOutcome = {
      status: 'completed',
      message: 'run completed successfully'
    }
    const frame = encodeRunResultFrame(outcome)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as RunResultFrame

    expect(decoded.type).toBe('run_result')
    expect(decoded.outcome.status).toBe('completed')
    expect(decoded.outcome.message).toBe('run completed successfully')
    expect(decoded.proxy_used).toBeUndefined()
  })

  it('encodes error outcome with all fields', () => {
    const outcome: RunResultOutcome = {
      status: 'error',
      message: 'Script failed',
      error_type: 'script_error',
      stack: 'Error: Script failed\n  at main.ts:42'
    }
    const frame = encodeRunResultFrame(outcome)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as RunResultFrame

    expect(decoded.type).toBe('run_result')
    expect(decoded.outcome.status).toBe('error')
    expect(decoded.outcome.message).toBe('Script failed')
    expect(decoded.outcome.error_type).toBe('script_error')
    expect(decoded.outcome.stack).toBe('Error: Script failed\n  at main.ts:42')
  })

  it('encodes crash outcome correctly', () => {
    const outcome: RunResultOutcome = {
      status: 'crash',
      message: 'executor crashed'
    }
    const frame = encodeRunResultFrame(outcome)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as RunResultFrame

    expect(decoded.type).toBe('run_result')
    expect(decoded.outcome.status).toBe('crash')
    expect(decoded.outcome.message).toBe('executor crashed')
  })

  it('includes proxy_used when provided', () => {
    const outcome: RunResultOutcome = {
      status: 'completed'
    }
    const proxyUsed = {
      protocol: 'http' as const,
      host: 'proxy.example.com',
      port: 8080,
      username: 'user'
    }
    const frame = encodeRunResultFrame(outcome, proxyUsed)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as RunResultFrame

    expect(decoded.type).toBe('run_result')
    expect(decoded.proxy_used).toBeDefined()
    expect(decoded.proxy_used!.protocol).toBe('http')
    expect(decoded.proxy_used!.host).toBe('proxy.example.com')
    expect(decoded.proxy_used!.port).toBe(8080)
    expect(decoded.proxy_used!.username).toBe('user')
  })

  it('omits proxy_used when not provided', () => {
    const outcome: RunResultOutcome = {
      status: 'completed'
    }
    const frame = encodeRunResultFrame(outcome)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as RunResultFrame

    expect(decoded.proxy_used).toBeUndefined()
  })

  it('does not include password in proxy_used (type safety)', () => {
    const outcome: RunResultOutcome = {
      status: 'completed'
    }
    // TypeScript ensures password is not in the type
    const proxyUsed = {
      protocol: 'https' as const,
      host: 'proxy.example.com',
      port: 443
      // No password field - this is enforced by the type
    }
    const frame = encodeRunResultFrame(outcome, proxyUsed)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as Record<string, unknown>

    // Verify no password field exists
    const proxyUsedData = decoded.proxy_used as Record<string, unknown>
    expect(proxyUsedData.password).toBeUndefined()
  })
})

describe('encodeFileWriteFrame', () => {
  it('encodes write_id into frame', () => {
    const data = new Uint8Array([1, 2, 3])
    const frame = encodeFileWriteFrame('test.png', 'image/png', data, 42)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as FileWriteFrame

    expect(decoded.type).toBe('file_write')
    expect(decoded.write_id).toBe(42)
    expect(decoded.filename).toBe('test.png')
    expect(decoded.content_type).toBe('image/png')
    expect(new Uint8Array(decoded.data)).toEqual(data)
  })

  it('defaults write_id to 0 when omitted', () => {
    const data = new Uint8Array([1, 2, 3])
    const frame = encodeFileWriteFrame('test.csv', 'text/csv', data)

    const payloadLength = frame.readUInt32BE(0)
    const payload = frame.subarray(LENGTH_PREFIX_SIZE, LENGTH_PREFIX_SIZE + payloadLength)
    const decoded = msgpackDecode(payload) as FileWriteFrame

    expect(decoded.write_id).toBe(0)
  })

  it('throws ChunkValidationError when data exceeds MAX_CHUNK_SIZE', () => {
    const data = new Uint8Array(MAX_CHUNK_SIZE + 1)

    expect(() => encodeFileWriteFrame('big.bin', 'application/octet-stream', data, 1)).toThrow(
      ChunkValidationError
    )
  })
})

describe('decodeFileWriteAck', () => {
  it('decodes success ack', () => {
    const payload = msgpackEncode({
      type: 'file_write_ack',
      write_id: 7,
      ok: true
    })

    const ack = decodeFileWriteAck(payload)

    expect(ack.type).toBe('file_write_ack')
    expect(ack.write_id).toBe(7)
    expect(ack.ok).toBe(true)
    expect(ack.error).toBeUndefined()
  })

  it('decodes error ack', () => {
    const payload = msgpackEncode({
      type: 'file_write_ack',
      write_id: 3,
      ok: false,
      error: 'S3 PutObject failed'
    })

    const ack = decodeFileWriteAck(payload)

    expect(ack.type).toBe('file_write_ack')
    expect(ack.write_id).toBe(3)
    expect(ack.ok).toBe(false)
    expect(ack.error).toBe('S3 PutObject failed')
  })

  it('throws on wrong frame type', () => {
    const payload = msgpackEncode({
      type: 'artifact_chunk',
      write_id: 1,
      ok: true
    })

    expect(() => decodeFileWriteAck(payload)).toThrow('Expected file_write_ack frame')
  })
})
