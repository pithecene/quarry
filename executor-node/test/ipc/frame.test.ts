import { decode as msgpackDecode, encode as msgpackEncode } from '@msgpack/msgpack'
import type { ArtifactId, EventEnvelope, EventId, JobId, RunId } from '@pithecene/quarry-sdk'
import { describe, expect, it } from 'vitest'
import {
  type ArtifactRangeFrame,
  ChunkValidationError,
  decodeArtifactRangeAck,
  decodeFileWriteAck,
  encodeArtifactOpenFrame,
  encodeArtifactRangeFrame,
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
      type: 'artifact_open',
      write_id: 1,
      ok: true
    })

    expect(() => decodeFileWriteAck(payload)).toThrow('Expected file_write_ack frame')
  })
})

describe('artifact frames (CONTRACT_IPC.md §Artifact Frames)', () => {
  it('encodes an open frame with the size and the expected digest', () => {
    const frame = encodeArtifactOpenFrame('art-1' as ArtifactId, 12, 'abc123')
    const payloadLength = frame.readUInt32BE(0)
    const decoded = msgpackDecode(frame.subarray(4, 4 + payloadLength))
    expect(decoded).toEqual({
      type: 'artifact_open',
      artifact_id: 'art-1',
      size_bytes: 12,
      expected_sha256: 'abc123'
    })
  })

  it('encodes an open frame without a digest when the script declares none', () => {
    const frame = encodeArtifactOpenFrame('art-1' as ArtifactId, 0)
    const payloadLength = frame.readUInt32BE(0)
    expect(msgpackDecode(frame.subarray(4, 4 + payloadLength))).toEqual({
      type: 'artifact_open',
      artifact_id: 'art-1',
      size_bytes: 0
    })
  })

  it('encodes a range frame with its id, offset, and bytes as bin', () => {
    const data = new Uint8Array([1, 2, 3])
    const frame = encodeArtifactRangeFrame('art-1' as ArtifactId, 7, 8_388_608, data)
    const payloadLength = frame.readUInt32BE(0)
    const decoded = msgpackDecode(frame.subarray(4, 4 + payloadLength)) as ArtifactRangeFrame
    expect(decoded.type).toBe('artifact_range')
    expect(decoded.artifact_id).toBe('art-1')
    expect(decoded.range_id).toBe(7)
    expect(decoded.offset).toBe(8_388_608)
    expect(decoded.data).toBeInstanceOf(Uint8Array)
    expect(new Uint8Array(decoded.data)).toEqual(data)
  })

  it('refuses a range above MAX_CHUNK_SIZE', () => {
    const data = new Uint8Array(MAX_CHUNK_SIZE + 1)
    expect(() => encodeArtifactRangeFrame('art-1' as ArtifactId, 1, 0, data)).toThrow(
      ChunkValidationError
    )
  })

  it('decodes a range ack with and without an error', () => {
    const ok = msgpackEncode({ type: 'artifact_range_ack', range_id: 3, ok: true })
    expect(decodeArtifactRangeAck(ok)).toEqual({
      type: 'artifact_range_ack',
      range_id: 3,
      ok: true
    })
    const refused = msgpackEncode({
      type: 'artifact_range_ack',
      range_id: 4,
      ok: false,
      error: 'range past size_bytes'
    })
    expect(decodeArtifactRangeAck(refused)).toEqual({
      type: 'artifact_range_ack',
      range_id: 4,
      ok: false,
      error: 'range past size_bytes'
    })
  })

  it('refuses to decode another frame as a range ack', () => {
    const other = msgpackEncode({ type: 'file_write_ack', write_id: 1, ok: true })
    expect(() => decodeArtifactRangeAck(other)).toThrow('Expected artifact_range_ack')
  })
})
