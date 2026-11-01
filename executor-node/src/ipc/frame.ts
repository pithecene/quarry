/**
 * IPC framing implementation per CONTRACT_IPC.md.
 *
 * Frame structure:
 * - 4-byte big-endian length prefix (unsigned, big-endian)
 * - payload bytes (msgpack-encoded)
 *
 * Constraints:
 * - Maximum frame size: 16 MiB (including length prefix)
 * - Maximum payload size: 16 MiB - 4 bytes
 * - Artifact range size: up to 8 MiB (raw bytes, before msgpack encoding)
 *
 * Every frame is msgpack-encoded:
 * - Event frame: msgpack-encoded EventEnvelope (discriminated by envelope.type)
 * - Artifact frames: 'artifact_open', 'artifact_range', and 'artifact_range_ack'
 *
 * Decoding discrimination: a frame whose type names an artifact, file write,
 * or run result frame is that frame; otherwise it is an event envelope.
 *
 * @module
 * @remarks Node.js only. Uses Buffer for transport efficiency.
 */

import { decode as msgpackDecode, encode as msgpackEncode } from '@msgpack/msgpack'
import type { ArtifactId, EventEnvelope } from '@pithecene/quarry-sdk'

/**
 * Maximum frame size in bytes (16 MiB), including length prefix.
 * Frames exceeding this limit are invalid and must be rejected.
 */
export const MAX_FRAME_SIZE = 16 * 1024 * 1024

/**
 * Maximum payload size in bytes (MAX_FRAME_SIZE - LENGTH_PREFIX_SIZE).
 */
export const MAX_PAYLOAD_SIZE = MAX_FRAME_SIZE - 4

/**
 * Maximum artifact range and file write data size in bytes (8 MiB).
 * This is the limit on raw bytes before msgpack encoding.
 * An artifact larger than this is sent as several ranges.
 */
export const MAX_CHUNK_SIZE = 8 * 1024 * 1024

/**
 * Length prefix size in bytes.
 */
export const LENGTH_PREFIX_SIZE = 4

/**
 * Frame type discriminant for run result control frames.
 */
export type RunResultType = 'run_result'

/**
 * Artifact open frame per CONTRACT_IPC.md §Artifact Frames: the artifact's
 * identity and size, sent before any of its bytes.
 */
export type ArtifactOpenFrame = {
  readonly type: 'artifact_open'
  readonly artifact_id: ArtifactId
  /** The artifact's length in bytes, zero for an empty artifact */
  readonly size_bytes: number
  /** Hex SHA-256 the script expects of the bytes, when it declares one */
  readonly expected_sha256?: string
}

/**
 * Artifact range frame per CONTRACT_IPC.md §Artifact Frames: one range of
 * an opened artifact's bytes, at most MAX_CHUNK_SIZE, at any offset.
 *
 * @remarks
 * The `data` field is encoded as msgpack bin type (not array).
 */
export type ArtifactRangeFrame = {
  readonly type: 'artifact_range'
  readonly artifact_id: ArtifactId
  /** Correlation ID counting up from 1 within the run, repeated by the ack */
  readonly range_id: number
  /** Byte offset of the range within the artifact */
  readonly offset: number
  /** Raw binary data (msgpack bin type) */
  readonly data: Uint8Array
}

/**
 * Artifact range acknowledgement frame (runtime → executor via stdin).
 * Sent once the range is committed, or once it is refused.
 */
export type ArtifactRangeAckFrame = {
  readonly type: 'artifact_range_ack'
  /** Correlation ID from the artifact_range frame */
  readonly range_id: number
  /** True once the range is committed */
  readonly ok: boolean
  /** Error message when ok is false */
  readonly error?: string
}

/**
 * Run result outcome status.
 */
export type RunResultStatus = 'completed' | 'error' | 'crash'

/**
 * Run result outcome per CONTRACT_IPC.md.
 * Describes the final outcome of a run.
 */
export type RunResultOutcome = {
  /** Outcome status */
  readonly status: RunResultStatus
  /** Human-readable message */
  readonly message?: string
  /** Error type (for error status) */
  readonly error_type?: string
  /** Stack trace (for error status) */
  readonly stack?: string
}

/**
 * Redacted proxy endpoint for run results (no password).
 * Per CONTRACT_PROXY.md: proxy_used must exclude password fields.
 */
export type ProxyEndpointRedactedFrame = {
  readonly protocol: 'http' | 'https' | 'socks5'
  readonly host: string
  readonly port: number
  readonly username?: string
}

/**
 * Run result control frame per CONTRACT_IPC.md.
 * This is a control frame emitted once after terminal event emission.
 * It does NOT affect seq ordering (not an event).
 */
export type RunResultFrame = {
  readonly type: 'run_result'
  /** Final run outcome */
  readonly outcome: RunResultOutcome
  /** Proxy endpoint used (redacted, no password) */
  readonly proxy_used?: ProxyEndpointRedactedFrame
}

/**
 * File write frame for sidecar file uploads via Lode Store.
 * Single-frame transport (not chunked). Max 8 MiB data.
 * Bypasses seq numbering, policy pipeline, and artifact manager.
 */
export type FileWriteFrame = {
  readonly type: 'file_write'
  /** Monotonic correlation ID, starts at 1. Zero means no ack expected (legacy). */
  readonly write_id: number
  /** Target filename (no path separators, no "..") */
  readonly filename: string
  /** MIME content type */
  readonly content_type: string
  /** Raw binary data (max MAX_CHUNK_SIZE) */
  readonly data: Uint8Array
}

/**
 * File write acknowledgement frame sent by runtime to executor via stdin.
 * Correlates to a file_write frame via write_id.
 */
export type FileWriteAckFrame = {
  readonly type: 'file_write_ack'
  /** Correlation ID from the file_write frame */
  readonly write_id: number
  /** True if the write succeeded */
  readonly ok: boolean
  /** Error message when ok is false */
  readonly error?: string
}

/**
 * Union of all frame payload types for decoding.
 * Discriminate using type field:
 * - 'artifact_open' → ArtifactOpenFrame, 'artifact_range' → ArtifactRangeFrame, 'artifact_range_ack' → ArtifactRangeAckFrame
 * - 'file_write' → FileWriteFrame (sidecar file upload)
 * - 'file_write_ack' → FileWriteAckFrame (runtime→executor ack)
 * - 'run_result' → RunResultFrame (control, not counted in seq)
 * - other (item, log, etc.) → EventEnvelope
 */
export type Frame =
  | EventEnvelope
  | ArtifactOpenFrame
  | ArtifactRangeFrame
  | ArtifactRangeAckFrame
  | RunResultFrame
  | FileWriteFrame
  | FileWriteAckFrame

/**
 * Error thrown when a frame exceeds the maximum size.
 */
export class FrameSizeError extends Error {
  constructor(
    public readonly payloadSize: number,
    public readonly maxPayloadSize: number
  ) {
    super(`Payload size ${payloadSize} exceeds maximum ${maxPayloadSize}`)
    this.name = 'FrameSizeError'
  }
}

/**
 * Encode data into a framed buffer with length prefix.
 *
 * @param payload - The payload bytes to frame
 * @returns Buffer containing length prefix + payload
 * @throws FrameSizeError if payload exceeds MAX_PAYLOAD_SIZE
 *
 * @remarks
 * The total frame size (prefix + payload) is bounded by MAX_FRAME_SIZE.
 */
export function encodeFrame(payload: Uint8Array): Buffer {
  if (payload.length > MAX_PAYLOAD_SIZE) {
    throw new FrameSizeError(payload.length, MAX_PAYLOAD_SIZE)
  }

  const frame = Buffer.allocUnsafe(LENGTH_PREFIX_SIZE + payload.length)
  frame.writeUInt32BE(payload.length, 0)
  frame.set(payload, LENGTH_PREFIX_SIZE)

  return frame
}

/**
 * Encode an event envelope into a framed buffer.
 * Per CONTRACT_IPC.md, the payload is the msgpack-encoded envelope directly.
 *
 * @param envelope - The event envelope to encode
 * @returns Buffer containing length prefix + msgpack-encoded envelope
 * @throws FrameSizeError if encoded payload exceeds MAX_PAYLOAD_SIZE
 */
export function encodeEventFrame(envelope: EventEnvelope): Buffer {
  const payload = msgpackEncode(envelope)
  return encodeFrame(payload)
}

/**
 * Error thrown when a range or file write violates CONTRACT_IPC constraints.
 */
export class ChunkValidationError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ChunkValidationError'
  }
}

/**
 * Encode a run result control frame.
 *
 * Per CONTRACT_IPC.md, this is a control frame that:
 * - Is emitted once after terminal event emission attempt
 * - Does NOT affect seq ordering (not counted as an event)
 * - Contains outcome and optional proxy_used (redacted)
 *
 * @param outcome - The run outcome
 * @param proxyUsed - Optional redacted proxy endpoint (no password)
 * @returns Buffer containing length prefix + msgpack-encoded frame
 * @throws FrameSizeError if encoded payload exceeds MAX_PAYLOAD_SIZE
 */
export function encodeRunResultFrame(
  outcome: RunResultOutcome,
  proxyUsed?: ProxyEndpointRedactedFrame
): Buffer {
  const frame: RunResultFrame = {
    type: 'run_result',
    outcome,
    ...(proxyUsed && { proxy_used: proxyUsed })
  }
  const payload = msgpackEncode(frame)
  return encodeFrame(payload)
}

/**
 * Encode a file write frame for sidecar file uploads.
 *
 * File writes bypass seq numbering and the policy pipeline.
 * Max data size is MAX_CHUNK_SIZE (8 MiB).
 *
 * @param filename - Target filename (no path separators, no "..")
 * @param contentType - MIME content type
 * @param data - Raw binary data
 * @param writeId - Monotonic correlation ID for ack matching (0 = no ack expected)
 * @returns Buffer containing length prefix + msgpack-encoded frame
 * @throws ChunkValidationError if data exceeds MAX_CHUNK_SIZE
 * @throws FrameSizeError if encoded payload exceeds MAX_PAYLOAD_SIZE
 */
export function encodeFileWriteFrame(
  filename: string,
  contentType: string,
  data: Buffer | Uint8Array,
  writeId = 0
): Buffer {
  if (data.length > MAX_CHUNK_SIZE) {
    throw new ChunkValidationError(
      `file data size ${data.length} exceeds MAX_CHUNK_SIZE ${MAX_CHUNK_SIZE}`
    )
  }

  const frame: FileWriteFrame = {
    type: 'file_write',
    write_id: writeId,
    filename,
    content_type: contentType,
    data
  }
  const payload = msgpackEncode(frame)
  return encodeFrame(payload)
}

/**
 * Decode a file write acknowledgement frame from a msgpack payload.
 *
 * @param payload - Raw msgpack payload (without length prefix)
 * @returns Decoded FileWriteAckFrame
 * @throws Error if payload is not a valid file_write_ack frame
 */
export function decodeFileWriteAck(payload: Uint8Array): FileWriteAckFrame {
  const decoded = msgpackDecode(payload) as Record<string, unknown>
  if (decoded.type !== 'file_write_ack') {
    throw new Error(`Expected file_write_ack frame, got type: ${String(decoded.type)}`)
  }
  return {
    type: 'file_write_ack',
    write_id: decoded.write_id as number,
    ok: decoded.ok as boolean,
    ...(decoded.error != null && { error: decoded.error as string })
  }
}

/**
 * Encode an artifact open frame into a framed buffer.
 *
 * @param artifactId - The artifact ID
 * @param sizeBytes - The artifact's length in bytes
 * @param expectedSha256 - Hex SHA-256 the script expects, when it declares one
 * @returns Buffer containing length prefix + msgpack-encoded frame
 */
export function encodeArtifactOpenFrame(
  artifactId: ArtifactId,
  sizeBytes: number,
  expectedSha256?: string
): Buffer {
  const frame: ArtifactOpenFrame = {
    type: 'artifact_open',
    artifact_id: artifactId,
    size_bytes: sizeBytes,
    ...(expectedSha256 !== undefined && { expected_sha256: expectedSha256 })
  }
  return encodeFrame(msgpackEncode(frame))
}

/**
 * Encode an artifact range frame into a framed buffer.
 *
 * @param artifactId - The artifact ID
 * @param rangeId - Correlation ID for ack matching, counting up from 1
 * @param offset - Byte offset of the range within the artifact
 * @param data - The range's bytes (must be <= MAX_CHUNK_SIZE per CONTRACT_IPC)
 * @returns Buffer containing length prefix + msgpack-encoded frame
 * @throws ChunkValidationError if data exceeds MAX_CHUNK_SIZE
 * @throws FrameSizeError if encoded payload exceeds MAX_PAYLOAD_SIZE
 */
export function encodeArtifactRangeFrame(
  artifactId: ArtifactId,
  rangeId: number,
  offset: number,
  data: Uint8Array
): Buffer {
  if (data.length > MAX_CHUNK_SIZE) {
    throw new ChunkValidationError(
      `range size ${data.length} exceeds MAX_CHUNK_SIZE ${MAX_CHUNK_SIZE}`
    )
  }
  const frame: ArtifactRangeFrame = {
    type: 'artifact_range',
    artifact_id: artifactId,
    range_id: rangeId,
    offset,
    data
  }
  return encodeFrame(msgpackEncode(frame))
}

/**
 * Decode an artifact range acknowledgement frame from a msgpack payload.
 *
 * @param payload - Raw msgpack payload (without length prefix)
 * @returns Decoded ArtifactRangeAckFrame
 * @throws Error if payload is not a valid artifact_range_ack frame
 */
export function decodeArtifactRangeAck(payload: Uint8Array): ArtifactRangeAckFrame {
  const decoded = msgpackDecode(payload) as Record<string, unknown>
  if (decoded.type !== 'artifact_range_ack') {
    throw new Error(`Expected artifact_range_ack frame, got type: ${String(decoded.type)}`)
  }
  return {
    type: 'artifact_range_ack',
    range_id: decoded.range_id as number,
    ok: decoded.ok as boolean,
    ...(decoded.error != null && { error: decoded.error as string })
  }
}
