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
 * - Artifact chunk size: up to 8 MiB (raw bytes, before msgpack encoding)
 *
 * Two frame types exist, both msgpack-encoded:
 * - Event frame: msgpack-encoded EventEnvelope (discriminated by envelope.type)
 * - Artifact chunk frame: msgpack-encoded chunk envelope with type='artifact_chunk'
 *
 * Decoding discrimination: if decoded.type === 'artifact_chunk', it's a chunk
 * frame; otherwise it's an event envelope (type will be 'item', 'log', etc.).
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
 * Maximum artifact chunk size in bytes (8 MiB).
 * This is the limit on raw bytes before msgpack encoding.
 * Artifacts larger than this must be split into multiple chunks.
 */
export const MAX_CHUNK_SIZE = 8 * 1024 * 1024

/**
 * Length prefix size in bytes.
 */
export const LENGTH_PREFIX_SIZE = 4

/**
 * Frame type discriminant for artifact chunk frames.
 * Event frames use the EventEnvelope's own type field ('item', 'log', etc.).
 */
export type ArtifactChunkType = 'artifact_chunk'

/**
 * Frame type discriminant for run result control frames.
 */
export type RunResultType = 'run_result'

/**
 * Artifact chunk frame per CONTRACT_IPC.md.
 * This is a stream-level construct, not a normal emit event.
 *
 * @remarks
 * The `data` field is encoded as msgpack bin type (not array).
 * Receivers must decode it as raw bytes.
 */
export type ArtifactChunkFrame = {
  readonly type: 'artifact_chunk'
  readonly artifact_id: ArtifactId
  /** Sequence number, starts at 1 */
  readonly seq: number
  /** True if this is the last chunk */
  readonly is_last: boolean
  /** Raw binary data (msgpack bin type) */
  readonly data: Uint8Array
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
 * - 'artifact_chunk' → ArtifactChunkFrame
 * - 'file_write' → FileWriteFrame (sidecar file upload)
 * - 'file_write_ack' → FileWriteAckFrame (runtime→executor ack)
 * - 'run_result' → RunResultFrame (control, not counted in seq)
 * - other (item, log, etc.) → EventEnvelope
 */
export type Frame =
  | EventEnvelope
  | ArtifactChunkFrame
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
 * Error thrown when chunk parameters violate CONTRACT_IPC constraints.
 */
export class ChunkValidationError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ChunkValidationError'
  }
}

/**
 * Encode an artifact chunk into a framed buffer.
 *
 * @param artifactId - The artifact ID
 * @param seq - Sequence number (must be >= 1 per CONTRACT_IPC)
 * @param isLast - True if this is the final chunk
 * @param data - Raw binary data (must be <= MAX_CHUNK_SIZE per CONTRACT_IPC)
 * @returns Buffer containing length prefix + msgpack-encoded frame
 * @throws ChunkValidationError if seq < 1 or data exceeds MAX_CHUNK_SIZE
 * @throws FrameSizeError if encoded payload exceeds MAX_PAYLOAD_SIZE
 */
export function encodeArtifactChunkFrame(
  artifactId: ArtifactId,
  seq: number,
  isLast: boolean,
  data: Uint8Array
): Buffer {
  // Validate per CONTRACT_IPC constraints
  if (seq < 1) {
    throw new ChunkValidationError(`seq must be >= 1, got ${seq}`)
  }
  if (data.length > MAX_CHUNK_SIZE) {
    throw new ChunkValidationError(
      `data size ${data.length} exceeds MAX_CHUNK_SIZE ${MAX_CHUNK_SIZE}`
    )
  }

  const frame: ArtifactChunkFrame = {
    type: 'artifact_chunk',
    artifact_id: artifactId,
    seq,
    is_last: isLast,
    data
  }
  const payload = msgpackEncode(frame)
  return encodeFrame(payload)
}

/**
 * Metadata for a single artifact chunk (without data).
 * Used by chunk iterators to avoid data copying.
 */
export type ChunkMeta = {
  readonly seq: number
  readonly isLast: boolean
  readonly offset: number
  readonly length: number
}

/**
 * Calculate chunk metadata for artifact data.
 * Does not copy data; returns offsets for slicing.
 *
 * @param totalSize - Total size of the artifact data
 * @returns Array of chunk metadata
 */
export function calculateChunks(totalSize: number): ChunkMeta[] {
  const chunks: ChunkMeta[] = []

  if (totalSize === 0) {
    // Empty artifact: single chunk with is_last=true
    chunks.push({ seq: 1, isLast: true, offset: 0, length: 0 })
    return chunks
  }

  let offset = 0
  let seq = 1

  while (offset < totalSize) {
    const remaining = totalSize - offset
    const length = Math.min(remaining, MAX_CHUNK_SIZE)
    const isLast = offset + length >= totalSize

    chunks.push({ seq, isLast, offset, length })

    offset += length
    seq++
  }

  return chunks
}

/**
 * Generator that yields encoded artifact chunk frames.
 * Memory-efficient: encodes one chunk at a time.
 *
 * @param artifactId - The artifact ID for the chunks
 * @param data - The binary data to chunk
 * @yields Encoded frame buffers ready for transport
 */
export function* encodeArtifactChunks(
  artifactId: ArtifactId,
  data: Buffer | Uint8Array
): Generator<Buffer, void, unknown> {
  const chunks = calculateChunks(data.length)

  for (const chunk of chunks) {
    const chunkData = data.subarray(chunk.offset, chunk.offset + chunk.length)
    yield encodeArtifactChunkFrame(artifactId, chunk.seq, chunk.isLast, chunkData)
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
