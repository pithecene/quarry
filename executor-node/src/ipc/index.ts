/**
 * IPC module for executor-runtime communication.
 *
 * Implements CONTRACT_IPC.md framing and streaming protocol.
 *
 * @module
 */

export { AckReader } from './ack-reader.js'
export {
  type ArtifactChunkFrame,
  type ArtifactChunkType,
  type ArtifactOpenFrame,
  type ArtifactRangeAckFrame,
  type ArtifactRangeFrame,
  type ChunkMeta,
  ChunkValidationError,
  calculateChunks,
  decodeArtifactRangeAck,
  decodeFileWriteAck,
  encodeArtifactChunkFrame,
  encodeArtifactChunks,
  encodeArtifactOpenFrame,
  encodeArtifactRangeFrame,
  encodeEventFrame,
  encodeFileWriteFrame,
  encodeFrame,
  type FileWriteAckFrame,
  type FileWriteFrame,
  type Frame,
  FrameSizeError,
  LENGTH_PREFIX_SIZE,
  MAX_CHUNK_SIZE,
  MAX_FRAME_SIZE,
  MAX_PAYLOAD_SIZE
} from './frame.js'
export {
  ObservingSink,
  SinkAlreadyFailedError,
  type SinkState,
  type TerminalState,
  type TerminalType
} from './observing-sink.js'
export { drainStdout, StdioSink, StreamClosedError } from './sink.js'
export { installStdoutGuard, type StdoutGuardResult } from './stdout-guard.js'
