/**
 * Quarry Executor Node
 *
 * Executes Quarry scripts in a Puppeteer environment and streams
 * events to the runtime via IPC.
 *
 * @packageDocumentation
 */

// Executor
export {
  type ExecutionOutcome,
  type ExecutorConfig,
  type ExecutorResult,
  execute,
  parseRunMeta
} from './executor.js'
// IPC (re-export for advanced usage)
export {
  // Types
  type ArtifactChunkFrame,
  type ArtifactChunkType,
  type ArtifactOpenFrame,
  type ArtifactRangeAckFrame,
  type ArtifactRangeFrame,
  type ChunkMeta,
  // Errors
  ChunkValidationError,
  type Frame,
  FrameSizeError,
  // Constants
  LENGTH_PREFIX_SIZE,
  MAX_CHUNK_SIZE,
  MAX_FRAME_SIZE,
  MAX_PAYLOAD_SIZE,
  // Sink
  ObservingSink,
  SinkAlreadyFailedError,
  type SinkState,
  StdioSink,
  StreamClosedError,
  type TerminalState,
  type TerminalType
} from './ipc/index.js'
// Loader
export { type LoadedScript, loadScript, ScriptLoadError } from './loader.js'
