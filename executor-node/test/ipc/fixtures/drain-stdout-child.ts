/**
 * Child process fixture for drain-stdout integration test.
 *
 * Writes IPC frames (item event, run_complete terminal, run_result control)
 * to stdout via StdioSink, calls drainStdout(), then exits immediately.
 * The parent test verifies all frames are readable from the pipe before EOF.
 */
import type { EventEnvelope, EventId, RunId } from '@pithecene/quarry-sdk'
import { drainStdout, StdioSink } from '../../../src/ipc/sink.js'

const sink = new StdioSink(process.stdout)

// Item event
await sink.writeEvent({
  contract_version: '0.7.3',
  event_id: 'evt-1' as EventId,
  run_id: 'run-drain-test' as RunId,
  seq: 1,
  type: 'item',
  ts: '2024-01-01T00:00:00.000Z',
  payload: { item_type: 'test', data: { key: 'value' } },
  attempt: 1
} as EventEnvelope<'item'>)

// Terminal event (run_complete)
await sink.writeEvent({
  contract_version: '0.7.3',
  event_id: 'evt-2' as EventId,
  run_id: 'run-drain-test' as RunId,
  seq: 2,
  type: 'run_complete',
  ts: '2024-01-01T00:00:00.000Z',
  payload: { summary: { items: 1 } },
  attempt: 1
} as EventEnvelope<'run_complete'>)

// Run result control frame
await sink.writeRunResult({
  status: 'completed',
  message: 'run completed successfully'
})

await drainStdout()
process.exit(0)
