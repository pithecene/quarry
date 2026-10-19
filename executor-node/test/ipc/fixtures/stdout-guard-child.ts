/**
 * Child process fixture for stdout-guard integration test.
 *
 * Installs the stdout guard, writes IPC frames via StdioSink (using ipcWrite
 * to bypass the patched process.stdout.write), injects a stray stdout write
 * (simulating third-party code), writes another IPC frame, drains stdout, and
 * exits. The parent test verifies:
 * - stdout contains exactly 2 clean IPC frames (no stray text)
 * - stderr contains the stray text + guard warning
 */
import type { EventEnvelope, EventId, RunId } from '@pithecene/quarry-sdk'
import { StdioSink } from '../../../src/ipc/sink.js'
import { installStdoutGuard } from '../../../src/ipc/stdout-guard.js'

const { ipcOutput, ipcWrite } = installStdoutGuard()
const sink = new StdioSink(ipcOutput, ipcWrite)

// First IPC frame
await sink.writeEvent({
  contract_version: '0.7.3',
  event_id: 'evt-1' as EventId,
  run_id: 'run-guard-test' as RunId,
  seq: 1,
  type: 'item',
  ts: '2024-01-01T00:00:00.000Z',
  payload: { item_type: 'test', data: { key: 'value' } },
  attempt: 1
} as EventEnvelope<'item'>)

// Stray stdout write (simulates third-party code like puppeteer-extra)
process.stdout.write('Browser started successfully\n')

// Second IPC frame
await sink.writeEvent({
  contract_version: '0.7.3',
  event_id: 'evt-2' as EventId,
  run_id: 'run-guard-test' as RunId,
  seq: 2,
  type: 'run_complete',
  ts: '2024-01-01T00:00:00.000Z',
  payload: { summary: { items: 1 } },
  attempt: 1
} as EventEnvelope<'run_complete'>)

// Drain the real stdout (ipcOutput shares the underlying fd)
// drainStdout calls process.stdout.end() which is NOT patched
process.stdout.end(() => {
  process.exit(0)
})
