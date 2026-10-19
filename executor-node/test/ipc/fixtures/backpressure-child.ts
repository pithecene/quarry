/**
 * Child process fixture for backpressure integration test.
 *
 * Installs the stdout guard, creates a StdioSink with an instrumented
 * writeFn that counts backpressure events (write returning false), and
 * writes ~400KB of IPC frames — enough to exceed the OS pipe buffer
 * (~64KB) plus Node's internal Writable buffer (~64KB highWaterMark),
 * totaling ~180KB capacity before write() returns false.
 *
 * The parent test withholds reading from stdout until it receives the
 * 'BP' signal on stderr, ensuring the pipe is truly full.
 *
 * Signals:
 * - 'BP' on stderr: emitted immediately when write() first returns false.
 *   Tells the parent the pipe is full and the child is blocked on drain.
 * - 'backpressure_events=N' on stderr: final count, emitted after all
 *   writes complete, for the parent to assert N > 0.
 *
 * If the drain event is not delivered (the bug this test pins), the child
 * will hang indefinitely and the parent test's timeout will catch it.
 */
import type { EventEnvelope, EventId, RunId } from '@pithecene/quarry-sdk'
import { drainStdout, StdioSink } from '../../../src/ipc/sink.js'
import { installStdoutGuard } from '../../../src/ipc/stdout-guard.js'

const { ipcOutput, ipcWrite } = installStdoutGuard()

// Instrument writeFn to count backpressure events (write returning false)
// and signal the parent when the first one occurs.
let backpressureCount = 0
const countingWrite = (data: Buffer): boolean => {
  const ok = ipcWrite(data)
  if (!ok) {
    backpressureCount++
    // Signal parent on first backpressure so it knows to start reading.
    // This is a separate pipe (stderr), so it won't affect stdout state.
    if (backpressureCount === 1) {
      process.stderr.write('BP\n')
    }
  }
  return ok
}

const sink = new StdioSink(ipcOutput, countingWrite)

// Write 50 frames × ~8KB each ≈ 400KB — well over the ~180KB combined
// capacity of the OS pipe buffer + Node's internal Writable buffer.
// This guarantees at least one write() returns false when the parent
// withholds reading.
const FRAME_COUNT = 50

for (let i = 1; i <= FRAME_COUNT; i++) {
  await sink.writeEvent({
    contract_version: '0.7.3',
    event_id: `evt-${i}` as EventId,
    run_id: 'run-bp-test' as RunId,
    seq: i,
    type: 'item',
    ts: '2024-01-01T00:00:00.000Z',
    payload: { item_type: 'test', data: { padding: 'x'.repeat(8000) } },
    attempt: 1
  } as EventEnvelope<'item'>)
}

// Inject a stray stdout write to exercise the guard under load
process.stdout.write('stray write during backpressure\n')

// Terminal event
await sink.writeEvent({
  contract_version: '0.7.3',
  event_id: `evt-${FRAME_COUNT + 1}` as EventId,
  run_id: 'run-bp-test' as RunId,
  seq: FRAME_COUNT + 1,
  type: 'run_complete',
  ts: '2024-01-01T00:00:00.000Z',
  payload: { summary: { items: FRAME_COUNT } },
  attempt: 1
} as EventEnvelope<'run_complete'>)

// Report backpressure count so the parent test can verify it occurred
process.stderr.write(`backpressure_events=${backpressureCount}\n`)

await drainStdout()
process.exit(0)
