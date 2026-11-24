/**
 * Integration test: IPC frames survive backpressure through stdout guard.
 *
 * Spawns a child process that installs the stdout guard, creates a StdioSink
 * with the split contract (ipcOutput for events, ipcWrite for data), and
 * writes ~400KB of IPC frames — enough to exceed the OS pipe buffer plus
 * Node's internal Writable buffer (~180KB combined on Linux) and force at
 * least one backpressure cycle.
 *
 * The parent deliberately does NOT read from child stdout until the child
 * signals on stderr that backpressure occurred (write returned false). This
 * guarantees the OS pipe buffer fills, Node's internal buffer exceeds its
 * highWaterMark, and write() returns false — exercising the exact drain
 * path this test exists to cover. Once the signal arrives, the parent
 * begins reading, drain fires in the child, and execution continues.
 *
 * This test pins the drain deadlock fixed in #163: the old Object.create
 * proxy caused EventEmitter._events divergence after listener cleanup,
 * silently breaking drain delivery and hanging the executor forever.
 *
 * The 10s timeout is the deadlock detector — if drain delivery fails, the
 * child hangs and the test times out instead of passing silently.
 */
import { type ChildProcess, spawn } from 'node:child_process'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import { decodeFrames } from './helpers.js'

const FRAME_COUNT = 50

const testDir = dirname(fileURLToPath(import.meta.url))
const fixturePath = resolve(testDir, 'fixtures/backpressure-child.ts')
const tsxBin = resolve(testDir, '../../../node_modules/.bin/tsx')

/**
 * Parse `backpressure_events=N` from child stderr output.
 */
function parseBackpressureCount(stderr: string): number {
  const match = stderr.match(/backpressure_events=(\d+)/)
  if (!match) return -1
  return Number.parseInt(match[1], 10)
}

/**
 * Spawn the fixture child process and collect stdout + stderr + exit code.
 *
 * Does NOT read child stdout until either:
 * (a) the child signals backpressure on stderr ('BP'), or
 * (b) the child exits (fallback for the no-backpressure case).
 *
 * By withholding reads, the OS pipe buffer (~64KB) and Node's internal
 * Writable buffer (~64KB highWaterMark) both fill. Once combined capacity
 * (~180KB on Linux) is exhausted, write() returns false and the child must
 * wait for drain — the code path this test pins.
 */
function runFixture(): Promise<{ stdout: Buffer; stderr: string; exitCode: number | null }> {
  return new Promise((resolve, reject) => {
    let child: ChildProcess
    try {
      child = spawn(tsxBin, [fixturePath], {
        stdio: ['ignore', 'pipe', 'pipe']
      })
    } catch (err) {
      reject(err)
      return
    }

    const stdoutChunks: Buffer[] = []
    const stderrChunks: Buffer[] = []
    let reading = false
    let exitCode: number | null = null
    let exited = false
    let stdoutEnded = false

    const startReading = (): void => {
      if (reading) return
      reading = true
      child.stdout!.on('data', (chunk: Buffer) => stdoutChunks.push(chunk))
      child.stdout!.on('end', () => {
        stdoutEnded = true
        tryResolve()
      })
      // Guard against missed 'end': if stdout already ended before we
      // attached the listener (possible when exit fires late), the 'end'
      // event won't re-emit and stdoutEnded would never flip.
      if (child.stdout!.readableEnded) {
        stdoutEnded = true
        tryResolve()
      }
    }

    const tryResolve = (): void => {
      if (!exited || !stdoutEnded) return
      resolve({
        stdout: Buffer.concat(stdoutChunks),
        stderr: Buffer.concat(stderrChunks).toString('utf-8'),
        exitCode
      })
    }

    // Watch stderr for the backpressure signal. When the child's write()
    // returns false, it writes 'BP' to stderr. That's our cue to start
    // reading stdout — the pipe is full and the child is blocked on drain.
    child.stderr!.on('data', (chunk: Buffer) => {
      stderrChunks.push(chunk)
      if (!reading && chunk.toString().includes('BP')) {
        startReading()
      }
    })

    // Fallback: if the child exits without triggering backpressure (should
    // not happen with enough data, but handles edge cases), start reading
    // so we can still collect frames and fail on the backpressure assertion.
    child.on('exit', (code) => {
      exitCode = code
      exited = true
      startReading()
      // stdout 'end' may have already fired if stream was already flowing
      if (stdoutEnded) tryResolve()
    })

    child.on('error', reject)
  })
}

describe('backpressure integration', () => {
  it('IPC frames survive backpressure through stdout guard without deadlock', async () => {
    const { stdout, stderr, exitCode } = await runFixture()

    // Child should exit cleanly — a deadlock would hit the 10s timeout
    expect(exitCode, `child stderr: ${stderr}`).toBe(0)

    // Decode all frames from stdout
    const frames = decodeFrames(stdout) as Record<string, unknown>[]

    // All 51 frames (50 items + 1 run_complete) must arrive intact
    expect(frames).toHaveLength(FRAME_COUNT + 1)

    // Verify item frame ordering
    for (let i = 0; i < FRAME_COUNT; i++) {
      expect(frames[i].type).toBe('item')
      expect(frames[i].seq).toBe(i + 1)
    }

    // Verify terminal frame
    expect(frames[FRAME_COUNT].type).toBe('run_complete')
    expect(frames[FRAME_COUNT].seq).toBe(FRAME_COUNT + 1)

    // Stray write should appear on stderr, not stdout
    expect(stderr).toContain('[quarry] stdout guard:')
    expect(stderr).toContain('stray write during backpressure')

    // Prove backpressure actually occurred — the child's instrumented
    // writeFn must have seen at least one write() return false.
    const bpCount = parseBackpressureCount(stderr)
    expect(bpCount, 'child must report backpressure_events count on stderr').toBeGreaterThanOrEqual(
      0
    )
    expect(
      bpCount,
      'backpressure must have occurred (write returned false at least once)'
    ).toBeGreaterThan(0)
  }, 10_000) // 10s timeout: deadlock detector
})
