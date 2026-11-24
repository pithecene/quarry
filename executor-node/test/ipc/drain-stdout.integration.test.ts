/**
 * Integration test: terminal frame visibility at the pipe boundary.
 *
 * Spawns a child process that writes IPC frames (including terminal event
 * and run_result) to stdout via StdioSink, calls drainStdout(), then
 * process.exit(). The parent reads the pipe and asserts every frame —
 * including the terminal — is present before EOF.
 *
 * This pins the race where process.exit() discards buffered stdout data,
 * causing the Go runtime to see EOF without a terminal event and classify
 * the run as executor_crash.
 */
import { describe, expect, it } from 'vitest'
import { decodeFrames, runFixture } from './helpers.js'

describe('drainStdout integration', () => {
  it('terminal event and run_result are visible on pipe before EOF', async () => {
    const { stdout, stderr, exitCode } = await runFixture('drain-stdout-child.ts')

    // Child should exit cleanly
    expect(exitCode, `child stderr: ${stderr}`).toBe(0)

    const frames = decodeFrames(stdout) as Record<string, unknown>[]

    // All three frames must survive the pipe: item, run_complete, run_result
    expect(frames).toHaveLength(3)

    // Frame 0: item event
    expect(frames[0].type).toBe('item')
    expect(frames[0].seq).toBe(1)

    // Frame 1: terminal event (run_complete)
    expect(frames[1].type).toBe('run_complete')
    expect(frames[1].seq).toBe(2)

    // Frame 2: run_result control frame
    expect(frames[2].type).toBe('run_result')
    const outcome = frames[2].outcome as Record<string, unknown>
    expect(outcome.status).toBe('completed')
  }, 15_000)
})
