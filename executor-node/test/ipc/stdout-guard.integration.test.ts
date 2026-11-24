/**
 * Integration test: stdout guard protects IPC channel from stray writes.
 *
 * Spawns a child process that installs the stdout guard, writes IPC frames
 * through ipcOutput, injects a stray process.stdout.write (simulating
 * third-party code), and exits. The parent reads the pipe and asserts:
 * - stdout contains exactly 2 valid IPC frames (no stray text contamination)
 * - stderr contains the stray text + guard warning
 * - exit code 0
 */
import { describe, expect, it } from 'vitest'
import { runFrames } from './helpers.js'

describe('stdout guard integration', () => {
  it('IPC frames are clean on stdout, stray writes appear on stderr', async () => {
    const { frames, stderr } = await runFrames('stdout-guard-child.ts')
    expect(frames).toHaveLength(2)

    // Frame 0: item event
    expect(frames[0].type).toBe('item')
    expect(frames[0].seq).toBe(1)

    // Frame 1: run_complete terminal event
    expect(frames[1].type).toBe('run_complete')
    expect(frames[1].seq).toBe(2)

    // Stderr should contain the guard warning
    expect(stderr).toContain('[quarry] stdout guard:')
    expect(stderr).toContain('Browser started successfully')
  }, 15_000)
})
