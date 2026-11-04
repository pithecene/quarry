import type * as ChildProcessModule from 'node:child_process'
import { describe, expect, it, vi } from 'vitest'

/** The SIGTERM listener count at the moment spawnRun starts its command. */
const atSpawn = vi.hoisted(() => ({ sigtermListeners: -1 }))

// The command starts running as soon as spawn returns, so a SIGTERM may
// arrive before spawnRun's next statement. The mock records whether the
// forwarding listener is already in place when spawn is called.
vi.mock('node:child_process', async (importOriginal) => {
  const actual = await importOriginal<typeof ChildProcessModule>()
  return {
    ...actual,
    spawn: ((...args: Parameters<typeof actual.spawn>) => {
      atSpawn.sigtermListeners = process.listenerCount('SIGTERM')
      return actual.spawn(...args)
    }) as typeof actual.spawn
  }
})

const { spawnRun } = await import('../src/process.ts')

describe('spawnRun signal forwarding', () => {
  it('listens for SIGTERM before it starts the command', async () => {
    const before = process.listenerCount('SIGTERM')
    await spawnRun(process.execPath, ['-e', ''])
    expect(atSpawn.sigtermListeners).toBe(before + 1)
    expect(process.listenerCount('SIGTERM')).toBe(before)
  })
})
