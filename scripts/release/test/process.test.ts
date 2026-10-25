import { type ChildProcess, spawn } from 'node:child_process'
import { once } from 'node:events'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import { lastLine, spawnRun } from '../src/process.ts'

const PROCESS_TS = fileURLToPath(new URL('../src/process.ts', import.meta.url))

const node = (script: string) => [process.execPath, ['-e', script]] as const

/**
 * Whether a process with this pid is still running. A killed process whose
 * parent has not reaped it yet is a zombie: signal 0 still finds it, but it no
 * longer runs. Orphans are reparented when the harness exits, and a runner's
 * reaper may take longer than the test waits, so on Linux a zombie counts as
 * gone.
 */
const isAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0)
  } catch {
    return false
  }
  return !isZombie(pid)
}

/** Whether /proc reports the process as a zombie (Linux only). */
const isZombie = (pid: number): boolean => {
  try {
    // The state follows the parenthesized command name, which may contain spaces.
    const stat = readFileSync(`/proc/${pid}/stat`, 'utf8')
    return stat.slice(stat.lastIndexOf(')') + 2, stat.lastIndexOf(')') + 3) === 'Z'
  } catch {
    return false
  }
}

/**
 * Whether the process is gone within `ms`. A killed orphan stays visible
 * as a zombie until its new parent reaps it, so the check polls.
 */
const goneWithin = async (pid: number, ms = 5_000): Promise<boolean> => {
  const deadline = Date.now() + ms
  while (isAlive(pid)) {
    if (Date.now() > deadline) return false
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  return true
}

/** The first line the child writes to stdout, as a number. */
const firstLine = async (child: ChildProcess): Promise<number> => {
  const stdout = child.stdout
  if (stdout === null) throw new Error('child has no stdout')
  let text = ''
  for await (const chunk of stdout) {
    text += String(chunk)
    if (text.includes('\n')) break
  }
  return Number(text.split('\n')[0])
}

describe('spawnRun', () => {
  it('captures stdout, stderr, and the exit code', async () => {
    const [cmd, args] = node(
      'process.stdout.write("out"); process.stderr.write("err"); process.exitCode = 3'
    )
    expect(await spawnRun(cmd, args)).toEqual({
      code: 3,
      stdout: 'out',
      stderr: 'err',
      timedOut: false
    })
  })

  it('kills a command that exceeds its timeout', async () => {
    const [cmd, args] = node('setTimeout(() => {}, 60_000)')
    const result = await spawnRun(cmd, args, { timeoutMs: 100 })
    expect(result.timedOut).toBe(true)
    expect(result.code).toBeNull()
  })

  it('kills the whole process group on timeout, so a descendant cannot hold the pipes', async () => {
    // The shell starts a background sleep that inherits the output pipes.
    const started = Date.now()
    const result = await spawnRun('sh', ['-c', 'sleep 30 & echo $!; wait'], { timeoutMs: 200 })
    expect(result.timedOut).toBe(true)
    expect(Date.now() - started).toBeLessThan(5_000)
    expect(await goneWithin(Number(result.stdout.trim()))).toBe(true)
  })

  it('kills a descendant that ignores SIGTERM once the timed-out leader exits', async () => {
    // The worker ignores SIGTERM and does not hold the output pipes, so the
    // leader's exit alone would let it run on.
    const script = '(trap "" TERM; exec sleep 30) >/dev/null 2>&1 & echo $!; wait'
    const result = await spawnRun('sh', ['-c', script], { timeoutMs: 200 })
    expect(result.timedOut).toBe(true)
    expect(await goneWithin(Number(result.stdout.trim()))).toBe(true)
  })

  it('passes SIGTERM on to the running command and then exits by it', async () => {
    // A child process runs spawnRun, so the signal does not reach vitest.
    const harness = spawn(
      process.execPath,
      [
        '--input-type=module',
        '-e',
        `import { spawnRun } from ${JSON.stringify(PROCESS_TS)}
         await spawnRun('sh', ['-c', 'echo $$; exec sleep 30'], { output: 'inherit' })`
      ],
      { stdio: ['ignore', 'pipe', 'inherit'] }
    )
    const pid = await firstLine(harness)
    harness.kill('SIGTERM')
    const [, signal] = await once(harness, 'exit')
    expect(signal).toBe('SIGTERM')
    expect(await goneWithin(pid)).toBe(true)
  }, 15_000)

  it('runs in the given working directory', async () => {
    const [cmd, args] = node('process.stdout.write(process.cwd())')
    expect((await spawnRun(cmd, args, { cwd: '/' })).stdout).toBe('/')
  })

  it('rejects when the command cannot be started', async () => {
    await expect(spawnRun('definitely-not-a-command-xyz', [])).rejects.toThrow(/ENOENT/)
  })
})

describe('lastLine', () => {
  it.each([
    ['a\nb\n', 'b'],
    ['a\nb\n\n  \n', 'b'],
    ['', '']
  ])('of %j is %j', (text, line) => {
    expect(lastLine(text)).toBe(line)
  })
})
