import { spawn } from 'node:child_process'

export type RunOptions = {
  /** Working directory; the current one when omitted. */
  readonly cwd?: string
  /**
   * `capture` collects stdout and stderr for the caller. `inherit` streams
   * them to this process's output, for long steps whose logs belong in CI.
   */
  readonly output?: 'capture' | 'inherit'
  /** Kills the command after this many milliseconds. */
  readonly timeoutMs?: number
}

export type RunResult = {
  /** Exit code, or null when the command was killed by a signal. */
  readonly code: number | null
  readonly stdout: string
  readonly stderr: string
  readonly timedOut: boolean
}

/**
 * Runs an external command. A non-zero exit is a result, not an exception:
 * callers decide what each outcome means. Only a command that cannot be
 * started at all rejects.
 */
export type Run = (
  command: string,
  args: readonly string[],
  options?: RunOptions
) => Promise<RunResult>

/** Grace period between SIGTERM and SIGKILL when a command times out. */
const KILL_GRACE_MS = 10_000

/** Signals that stop this process, forwarded to a running command. */
const FORWARDED_SIGNALS = ['SIGINT', 'SIGTERM'] as const

/**
 * Runs commands as child processes. Each command leads its own process
 * group, so a timeout or a cancelled job stops everything it started: a
 * publish client that forks a worker cannot outlive its attempt, and no
 * descendant can hold the output pipes open.
 */
export const spawnRun: Run = (command, args, options = {}) =>
  new Promise((resolve, reject) => {
    const output = options.output ?? 'capture'
    const child = spawn(command, args, {
      cwd: options.cwd,
      detached: true,
      stdio: output === 'capture' ? ['ignore', 'pipe', 'pipe'] : ['ignore', 'inherit', 'inherit']
    })

    let stdout = ''
    let stderr = ''
    child.stdout?.setEncoding('utf8').on('data', (chunk: string) => {
      stdout += chunk
    })
    child.stderr?.setEncoding('utf8').on('data', (chunk: string) => {
      stderr += chunk
    })

    const killGroup = (signal: NodeJS.Signals): void => {
      if (child.pid === undefined) return
      try {
        process.kill(-child.pid, signal)
      } catch {
        // The group has already exited.
      }
    }

    let timedOut = false
    const timers: NodeJS.Timeout[] = []
    /** Sends `signal` to the group, then SIGKILL if it outlives the grace period. */
    const terminate = (signal: NodeJS.Signals): void => {
      killGroup(signal)
      timers.push(setTimeout(() => killGroup('SIGKILL'), KILL_GRACE_MS))
    }
    if (options.timeoutMs !== undefined) {
      timers.push(
        setTimeout(() => {
          timedOut = true
          terminate('SIGTERM')
        }, options.timeoutMs)
      )
    }

    // A command in its own group no longer receives the signals that stop
    // this process, so they are passed on. Once the group has exited, this
    // process re-raises the signal and exits as the signal intends.
    let stopping: NodeJS.Signals | undefined
    const forward = (signal: NodeJS.Signals): void => {
      stopping = signal
      terminate(signal)
    }
    for (const signal of FORWARDED_SIGNALS) process.once(signal, forward)

    const cleanup = (): void => {
      for (const timer of timers) clearTimeout(timer)
      for (const signal of FORWARDED_SIGNALS) process.removeListener(signal, forward)
    }

    child.on('error', (error) => {
      cleanup()
      reject(error)
    })
    child.on('close', (code) => {
      // Once termination has begun, the leader's exit does not end the
      // group: a descendant that ignores SIGTERM would survive the cleared
      // SIGKILL timer. Whatever remains is killed now.
      if (timedOut || stopping !== undefined) killGroup('SIGKILL')
      cleanup()
      if (stopping !== undefined) {
        process.kill(process.pid, stopping)
        return
      }
      resolve({ code, stdout, stderr, timedOut })
    })
  })

/** The last non-empty line of command output, for concise error messages. */
export const lastLine = (text: string): string => text.trimEnd().split('\n').at(-1)?.trim() ?? ''
