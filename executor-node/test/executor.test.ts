import { PassThrough } from 'node:stream'
import { decode as msgpackDecode } from '@msgpack/msgpack'
import type {
  ArtifactId,
  EmitSink,
  EventEnvelope,
  EventId,
  JobId,
  ProxyEndpoint,
  RunId
} from '@pithecene/quarry-sdk'
import { beforeEach, describe, expect, it, type Mock, vi } from 'vitest'
import {
  _resetPuppeteerForTesting,
  type ExecutorConfig,
  type ExecutorResult,
  execute,
  parseRunMeta
} from '../src/executor.js'
import type { RunResultFrame } from '../src/ipc/frame.js'
import { ObservingSink, SinkAlreadyFailedError } from '../src/ipc/observing-sink.js'
import type { LoadedScript } from '../src/loader.js'

// Mock dependencies
vi.mock('../src/loader.js', () => ({
  loadScript: vi.fn(),
  ScriptLoadError: class ScriptLoadError extends Error {
    constructor(
      public scriptPath: string,
      public reason: string
    ) {
      super(`Failed to load script "${scriptPath}": ${reason}`)
      this.name = 'ScriptLoadError'
    }
  }
}))

vi.mock('puppeteer', () => ({
  default: {
    launch: vi.fn()
  }
}))

const mockUse = vi.fn()
vi.mock('puppeteer-extra', () => ({
  addExtra: vi.fn((puppeteer: Record<string, unknown>) => ({
    ...puppeteer,
    use: mockUse
  }))
}))

const mockStealthInstance = { _type: 'stealth-plugin' }
vi.mock('puppeteer-extra-plugin-stealth', () => ({
  default: vi.fn(() => mockStealthInstance)
}))

const mockAdblockerInstance = { _type: 'adblocker-plugin' }
vi.mock('puppeteer-extra-plugin-adblocker', () => ({
  default: vi.fn(() => mockAdblockerInstance)
}))

import puppeteer from 'puppeteer'
import { loadScript, ScriptLoadError } from '../src/loader.js'

/**
 * Create a mock Puppeteer setup.
 */
function createMockPuppeteer() {
  const page = {
    close: vi.fn().mockResolvedValue(undefined)
  }
  const browserContext = {
    newPage: vi.fn().mockResolvedValue(page),
    close: vi.fn().mockResolvedValue(undefined)
  }
  const browser = {
    createBrowserContext: vi.fn().mockResolvedValue(browserContext),
    close: vi.fn().mockResolvedValue(undefined)
  }

  return { browser, browserContext, page }
}

/**
 * Create a mock output stream that behaves like a real writable stream.
 */
function createMockOutput() {
  return new PassThrough()
}

describe('parseRunMeta', () => {
  describe('required field validation', () => {
    it('throws on null input', () => {
      expect(() => parseRunMeta(null)).toThrow('run metadata must be an object')
    })

    it('throws on non-object input', () => {
      expect(() => parseRunMeta('string')).toThrow('run metadata must be an object')
      expect(() => parseRunMeta(123)).toThrow('run metadata must be an object')
    })

    it('throws on missing run_id', () => {
      expect(() => parseRunMeta({ attempt: 1 })).toThrow('run_id must be a non-empty string')
    })

    it('throws on empty run_id', () => {
      expect(() => parseRunMeta({ run_id: '', attempt: 1 })).toThrow(
        'run_id must be a non-empty string'
      )
    })

    it('throws on missing attempt', () => {
      expect(() => parseRunMeta({ run_id: 'run-123' })).toThrow(
        'attempt must be a positive integer'
      )
    })

    it('throws on non-integer attempt', () => {
      expect(() => parseRunMeta({ run_id: 'run-123', attempt: 1.5 })).toThrow(
        'attempt must be a positive integer'
      )
    })

    it('throws on attempt < 1', () => {
      expect(() => parseRunMeta({ run_id: 'run-123', attempt: 0 })).toThrow(
        'attempt must be a positive integer'
      )
      expect(() => parseRunMeta({ run_id: 'run-123', attempt: -1 })).toThrow(
        'attempt must be a positive integer'
      )
    })
  })

  describe('valid input parsing', () => {
    it('parses minimal valid input', () => {
      const run = parseRunMeta({ run_id: 'run-123', attempt: 1 })

      expect(run.run_id).toBe('run-123')
      expect(run.attempt).toBe(1)
      expect(run.job_id).toBeUndefined()
      expect(run.parent_run_id).toBeUndefined()
    })

    it('parses input with job_id', () => {
      const run = parseRunMeta({ run_id: 'run-123', attempt: 1, job_id: 'job-456' })

      expect(run.job_id).toBe('job-456')
    })

    it('ignores empty job_id', () => {
      const run = parseRunMeta({ run_id: 'run-123', attempt: 1, job_id: '' })

      expect(run.job_id).toBeUndefined()
    })
  })

  describe('lineage validation (strict)', () => {
    it('throws when initial run (attempt=1) has parent_run_id', () => {
      expect(() =>
        parseRunMeta({
          run_id: 'run-123',
          attempt: 1,
          parent_run_id: 'run-parent'
        })
      ).toThrow('initial run (attempt=1) must not have parent_run_id')
    })

    it('throws when retry run (attempt>1) lacks parent_run_id', () => {
      expect(() =>
        parseRunMeta({
          run_id: 'run-123',
          attempt: 3
        })
      ).toThrow('retry run (attempt=3) must have parent_run_id')
    })

    it('accepts valid initial run without parent_run_id', () => {
      const run = parseRunMeta({
        run_id: 'run-123',
        attempt: 1
      })

      expect(run.attempt).toBe(1)
      expect(run.parent_run_id).toBeUndefined()
    })

    it('accepts valid retry run with parent_run_id', () => {
      const run = parseRunMeta({
        run_id: 'run-123',
        attempt: 2,
        parent_run_id: 'run-parent'
      })

      expect(run.attempt).toBe(2)
      expect(run.parent_run_id).toBe('run-parent')
    })
  })

  describe('type branding', () => {
    it('returns branded types', () => {
      const run = parseRunMeta({
        run_id: 'run-123',
        attempt: 2,
        job_id: 'job-456',
        parent_run_id: 'run-parent'
      })

      // TypeScript would catch misuse, but we can verify the values
      const runId: RunId = run.run_id
      const jobId: JobId | undefined = run.job_id
      const parentRunId: RunId | undefined = run.parent_run_id

      expect(runId).toBe('run-123')
      expect(jobId).toBe('job-456')
      expect(parentRunId).toBe('run-parent')
    })
  })
})

describe('execute()', () => {
  let mockPuppeteer: ReturnType<typeof createMockPuppeteer>
  let mockOutput: PassThrough

  beforeEach(() => {
    vi.clearAllMocks()

    mockPuppeteer = createMockPuppeteer()
    ;(puppeteer.launch as Mock).mockResolvedValue(mockPuppeteer.browser)

    mockOutput = createMockOutput()
  })

  function createConfig(overrides: Partial<ExecutorConfig> = {}): ExecutorConfig {
    return {
      scriptPath: '/path/to/script.js',
      job: { test: true },
      run: {
        run_id: 'run-123' as RunId,
        attempt: 1
      },
      output: mockOutput,
      ...overrides
    }
  }

  function createMockScript(overrides: Partial<LoadedScript> = {}): LoadedScript {
    return {
      script: vi.fn().mockResolvedValue(undefined),
      hooks: {},
      module: { default: vi.fn() },
      ...overrides
    }
  }

  describe('outcome precedence', () => {
    it('returns error status when terminal run_error was emitted', async () => {
      const mockScript = createMockScript({
        script: vi.fn().mockImplementation(async (ctx) => {
          await ctx.emit.runError({
            error_type: 'validation_failed',
            message: 'Invalid data found'
          })
        })
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('error')
      if (result.outcome.status === 'error') {
        expect(result.outcome.errorType).toBe('validation_failed')
        expect(result.outcome.message).toBe('Invalid data found')
      }
      expect(result.terminalEmitted).toBe(true)
    })

    it('returns completed status when terminal run_complete was emitted', async () => {
      const mockScript = createMockScript({
        script: vi.fn().mockImplementation(async (ctx) => {
          await ctx.emit.runComplete({ summary: { items: 10 } })
        })
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('completed')
      if (result.outcome.status === 'completed') {
        expect(result.outcome.summary).toEqual({ items: 10 })
      }
      expect(result.terminalEmitted).toBe(true)
    })

    it('auto-emits run_error when script throws without terminal', async () => {
      const mockScript = createMockScript({
        script: vi.fn().mockRejectedValue(new Error('Script failed'))
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('error')
      if (result.outcome.status === 'error') {
        expect(result.outcome.errorType).toBe('script_error')
        expect(result.outcome.message).toBe('Script failed')
      }
      // terminalEmitted is true after auto-emit because the sink observes the terminal event
      expect(result.terminalEmitted).toBe(true)
    })

    it('auto-emits run_complete when script completes without terminal', async () => {
      const mockScript = createMockScript({
        script: vi.fn().mockResolvedValue(undefined)
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('completed')
    })
  })

  describe('onError hook behavior', () => {
    it('calls onError when script throws before terminal event', async () => {
      const onError = vi.fn()
      const scriptError = new Error('Script failed')
      const mockScript = createMockScript({
        script: vi.fn().mockRejectedValue(scriptError),
        hooks: { onError }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(onError).toHaveBeenCalledTimes(1)
      expect(onError).toHaveBeenCalledWith(scriptError, expect.any(Object))
    })

    it('does NOT call onError when script throws after terminal event', async () => {
      const onError = vi.fn()
      const mockScript = createMockScript({
        script: vi.fn().mockImplementation(async (ctx) => {
          await ctx.emit.runComplete({ summary: {} })
          throw new Error('Post-terminal error')
        }),
        hooks: { onError }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(onError).not.toHaveBeenCalled()
    })

    it('swallows errors from onError hook', async () => {
      const onError = vi.fn().mockRejectedValue(new Error('Hook failed'))
      const mockScript = createMockScript({
        script: vi.fn().mockRejectedValue(new Error('Script failed')),
        hooks: { onError }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      // Should not throw
      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('error')
      expect(onError).toHaveBeenCalled()
    })
  })

  describe('sink failure scenarios', () => {
    it('returns crash when script load fails', async () => {
      ;(loadScript as Mock).mockRejectedValue(
        new ScriptLoadError('/path/to/script.js', 'file not found')
      )

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('crash')
      if (result.outcome.status === 'crash') {
        expect(result.outcome.message).toContain('Failed to load script')
      }
    })

    it('returns crash when puppeteer fails to launch', async () => {
      ;(loadScript as Mock).mockResolvedValue(createMockScript())
      ;(puppeteer.launch as Mock).mockRejectedValue(new Error('Failed to launch browser'))

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('crash')
      if (result.outcome.status === 'crash') {
        expect(result.outcome.message).toBe('Failed to launch browser')
      }
    })
  })

  describe('lifecycle hooks', () => {
    it('calls beforeRun before script', async () => {
      const callOrder: string[] = []
      const mockScript = createMockScript({
        script: vi.fn().mockImplementation(async () => {
          callOrder.push('script')
        }),
        hooks: {
          beforeRun: vi.fn().mockImplementation(async () => {
            callOrder.push('beforeRun')
          })
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(callOrder).toEqual(['beforeRun', 'script'])
    })

    it('calls afterRun after script on success', async () => {
      const callOrder: string[] = []
      const mockScript = createMockScript({
        script: vi.fn().mockImplementation(async () => {
          callOrder.push('script')
        }),
        hooks: {
          afterRun: vi.fn().mockImplementation(async () => {
            callOrder.push('afterRun')
          })
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(callOrder).toEqual(['script', 'afterRun'])
    })

    it('does not call afterRun when script throws', async () => {
      const afterRun = vi.fn()
      const mockScript = createMockScript({
        script: vi.fn().mockRejectedValue(new Error('Script failed')),
        hooks: { afterRun }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(afterRun).not.toHaveBeenCalled()
    })

    it('calls cleanup after terminal emission', async () => {
      const callOrder: string[] = []
      const mockScript = createMockScript({
        script: vi.fn().mockImplementation(async (ctx) => {
          callOrder.push('script')
          await ctx.emit.runComplete()
          callOrder.push('after-emit')
        }),
        hooks: {
          cleanup: vi.fn().mockImplementation(async () => {
            callOrder.push('cleanup')
          })
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(callOrder).toContain('cleanup')
      // Cleanup runs after script completes (not immediately after terminal emit)
      expect(callOrder.indexOf('cleanup')).toBeGreaterThan(callOrder.indexOf('script'))
    })

    it('swallows errors from cleanup hook', async () => {
      const cleanup = vi.fn().mockRejectedValue(new Error('Cleanup failed'))
      const mockScript = createMockScript({
        script: vi.fn().mockResolvedValue(undefined),
        hooks: { cleanup }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      // Should not throw
      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('completed')
      expect(cleanup).toHaveBeenCalled()
    })
  })

  describe('prepare hook', () => {
    it('proceeds normally when prepare returns continue', async () => {
      const scriptFn = vi.fn().mockResolvedValue(undefined)
      const mockScript = createMockScript({
        script: scriptFn,
        hooks: {
          prepare: vi.fn().mockResolvedValue({ action: 'continue' })
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('completed')
      expect(scriptFn).toHaveBeenCalled()
    })

    it('passes transformed job when prepare returns continue with job', async () => {
      const scriptFn = vi.fn().mockResolvedValue(undefined)
      const transformedJob = { transformed: true, url: 'https://example.com' }
      const mockScript = createMockScript({
        script: scriptFn,
        hooks: {
          prepare: vi.fn().mockResolvedValue({ action: 'continue', job: transformedJob })
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('completed')
      expect(scriptFn).toHaveBeenCalledTimes(1)
      // The context passed to the script should contain the transformed job
      const ctx = scriptFn.mock.calls[0][0]
      expect(ctx.job).toEqual(transformedJob)
    })

    it('skips run when prepare returns skip with reason', async () => {
      const scriptFn = vi.fn().mockResolvedValue(undefined)
      const mockScript = createMockScript({
        script: scriptFn,
        hooks: {
          prepare: vi.fn().mockResolvedValue({ action: 'skip', reason: 'duplicate job' })
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('completed')
      expect(result.terminalEmitted).toBe(true)
      if (result.outcome.status === 'completed') {
        expect(result.outcome.summary).toEqual({ skipped: true, reason: 'duplicate job' })
      }
      // Script must NOT have been called
      expect(scriptFn).not.toHaveBeenCalled()
      // Browser must NOT have been launched
      expect(puppeteer.launch).not.toHaveBeenCalled()
    })

    it('skips run when prepare returns skip without reason', async () => {
      const scriptFn = vi.fn().mockResolvedValue(undefined)
      const mockScript = createMockScript({
        script: scriptFn,
        hooks: {
          prepare: vi.fn().mockResolvedValue({ action: 'skip' })
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('completed')
      expect(result.terminalEmitted).toBe(true)
      if (result.outcome.status === 'completed') {
        expect(result.outcome.summary).toEqual({ skipped: true })
      }
      expect(scriptFn).not.toHaveBeenCalled()
    })

    it('returns crash when prepare throws', async () => {
      const scriptFn = vi.fn().mockResolvedValue(undefined)
      const mockScript = createMockScript({
        script: scriptFn,
        hooks: {
          prepare: vi.fn().mockRejectedValue(new Error('prepare boom'))
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('crash')
      if (result.outcome.status === 'crash') {
        expect(result.outcome.message).toBe('prepare boom')
      }
      expect(scriptFn).not.toHaveBeenCalled()
      expect(puppeteer.launch).not.toHaveBeenCalled()
    })

    it('executes normally when prepare is not exported', async () => {
      const scriptFn = vi.fn().mockResolvedValue(undefined)
      const mockScript = createMockScript({
        script: scriptFn,
        hooks: {}
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('completed')
      expect(scriptFn).toHaveBeenCalled()
    })

    it('does not call any downstream hooks on prepare skip', async () => {
      const beforeRun = vi.fn()
      const afterRun = vi.fn()
      const onError = vi.fn()
      const beforeTerminal = vi.fn()
      const cleanup = vi.fn()
      const mockScript = createMockScript({
        script: vi.fn().mockResolvedValue(undefined),
        hooks: {
          prepare: vi.fn().mockResolvedValue({ action: 'skip', reason: 'stale' }),
          beforeRun,
          afterRun,
          onError,
          beforeTerminal,
          cleanup
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(beforeRun).not.toHaveBeenCalled()
      expect(afterRun).not.toHaveBeenCalled()
      expect(onError).not.toHaveBeenCalled()
      expect(beforeTerminal).not.toHaveBeenCalled()
      expect(cleanup).not.toHaveBeenCalled()
    })

    it('returns crash with diagnostic when prepare returns null', async () => {
      const scriptFn = vi.fn().mockResolvedValue(undefined)
      const mockScript = createMockScript({
        script: scriptFn,
        hooks: {
          prepare: vi.fn().mockResolvedValue(null)
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('crash')
      if (result.outcome.status === 'crash') {
        expect(result.outcome.message).toContain('prepare hook must return')
      }
      expect(scriptFn).not.toHaveBeenCalled()
    })

    it('returns crash with diagnostic when prepare returns undefined', async () => {
      const scriptFn = vi.fn().mockResolvedValue(undefined)
      const mockScript = createMockScript({
        script: scriptFn,
        hooks: {
          prepare: vi.fn().mockResolvedValue(undefined)
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('crash')
      if (result.outcome.status === 'crash') {
        expect(result.outcome.message).toContain('prepare hook must return')
      }
      expect(scriptFn).not.toHaveBeenCalled()
    })

    it('returns crash with diagnostic when prepare returns object without action', async () => {
      const scriptFn = vi.fn().mockResolvedValue(undefined)
      const mockScript = createMockScript({
        script: scriptFn,
        hooks: {
          prepare: vi.fn().mockResolvedValue({ foo: 'bar' })
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('crash')
      if (result.outcome.status === 'crash') {
        expect(result.outcome.message).toContain('prepare hook must return')
      }
      expect(scriptFn).not.toHaveBeenCalled()
    })
  })

  describe('beforeTerminal hook', () => {
    it('fires with completed signal on success path', async () => {
      const beforeTerminal = vi.fn()
      const mockScript = createMockScript({
        script: vi.fn().mockResolvedValue(undefined),
        hooks: { beforeTerminal }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(beforeTerminal).toHaveBeenCalledTimes(1)
      const [signal, ctx] = beforeTerminal.mock.calls[0]
      expect(signal).toEqual({ outcome: 'completed' })
      expect(ctx).toHaveProperty('emit')
      expect(ctx).toHaveProperty('job')
    })

    it('fires with error signal on error path', async () => {
      const scriptError = new Error('Script failed')
      const beforeTerminal = vi.fn()
      const mockScript = createMockScript({
        script: vi.fn().mockRejectedValue(scriptError),
        hooks: { beforeTerminal }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(beforeTerminal).toHaveBeenCalledTimes(1)
      const [signal] = beforeTerminal.mock.calls[0]
      expect(signal.outcome).toBe('error')
      expect(signal.error).toBe(scriptError)
    })

    it('can emit items from beforeTerminal', async () => {
      const beforeTerminal = vi.fn().mockImplementation(async (_signal, ctx) => {
        await ctx.emit.item({ item_type: 'summary', data: { total: 42 } })
      })
      const mockScript = createMockScript({
        script: vi.fn().mockResolvedValue(undefined),
        hooks: { beforeTerminal }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('completed')
      expect(beforeTerminal).toHaveBeenCalled()
    })

    it('is NOT called when script manually emitted terminal', async () => {
      const beforeTerminal = vi.fn()
      const mockScript = createMockScript({
        script: vi.fn().mockImplementation(async (ctx) => {
          await ctx.emit.runComplete({ summary: { done: true } })
        }),
        hooks: { beforeTerminal }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(beforeTerminal).not.toHaveBeenCalled()
    })

    it('is NOT called when script emitted terminal then threw', async () => {
      const beforeTerminal = vi.fn()
      const mockScript = createMockScript({
        script: vi.fn().mockImplementation(async (ctx) => {
          await ctx.emit.runComplete()
          throw new Error('Post-terminal throw')
        }),
        hooks: { beforeTerminal }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(beforeTerminal).not.toHaveBeenCalled()
    })

    it('suppresses auto-terminal when hook emits terminal itself', async () => {
      const beforeTerminal = vi.fn().mockImplementation(async (_signal, ctx) => {
        await ctx.emit.runComplete({ summary: { hook_terminal: true } })
      })
      const mockScript = createMockScript({
        script: vi.fn().mockResolvedValue(undefined),
        hooks: { beforeTerminal }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('completed')
      expect(result.terminalEmitted).toBe(true)
      if (result.outcome.status === 'completed') {
        expect(result.outcome.summary).toEqual({ hook_terminal: true })
      }
    })

    it('swallows errors and terminal is still emitted', async () => {
      const beforeTerminal = vi.fn().mockRejectedValue(new Error('hook boom'))
      const mockScript = createMockScript({
        script: vi.fn().mockResolvedValue(undefined),
        hooks: { beforeTerminal }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      const result = await execute(createConfig())

      expect(result.outcome.status).toBe('completed')
      expect(result.terminalEmitted).toBe(true)
      expect(beforeTerminal).toHaveBeenCalled()
    })

    it('fires afterRun → beforeTerminal → cleanup in order on success', async () => {
      const callOrder: string[] = []
      const mockScript = createMockScript({
        script: vi.fn().mockImplementation(async () => {
          callOrder.push('script')
        }),
        hooks: {
          afterRun: vi.fn().mockImplementation(async () => {
            callOrder.push('afterRun')
          }),
          beforeTerminal: vi.fn().mockImplementation(async () => {
            callOrder.push('beforeTerminal')
          }),
          cleanup: vi.fn().mockImplementation(async () => {
            callOrder.push('cleanup')
          })
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(callOrder).toEqual(['script', 'afterRun', 'beforeTerminal', 'cleanup'])
    })

    it('fires onError → beforeTerminal → cleanup in order on error', async () => {
      const callOrder: string[] = []
      const mockScript = createMockScript({
        script: vi.fn().mockImplementation(async () => {
          callOrder.push('script')
          throw new Error('boom')
        }),
        hooks: {
          onError: vi.fn().mockImplementation(async () => {
            callOrder.push('onError')
          }),
          beforeTerminal: vi.fn().mockImplementation(async () => {
            callOrder.push('beforeTerminal')
          }),
          cleanup: vi.fn().mockImplementation(async () => {
            callOrder.push('cleanup')
          })
        }
      })
      ;(loadScript as Mock).mockResolvedValue(mockScript)

      await execute(createConfig())

      expect(callOrder).toEqual(['script', 'onError', 'beforeTerminal', 'cleanup'])
    })
  })
})

describe('SinkAlreadyFailedError', () => {
  it('includes original cause message in error message', () => {
    const originalError = new Error('Connection reset')
    const sinkError = new SinkAlreadyFailedError(originalError)

    expect(sinkError.message).toBe('Sink has already failed: Connection reset')
    expect(sinkError.cause).toBe(originalError)
  })

  it('handles non-Error cause', () => {
    const sinkError = new SinkAlreadyFailedError('string cause')

    expect(sinkError.message).toBe('Sink has already failed: string cause')
    expect(sinkError.cause).toBe('string cause')
  })
})

/**
 * Helper to extract run_result frame from output.
 * Reads all frames and returns the last run_result frame if found.
 */
function extractRunResultFrame(output: PassThrough): RunResultFrame | null {
  const chunks: Buffer[] = []
  let chunk: Buffer | null
  while ((chunk = output.read()) !== null) {
    chunks.push(chunk)
  }
  if (chunks.length === 0) return null

  const data = Buffer.concat(chunks)
  let offset = 0

  // Parse all frames, looking for run_result
  let lastRunResult: RunResultFrame | null = null
  while (offset < data.length) {
    if (offset + 4 > data.length) break
    const payloadLen = data.readUInt32BE(offset)
    offset += 4
    if (offset + payloadLen > data.length) break
    const payload = data.subarray(offset, offset + payloadLen)
    offset += payloadLen

    try {
      const decoded = msgpackDecode(payload) as Record<string, unknown>
      if (decoded.type === 'run_result') {
        lastRunResult = decoded as unknown as RunResultFrame
      }
    } catch {
      // Ignore decode errors
    }
  }

  return lastRunResult
}

describe('run_result frame emission', () => {
  let mockPuppeteer: ReturnType<typeof createMockPuppeteer>
  let mockOutput: PassThrough

  beforeEach(() => {
    vi.clearAllMocks()
    mockPuppeteer = createMockPuppeteer()
    ;(puppeteer.launch as Mock).mockResolvedValue(mockPuppeteer.browser)
    mockOutput = createMockOutput()
  })

  function createConfig(overrides: Partial<ExecutorConfig> = {}): ExecutorConfig {
    return {
      scriptPath: '/path/to/script.js',
      job: { test: true },
      run: {
        run_id: 'run-123' as RunId,
        attempt: 1
      },
      output: mockOutput,
      ...overrides
    }
  }

  function createMockScript(overrides: Partial<LoadedScript> = {}): LoadedScript {
    return {
      script: vi.fn().mockResolvedValue(undefined),
      hooks: {},
      module: { default: vi.fn() },
      ...overrides
    }
  }

  it('emits run_result frame with completed status on success', async () => {
    const mockScript = createMockScript({
      script: vi.fn().mockImplementation(async (ctx) => {
        await ctx.emit.runComplete({ summary: { items: 10 } })
      })
    })
    ;(loadScript as Mock).mockResolvedValue(mockScript)

    await execute(createConfig())

    const runResult = extractRunResultFrame(mockOutput)
    expect(runResult).not.toBeNull()
    expect(runResult!.type).toBe('run_result')
    expect(runResult!.outcome.status).toBe('completed')
  })

  it('emits run_result frame with error status on script error', async () => {
    const mockScript = createMockScript({
      script: vi.fn().mockImplementation(async (ctx) => {
        await ctx.emit.runError({
          error_type: 'validation_failed',
          message: 'Invalid data'
        })
      })
    })
    ;(loadScript as Mock).mockResolvedValue(mockScript)

    await execute(createConfig())

    const runResult = extractRunResultFrame(mockOutput)
    expect(runResult).not.toBeNull()
    expect(runResult!.outcome.status).toBe('error')
    expect(runResult!.outcome.error_type).toBe('validation_failed')
    expect(runResult!.outcome.message).toBe('Invalid data')
  })

  it('includes redacted proxy_used when proxy is configured', async () => {
    const mockScript = createMockScript({
      script: vi.fn().mockResolvedValue(undefined)
    })
    ;(loadScript as Mock).mockResolvedValue(mockScript)

    const proxy: ProxyEndpoint = {
      protocol: 'http',
      host: 'proxy.example.com',
      port: 8080,
      username: 'user',
      password: 'secret123'
    }

    await execute(createConfig({ proxy }))

    const runResult = extractRunResultFrame(mockOutput)
    expect(runResult).not.toBeNull()
    expect(runResult!.proxy_used).toBeDefined()
    expect(runResult!.proxy_used!.protocol).toBe('http')
    expect(runResult!.proxy_used!.host).toBe('proxy.example.com')
    expect(runResult!.proxy_used!.port).toBe(8080)
    expect(runResult!.proxy_used!.username).toBe('user')
    // Password must NOT be included (per CONTRACT_PROXY.md)
    expect((runResult!.proxy_used as Record<string, unknown>).password).toBeUndefined()
  })

  it('omits proxy_used when no proxy is configured', async () => {
    const mockScript = createMockScript({
      script: vi.fn().mockResolvedValue(undefined)
    })
    ;(loadScript as Mock).mockResolvedValue(mockScript)

    await execute(createConfig())

    const runResult = extractRunResultFrame(mockOutput)
    expect(runResult).not.toBeNull()
    expect(runResult!.proxy_used).toBeUndefined()
  })

  it('emits run_result frame when prepare returns skip', async () => {
    const mockScript = createMockScript({
      script: vi.fn().mockResolvedValue(undefined),
      hooks: {
        prepare: vi.fn().mockResolvedValue({ action: 'skip', reason: 'dup' })
      }
    })
    ;(loadScript as Mock).mockResolvedValue(mockScript)

    await execute(createConfig())

    const runResult = extractRunResultFrame(mockOutput)
    expect(runResult).not.toBeNull()
    expect(runResult!.type).toBe('run_result')
    expect(runResult!.outcome.status).toBe('completed')
  })

  it('emits run_result frame when prepare throws', async () => {
    const mockScript = createMockScript({
      script: vi.fn().mockResolvedValue(undefined),
      hooks: {
        prepare: vi.fn().mockRejectedValue(new Error('boom'))
      }
    })
    ;(loadScript as Mock).mockResolvedValue(mockScript)

    await execute(createConfig())

    const runResult = extractRunResultFrame(mockOutput)
    expect(runResult).not.toBeNull()
    expect(runResult!.outcome.status).toBe('crash')
    expect(runResult!.outcome.message).toBe('boom')
  })

  it('emits run_result frame when prepare returns malformed result', async () => {
    const mockScript = createMockScript({
      script: vi.fn().mockResolvedValue(undefined),
      hooks: {
        prepare: vi.fn().mockResolvedValue(null)
      }
    })
    ;(loadScript as Mock).mockResolvedValue(mockScript)

    await execute(createConfig())

    const runResult = extractRunResultFrame(mockOutput)
    expect(runResult).not.toBeNull()
    expect(runResult!.outcome.status).toBe('crash')
    expect(runResult!.outcome.message).toContain('prepare hook must return')
  })
})

describe('puppeteer-extra plugin setup', () => {
  let mockPuppeteer: ReturnType<typeof createMockPuppeteer>
  let mockOutput: PassThrough

  beforeEach(() => {
    vi.clearAllMocks()
    // Reset singleton so each test gets a fresh getPuppeteer() call
    _resetPuppeteerForTesting()

    mockPuppeteer = createMockPuppeteer()
    ;(puppeteer.launch as Mock).mockResolvedValue(mockPuppeteer.browser)

    mockOutput = createMockOutput()
  })

  function createConfig(overrides: Partial<ExecutorConfig> = {}): ExecutorConfig {
    return {
      scriptPath: '/path/to/script.js',
      job: { test: true },
      run: {
        run_id: 'run-123' as RunId,
        attempt: 1
      },
      output: mockOutput,
      ...overrides
    }
  }

  function createMockScript(overrides: Partial<LoadedScript> = {}): LoadedScript {
    return {
      script: vi.fn().mockResolvedValue(undefined),
      hooks: {},
      module: { default: vi.fn() },
      ...overrides
    }
  }

  it('applies stealth plugin by default', async () => {
    ;(loadScript as Mock).mockResolvedValue(createMockScript())

    await execute(createConfig())

    expect(mockUse).toHaveBeenCalledWith(mockStealthInstance)
  })

  it('does not apply stealth when stealth: false', async () => {
    ;(loadScript as Mock).mockResolvedValue(createMockScript())

    await execute(createConfig({ stealth: false }))

    expect(mockUse).not.toHaveBeenCalledWith(mockStealthInstance)
  })

  it('does not apply adblocker by default', async () => {
    ;(loadScript as Mock).mockResolvedValue(createMockScript())

    await execute(createConfig())

    expect(mockUse).not.toHaveBeenCalledWith(mockAdblockerInstance)
  })

  it('applies adblocker when adblocker: true', async () => {
    ;(loadScript as Mock).mockResolvedValue(createMockScript())

    await execute(createConfig({ adblocker: true }))

    expect(mockUse).toHaveBeenCalledWith(mockAdblockerInstance)
  })

  it('reinitializes when subsequent call has different plugin config', async () => {
    ;(loadScript as Mock).mockResolvedValue(createMockScript())

    // First call initializes with stealth: true (default)
    await execute(createConfig())
    expect(mockUse).toHaveBeenCalledWith(mockStealthInstance)

    const callsAfterFirst = mockUse.mock.calls.length

    // Second call requests different config (stealth: false) — should reinitialize
    // Do NOT reset singleton between calls
    await execute(createConfig({ stealth: false }))

    // mockUse should NOT have been called again with stealth (stealth: false)
    const stealthCallsAfterSecond = mockUse.mock.calls
      .slice(callsAfterFirst)
      .filter((args: unknown[]) => args[0] === mockStealthInstance)
    expect(stealthCallsAfterSecond).toHaveLength(0)
  })
})
