/**
 * Unit tests for emit.log() envelope correctness.
 *
 * Goal: Validate envelope construction for single log emits.
 *
 * Constraints:
 * - No concurrency
 * - No failures
 * - No terminal events
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { createEmitAPI } from '../../../src/emit-impl'
import { createRunMeta, FakeSink, validateEnvelope } from '../_harness'

describe('emit.log() envelope correctness', () => {
  let sink: FakeSink
  let run: ReturnType<typeof createRunMeta>

  beforeEach(() => {
    sink = new FakeSink()
    run = createRunMeta()
  })

  // Each case also checks the envelope type, which every log emit sets.
  it.each([
    {
      name: 'level',
      entry: { level: 'warn', message: 'warning' } as const,
      payload: { level: 'warn' }
    },
    {
      name: 'message',
      entry: { level: 'info', message: 'hello world' } as const,
      payload: { message: 'hello world' }
    },
    {
      name: 'optional fields',
      entry: {
        level: 'debug',
        message: 'debugging',
        fields: { user_id: 123, action: 'click' }
      } as const,
      payload: { level: 'debug', message: 'debugging', fields: { user_id: 123, action: 'click' } }
    }
  ])('creates a log envelope with $name in payload', async ({ entry, payload }) => {
    const emit = createEmitAPI(run, sink)

    await emit.log(entry)

    expect(sink.envelopes[0].type).toBe('log')
    expect(sink.envelopes[0].payload).toMatchObject(payload)
  })

  it('creates envelope without fields when not provided', async () => {
    const emit = createEmitAPI(run, sink)

    await emit.log({ level: 'info', message: 'no fields' })

    expect(sink.envelopes[0].payload).not.toHaveProperty('fields')
  })

  it('passes full envelope validation for all log levels', async () => {
    const emit = createEmitAPI(run, sink)

    await emit.log({ level: 'debug', message: 'debug' })
    await emit.log({ level: 'info', message: 'info' })
    await emit.log({ level: 'warn', message: 'warn' })
    await emit.log({ level: 'error', message: 'error' })

    for (const envelope of sink.envelopes) {
      const errors = validateEnvelope(envelope)
      expect(errors).toEqual([])
    }
  })

  describe('convenience methods', () => {
    it('emit.debug() creates log with level "debug"', async () => {
      const emit = createEmitAPI(run, sink)

      await emit.debug('debug message')

      expect(sink.envelopes[0].type).toBe('log')
      expect(sink.envelopes[0].payload).toMatchObject({
        level: 'debug',
        message: 'debug message'
      })
    })

    it('emit.info() creates log with level "info"', async () => {
      const emit = createEmitAPI(run, sink)

      await emit.info('info message')

      expect(sink.envelopes[0].payload).toMatchObject({
        level: 'info',
        message: 'info message'
      })
    })

    it('emit.warn() creates log with level "warn"', async () => {
      const emit = createEmitAPI(run, sink)

      await emit.warn('warn message')

      expect(sink.envelopes[0].payload).toMatchObject({
        level: 'warn',
        message: 'warn message'
      })
    })

    it('emit.error() creates log with level "error"', async () => {
      const emit = createEmitAPI(run, sink)

      await emit.error('error message')

      expect(sink.envelopes[0].payload).toMatchObject({
        level: 'error',
        message: 'error message'
      })
    })

    it('convenience methods accept optional fields', async () => {
      const emit = createEmitAPI(run, sink)

      await emit.info('with fields', { key: 'value' })

      expect(sink.envelopes[0].payload).toMatchObject({
        level: 'info',
        message: 'with fields',
        fields: { key: 'value' }
      })
    })
  })
})
