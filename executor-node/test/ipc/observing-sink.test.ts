import type { ArtifactId, EmitSink, EventEnvelope, EventId, RunId } from '@pithecene/quarry-sdk'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ObservingSink, SinkAlreadyFailedError } from '../../src/ipc/observing-sink.js'

/**
 * Create a mock EmitSink for testing.
 */
function createMockSink(): EmitSink & {
  writeEventMock: ReturnType<typeof vi.fn>
  writeArtifactDataMock: ReturnType<typeof vi.fn>
  writeFileMock: ReturnType<typeof vi.fn>
} {
  const writeEventMock = vi.fn().mockResolvedValue(undefined)
  const writeArtifactDataMock = vi.fn().mockResolvedValue(undefined)
  const writeFileMock = vi.fn().mockResolvedValue(undefined)

  return {
    writeEvent: writeEventMock,
    writeArtifactData: writeArtifactDataMock,
    writeFile: writeFileMock,
    writeEventMock,
    writeArtifactDataMock,
    writeFileMock
  }
}

/**
 * Create a minimal event envelope for testing.
 */
function makeEnvelope<T extends string>(
  type: T,
  payload: Record<string, unknown>,
  overrides: Partial<EventEnvelope> = {}
): EventEnvelope {
  return {
    contract_version: '0.1.0',
    event_id: 'evt-123' as EventId,
    run_id: 'run-456' as RunId,
    seq: 1,
    type,
    ts: '2024-01-01T00:00:00.000Z',
    payload,
    attempt: 1,
    ...overrides
  } as EventEnvelope
}

describe('ObservingSink', () => {
  let mockSink: ReturnType<typeof createMockSink>
  let observingSink: ObservingSink

  beforeEach(() => {
    mockSink = createMockSink()
    observingSink = new ObservingSink(mockSink)
  })

  describe('initial state', () => {
    it('has no terminal state initially', () => {
      expect(observingSink.getTerminalState()).toBeNull()
    })

    it('is not failed initially', () => {
      expect(observingSink.isSinkFailed()).toBe(false)
      expect(observingSink.getSinkFailure()).toBeNull()
    })
  })

  describe('writeEvent delegation', () => {
    it('delegates to inner sink', async () => {
      const envelope = makeEnvelope('item', { item_type: 'test', data: {} })
      await observingSink.writeEvent(envelope)

      expect(mockSink.writeEventMock).toHaveBeenCalledWith(envelope)
    })

    it('does not track non-terminal events', async () => {
      await observingSink.writeEvent(makeEnvelope('item', { item_type: 'test', data: {} }))
      await observingSink.writeEvent(makeEnvelope('log', { level: 'info', message: 'test' }))
      await observingSink.writeEvent(makeEnvelope('checkpoint', { checkpoint_id: 'cp-1' }))

      expect(observingSink.getTerminalState()).toBeNull()
    })
  })

  describe('writeArtifactData delegation', () => {
    it('delegates to inner sink', async () => {
      const artifactId = 'artifact-123' as ArtifactId
      const data = Buffer.from([1, 2, 3])

      await observingSink.writeArtifactData(artifactId, data)

      expect(mockSink.writeArtifactDataMock).toHaveBeenCalledWith(artifactId, data)
    })
  })

  describe('first terminal wins', () => {
    it('tracks run_complete terminal event', async () => {
      const envelope = makeEnvelope('run_complete', { summary: { items: 10 } })
      await observingSink.writeEvent(envelope)

      const state = observingSink.getTerminalState()
      expect(state).not.toBeNull()
      expect(state!.type).toBe('run_complete')
      expect(state!.summary).toEqual({ items: 10 })
    })

    it('tracks run_error terminal event', async () => {
      const envelope = makeEnvelope('run_error', {
        error_type: 'script_error',
        message: 'Something failed'
      })
      await observingSink.writeEvent(envelope)

      const state = observingSink.getTerminalState()
      expect(state).not.toBeNull()
      expect(state!.type).toBe('run_error')
      if (state!.type === 'run_error') {
        expect(state!.errorType).toBe('script_error')
        expect(state!.message).toBe('Something failed')
      }
    })

    it('ignores subsequent terminal events after run_complete', async () => {
      await observingSink.writeEvent(makeEnvelope('run_complete', { summary: { first: true } }))
      await observingSink.writeEvent(
        makeEnvelope('run_error', { error_type: 'err', message: 'msg' })
      )

      const state = observingSink.getTerminalState()
      expect(state!.type).toBe('run_complete')
      expect(state!.summary).toEqual({ first: true })
    })

    it('ignores subsequent terminal events after run_error', async () => {
      await observingSink.writeEvent(
        makeEnvelope('run_error', { error_type: 'first_error', message: 'first' })
      )
      await observingSink.writeEvent(makeEnvelope('run_complete', { summary: { second: true } }))

      const state = observingSink.getTerminalState()
      expect(state!.type).toBe('run_error')
      if (state!.type === 'run_error') {
        expect(state!.errorType).toBe('first_error')
      }
    })

    it('still writes subsequent terminal events to inner sink', async () => {
      const first = makeEnvelope('run_complete', {})
      const second = makeEnvelope('run_error', { error_type: 'err', message: 'msg' })

      await observingSink.writeEvent(first)
      await observingSink.writeEvent(second)

      expect(mockSink.writeEventMock).toHaveBeenCalledTimes(2)
      expect(mockSink.writeEventMock).toHaveBeenNthCalledWith(1, first)
      expect(mockSink.writeEventMock).toHaveBeenNthCalledWith(2, second)
    })
  })

  describe('first failure wins', () => {
    it('tracks sink failure on writeEvent', async () => {
      const error = new Error('Stream closed')
      mockSink.writeEventMock.mockRejectedValueOnce(error)

      await expect(
        observingSink.writeEvent(makeEnvelope('item', { item_type: 'test', data: {} }))
      ).rejects.toThrow('Stream closed')

      expect(observingSink.isSinkFailed()).toBe(true)
      expect(observingSink.getSinkFailure()).toBe(error)
    })

    it('tracks sink failure on writeArtifactData', async () => {
      const error = new Error('Write failed')
      mockSink.writeArtifactDataMock.mockRejectedValueOnce(error)

      await expect(
        observingSink.writeArtifactData('artifact-123' as ArtifactId, Buffer.from([1]))
      ).rejects.toThrow('Write failed')

      expect(observingSink.isSinkFailed()).toBe(true)
      expect(observingSink.getSinkFailure()).toBe(error)
    })

    it('preserves first failure cause', async () => {
      const firstError = new Error('First failure')
      const _secondError = new Error('Second failure')

      mockSink.writeEventMock.mockRejectedValueOnce(firstError)

      // First failure
      await expect(
        observingSink.writeEvent(makeEnvelope('item', { item_type: 'test', data: {} }))
      ).rejects.toThrow('First failure')

      // Second call throws SinkAlreadyFailedError, but original cause is preserved
      await expect(
        observingSink.writeEvent(makeEnvelope('item', { item_type: 'test', data: {} }))
      ).rejects.toThrow(SinkAlreadyFailedError)

      expect(observingSink.getSinkFailure()).toBe(firstError)
    })
  })

  describe('fail-fast after failure', () => {
    it('throws SinkAlreadyFailedError on writeEvent after failure', async () => {
      const originalError = new Error('Original failure')
      mockSink.writeEventMock.mockRejectedValueOnce(originalError)

      // Trigger failure
      await expect(
        observingSink.writeEvent(makeEnvelope('item', { item_type: 'test', data: {} }))
      ).rejects.toThrow()

      // Subsequent call should fail-fast
      await expect(
        observingSink.writeEvent(makeEnvelope('item', { item_type: 'test', data: {} }))
      ).rejects.toThrow(SinkAlreadyFailedError)

      // Inner sink should only be called once (not on fail-fast)
      expect(mockSink.writeEventMock).toHaveBeenCalledTimes(1)
    })

    it('throws SinkAlreadyFailedError on writeArtifactData after failure', async () => {
      const originalError = new Error('Original failure')
      mockSink.writeEventMock.mockRejectedValueOnce(originalError)

      // Trigger failure via writeEvent
      await expect(
        observingSink.writeEvent(makeEnvelope('item', { item_type: 'test', data: {} }))
      ).rejects.toThrow()

      // writeArtifactData should also fail-fast
      await expect(
        observingSink.writeArtifactData('artifact-123' as ArtifactId, Buffer.from([1]))
      ).rejects.toThrow(SinkAlreadyFailedError)

      expect(mockSink.writeArtifactDataMock).not.toHaveBeenCalled()
    })

    it('SinkAlreadyFailedError includes original cause', async () => {
      const originalError = new Error('Original failure')
      mockSink.writeEventMock.mockRejectedValueOnce(originalError)

      await expect(
        observingSink.writeEvent(makeEnvelope('item', { item_type: 'test', data: {} }))
      ).rejects.toThrow()

      try {
        await observingSink.writeEvent(makeEnvelope('item', { item_type: 'test', data: {} }))
        expect.fail('Should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(SinkAlreadyFailedError)
        expect((err as SinkAlreadyFailedError).cause).toBe(originalError)
      }
    })
  })

  describe('sink failure after terminal event', () => {
    it('records both terminal state and sink failure', async () => {
      // Write terminal successfully
      await observingSink.writeEvent(makeEnvelope('run_complete', { summary: { done: true } }))

      // Then fail on subsequent write
      const error = new Error('Late failure')
      mockSink.writeArtifactDataMock.mockRejectedValueOnce(error)

      await expect(
        observingSink.writeArtifactData('artifact-123' as ArtifactId, Buffer.from([1]))
      ).rejects.toThrow('Late failure')

      // Both states should be set
      expect(observingSink.getTerminalState()).not.toBeNull()
      expect(observingSink.getTerminalState()!.type).toBe('run_complete')
      expect(observingSink.isSinkFailed()).toBe(true)
      expect(observingSink.getSinkFailure()).toBe(error)
    })
  })

  describe('best-effort payload extraction', () => {
    it('handles run_error with missing error_type', async () => {
      const envelope = makeEnvelope('run_error', { message: 'just message' })
      await observingSink.writeEvent(envelope)

      const state = observingSink.getTerminalState()
      expect(state!.type).toBe('run_error')
      if (state!.type === 'run_error') {
        expect(state!.errorType).toBeUndefined()
        expect(state!.message).toBe('just message')
      }
    })

    it('handles run_error with missing message', async () => {
      const envelope = makeEnvelope('run_error', { error_type: 'type_only' })
      await observingSink.writeEvent(envelope)

      const state = observingSink.getTerminalState()
      expect(state!.type).toBe('run_error')
      if (state!.type === 'run_error') {
        expect(state!.errorType).toBe('type_only')
        expect(state!.message).toBeUndefined()
      }
    })

    it('handles run_error with completely malformed payload', async () => {
      const envelope = makeEnvelope('run_error', {})
      await observingSink.writeEvent(envelope)

      const state = observingSink.getTerminalState()
      // Type alone should still be captured
      expect(state!.type).toBe('run_error')
    })

    it('handles run_complete with missing summary', async () => {
      const envelope = makeEnvelope('run_complete', {})
      await observingSink.writeEvent(envelope)

      const state = observingSink.getTerminalState()
      expect(state!.type).toBe('run_complete')
      expect(state!.summary).toBeUndefined()
    })

    it('handles run_complete with non-object summary', async () => {
      // summary is not a plain object
      const envelope = makeEnvelope('run_complete', { summary: 'not an object' })
      await observingSink.writeEvent(envelope)

      const state = observingSink.getTerminalState()
      expect(state!.type).toBe('run_complete')
      // Non-object summary should be ignored
      expect(state!.summary).toBeUndefined()
    })
  })
})
