/**
 * Failure semantics tests for artifact write failures.
 *
 * Goal: Define behavior when writeArtifactRange fails.
 * Invariant: Artifact bytes written before artifact event.
 *            If bytes fail, no event emitted.
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { createEmitAPI, SinkFailedError } from '../../../src/emit-impl'
import { createRunMeta, FakeSink } from '../_harness'

describe('artifact write failure semantics', () => {
  let run: ReturnType<typeof createRunMeta>

  beforeEach(() => {
    run = createRunMeta()
  })

  it('artifact bytes written before artifact event', async () => {
    const sink = new FakeSink()
    const emit = createEmitAPI(run, sink)

    await emit.artifact({
      name: 'test.txt',
      content_type: 'text/plain',
      data: Buffer.from('content')
    })

    // Check order of calls
    expect(sink.allCalls.map((c) => c.kind)).toEqual([
      'openArtifact',
      'writeArtifactRange',
      'writeEvent'
    ])

    // Verify call order (data before event)
    expect(sink.allCalls[1].callIndex).toBeLessThan(sink.allCalls[2].callIndex)
  })

  it('no artifact event if bytes fail', async () => {
    const injectedError = new Error('Storage write failed')
    const sink = new FakeSink({ failOnArtifactWrite: 1, failureError: injectedError })
    const emit = createEmitAPI(run, sink)

    await expect(
      emit.artifact({ name: 'test.txt', content_type: 'text/plain', data: Buffer.from('data') })
    ).rejects.toThrow(injectedError)

    // No event should have been written
    expect(sink.envelopes).toHaveLength(0)
  })

  it('subsequent emits fail with SinkFailedError after artifact bytes fail', async () => {
    const sink = new FakeSink({ failOnArtifactWrite: 1 })
    const emit = createEmitAPI(run, sink)

    // Artifact write fails
    await emit
      .artifact({ name: 'test.txt', content_type: 'text/plain', data: Buffer.from('data') })
      .catch(() => {})

    // Subsequent emits fail with SinkFailedError
    await expect(emit.item({ item_type: 'test', data: {} })).rejects.toThrow(SinkFailedError)
  })

  it('artifact event fails after bytes succeed - orphaned blob scenario', async () => {
    // This tests the case where bytes succeed but the event write fails
    // Per contract: "Event fails after bytes → orphaned blob, eligible for GC"
    const sink = new FakeSink({ failOnEventWrite: 1 })
    const emit = createEmitAPI(run, sink)

    await expect(
      emit.artifact({ name: 'test.txt', content_type: 'text/plain', data: Buffer.from('data') })
    ).rejects.toThrow()

    // Bytes were written
    expect(sink.artifactRangeCalls).toHaveLength(1)
    // But event was not
    expect(sink.envelopes).toHaveLength(0)
  })

  it('no seq increment if artifact bytes fail', async () => {
    const sink = new FakeSink({ failOnArtifactWrite: 1 })
    const emit = createEmitAPI(run, sink)

    // Artifact fails
    await emit
      .artifact({ name: 'fail.txt', content_type: 'text/plain', data: Buffer.from('fail') })
      .catch(() => {})

    // No envelopes, no seq used
    expect(sink.envelopes).toHaveLength(0)
  })

  it('item emit works after sink is created but before artifact failure', async () => {
    const sink = new FakeSink({ failOnArtifactWrite: 1 })
    const emit = createEmitAPI(run, sink)

    // Item succeeds
    await emit.item({ item_type: 'before', data: {} })
    expect(sink.envelopes).toHaveLength(1)
    expect(sink.envelopes[0].seq).toBe(1)

    // Artifact fails
    await emit
      .artifact({ name: 'fail.txt', content_type: 'text/plain', data: Buffer.from('fail') })
      .catch(() => {})

    // No more events after failure
    await expect(emit.item({ item_type: 'after', data: {} })).rejects.toThrow(SinkFailedError)

    // Still only one envelope
    expect(sink.envelopes).toHaveLength(1)
  })

  it('multiple artifacts - second fails', async () => {
    const sink = new FakeSink({ failOnArtifactWrite: 2 })
    const emit = createEmitAPI(run, sink)

    // First artifact succeeds
    const id1 = await emit.artifact({
      name: 'a.txt',
      content_type: 'text/plain',
      data: Buffer.from('a')
    })
    expect(id1).toBeDefined()
    expect(sink.envelopes).toHaveLength(1)

    // Second artifact fails
    await expect(
      emit.artifact({ name: 'b.txt', content_type: 'text/plain', data: Buffer.from('b') })
    ).rejects.toThrow()

    // Still only first artifact persisted
    expect(sink.envelopes).toHaveLength(1)
  })

  it('artifact failure followed by runComplete also fails', async () => {
    const sink = new FakeSink({ failOnArtifactWrite: 1 })
    const emit = createEmitAPI(run, sink)

    // Artifact fails
    await emit
      .artifact({ name: 'test.txt', content_type: 'text/plain', data: Buffer.from('data') })
      .catch(() => {})

    // runComplete also fails
    await expect(emit.runComplete()).rejects.toThrow(SinkFailedError)
  })
})
