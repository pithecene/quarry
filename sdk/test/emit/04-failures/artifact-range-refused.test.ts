/**
 * Failure semantics tests for a refused artifact range.
 *
 * Goal: A range the runtime refuses rejects emit.artifact() and nothing
 * else (CONTRACT_IPC.md §Artifact Range Acknowledgement).
 * Invariant: The sink is not marked failed, so the script can emit on.
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { ArtifactRangeRefusedError } from '../../../src/emit'
import { createEmitAPI } from '../../../src/emit-impl'
import { createRunMeta, FakeSink } from '../_harness'

describe('refused artifact range semantics', () => {
  let run: ReturnType<typeof createRunMeta>

  beforeEach(() => {
    run = createRunMeta()
  })

  it('rejects the artifact with the refusal and no event is emitted', async () => {
    const refusal = new ArtifactRangeRefusedError('artifact range refused: range past size')
    const sink = new FakeSink({ failOnArtifactWrite: 1, failureError: refusal })
    const emit = createEmitAPI(run, sink)

    const rejection = await emit
      .artifact({ name: 'test.txt', content_type: 'text/plain', data: Buffer.from('data') })
      .catch((err: unknown) => err)
    expect(rejection).toBeInstanceOf(ArtifactRangeRefusedError)
    expect(rejection).toMatchObject({ message: refusal.message, offset: 0, cause: refusal })
    expect(sink.envelopes).toHaveLength(0)
  })

  it('later emits succeed after a refused range', async () => {
    const refusal = new ArtifactRangeRefusedError('artifact range refused')
    const sink = new FakeSink({ failOnArtifactWrite: 1, failureError: refusal })
    const emit = createEmitAPI(run, sink)

    await emit
      .artifact({ name: 'test.txt', content_type: 'text/plain', data: Buffer.from('data') })
      .catch(() => {})

    await expect(emit.item({ item_type: 'recovered', data: {} })).resolves.toBeUndefined()
    await expect(
      emit.artifact({ name: 'again.txt', content_type: 'text/plain', data: Buffer.from('again') })
    ).resolves.toBeDefined()
    await expect(emit.runComplete({ summary: {} })).resolves.toBeUndefined()
    expect(sink.envelopes.map((e) => e.type)).toEqual(['item', 'artifact', 'run_complete'])
  })
})
