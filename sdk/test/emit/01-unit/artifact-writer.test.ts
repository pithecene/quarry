/**
 * Unit tests for emit.artifactWriter().
 *
 * Goal: A writer opens the artifact with its declared size, sends each
 * write as ranges at their offsets, and emits the artifact event only once
 * the written ranges cover the artifact. It refuses a call it can judge
 * before sending anything, and the refusal leaves the sink usable.
 */
import { beforeEach, describe, expect, it } from 'vitest'
import {
  ARTIFACT_RANGE_MAX_BYTES,
  ArtifactRangeRefusedError,
  ArtifactWriterError
} from '../../../src/emit'
import { createEmitAPI } from '../../../src/emit-impl'
import { createRunMeta, FakeSink } from '../_harness'

const SHA = '1ded'.repeat(16)

describe('emit.artifactWriter()', () => {
  let run: ReturnType<typeof createRunMeta>
  let sink: FakeSink

  beforeEach(() => {
    run = createRunMeta()
    sink = new FakeSink()
  })

  it('opens the artifact with its size and expected digest', async () => {
    const emit = createEmitAPI(run, sink)
    const writer = await emit.artifactWriter({
      name: 'a.zip',
      content_type: 'application/zip',
      size_bytes: 10,
      sha256: SHA
    })

    expect(sink.artifactOpenCalls).toEqual([
      expect.objectContaining({
        artifactId: writer.artifact_id,
        sizeBytes: 10,
        expectedSha256: SHA
      })
    ])
    expect(sink.envelopes).toHaveLength(0)
  })

  it('writes ranges in any order and commits once they cover the artifact', async () => {
    const emit = createEmitAPI(run, sink)
    const writer = await emit.artifactWriter({
      name: 'a',
      content_type: 'text/plain',
      size_bytes: 6
    })

    await writer.writeAt(3, Buffer.from('def'))
    await writer.writeAt(0, Buffer.from('abc'))
    const id = await writer.commit()

    expect(id).toBe(writer.artifact_id)
    expect(sink.artifactRangeCalls.map((c) => [c.offset, Buffer.from(c.data).toString()])).toEqual([
      [3, 'def'],
      [0, 'abc']
    ])
    expect(sink.envelopes).toHaveLength(1)
    expect(sink.envelopes[0]).toMatchObject({
      type: 'artifact',
      payload: { artifact_id: id, name: 'a', content_type: 'text/plain', size_bytes: 6 }
    })
  })

  it('splits a write longer than a range into ranges at their offsets', async () => {
    const emit = createEmitAPI(run, sink)
    const size = ARTIFACT_RANGE_MAX_BYTES * 2 + 5
    const writer = await emit.artifactWriter({ name: 'big', content_type: 'x/y', size_bytes: size })

    await writer.writeAt(0, Buffer.alloc(size))
    await writer.commit()

    expect(sink.artifactRangeCalls.map((c) => [c.offset, c.data.byteLength])).toEqual([
      [0, ARTIFACT_RANGE_MAX_BYTES],
      [ARTIFACT_RANGE_MAX_BYTES, ARTIFACT_RANGE_MAX_BYTES],
      [ARTIFACT_RANGE_MAX_BYTES * 2, 5]
    ])
  })

  it('keeps the options it opened with when the caller changes them', async () => {
    const emit = createEmitAPI(run, sink)
    const options = { name: 'a', content_type: 'x/y', size_bytes: 2 }
    const writer = await emit.artifactWriter(options)
    options.size_bytes = 4
    options.name = 'changed'

    await expect(writer.writeAt(2, Buffer.from('cd'))).rejects.toBeInstanceOf(ArtifactWriterError)
    await writer.writeAt(0, Buffer.from('ab'))
    await writer.commit()

    expect(sink.envelopes[0]?.payload).toMatchObject({ name: 'a', size_bytes: 2 })
  })

  it('commits an empty artifact with no range', async () => {
    const emit = createEmitAPI(run, sink)
    const writer = await emit.artifactWriter({ name: 'e', content_type: 'x/y', size_bytes: 0 })

    await writer.commit()

    expect(sink.artifactRangeCalls).toHaveLength(0)
    expect(sink.envelopes[0]?.payload).toMatchObject({ size_bytes: 0 })
  })

  describe('refuses before sending anything', () => {
    const invalidOptions = [
      ['a fractional size', { size_bytes: 1.5 }],
      ['a negative size', { size_bytes: -1 }],
      ['a size above 1 GiB', { size_bytes: 1024 * 1024 * 1024 + 1 }],
      ['a digest that is no hex SHA-256', { size_bytes: 1, sha256: 'xyz' }]
    ] as const

    for (const [label, options] of invalidOptions) {
      it(`a writer with ${label}`, async () => {
        const emit = createEmitAPI(run, sink)
        await expect(
          emit.artifactWriter({ name: 'a', content_type: 'x/y', ...options })
        ).rejects.toBeInstanceOf(ArtifactWriterError)
        expect(sink.allCalls).toHaveLength(0)
      })
    }

    const invalidWrites = [
      ['an empty range', 0, 0],
      ['a negative offset', -1, 2],
      ['a fractional offset', 0.5, 2],
      ['a range past the end', 5, 2],
      ['a range overlapping a written one', 2, 2]
    ] as const

    for (const [label, offset, length] of invalidWrites) {
      it(label, async () => {
        const emit = createEmitAPI(run, sink)
        const writer = await emit.artifactWriter({ name: 'a', content_type: 'x/y', size_bytes: 6 })
        await writer.writeAt(0, Buffer.from('abc'))

        await expect(writer.writeAt(offset, Buffer.alloc(length))).rejects.toBeInstanceOf(
          ArtifactWriterError
        )
        expect(sink.artifactRangeCalls).toHaveLength(1)
      })
    }

    it('a commit before the ranges cover the artifact', async () => {
      const emit = createEmitAPI(run, sink)
      const writer = await emit.artifactWriter({ name: 'a', content_type: 'x/y', size_bytes: 6 })
      await writer.writeAt(0, Buffer.from('abc'))

      await expect(writer.commit()).rejects.toBeInstanceOf(ArtifactWriterError)
      expect(sink.envelopes).toHaveLength(0)
    })

    it('a write or a commit after the commit', async () => {
      const emit = createEmitAPI(run, sink)
      const writer = await emit.artifactWriter({ name: 'a', content_type: 'x/y', size_bytes: 1 })
      await writer.writeAt(0, Buffer.from('a'))
      await writer.commit()

      await expect(writer.writeAt(0, Buffer.from('a'))).rejects.toBeInstanceOf(ArtifactWriterError)
      await expect(writer.commit()).rejects.toBeInstanceOf(ArtifactWriterError)
      expect(sink.envelopes).toHaveLength(1)
    })

    it('and leaves the sink usable', async () => {
      const emit = createEmitAPI(run, sink)
      const writer = await emit.artifactWriter({ name: 'a', content_type: 'x/y', size_bytes: 2 })
      await writer.commit().catch(() => {})

      await writer.writeAt(0, Buffer.from('ab'))
      await writer.commit()
      await emit.item({ item_type: 't', data: {} })
      expect(sink.envelopes.map((e) => e.type)).toEqual(['artifact', 'item'])
    })
  })

  it('reports the offset of a refused range and keeps the ranges before it', async () => {
    const refusal = new ArtifactRangeRefusedError('storage failed')
    sink = new FakeSink({ failOnArtifactWrite: 2, failureError: refusal })
    const emit = createEmitAPI(run, sink)
    const size = ARTIFACT_RANGE_MAX_BYTES + 3
    const writer = await emit.artifactWriter({ name: 'a', content_type: 'x/y', size_bytes: size })
    const data = Buffer.alloc(size)

    const rejection = await writer.writeAt(0, data).catch((err: unknown) => err)
    expect(rejection).toBeInstanceOf(ArtifactRangeRefusedError)
    expect(rejection).toMatchObject({ offset: ARTIFACT_RANGE_MAX_BYTES, cause: refusal })

    // The first range is committed, so the script resumes from the offset.
    await writer.writeAt(ARTIFACT_RANGE_MAX_BYTES, data.subarray(ARTIFACT_RANGE_MAX_BYTES))
    await expect(writer.commit()).resolves.toBe(writer.artifact_id)
  })

  it('shares the emit chain with other emits', async () => {
    const emit = createEmitAPI(run, sink)
    const writer = await emit.artifactWriter({ name: 'a', content_type: 'x/y', size_bytes: 1 })

    await Promise.all([
      emit.item({ item_type: 'before', data: {} }),
      writer.writeAt(0, Buffer.from('a')),
      writer.commit(),
      emit.item({ item_type: 'after', data: {} })
    ])

    expect(sink.envelopes.map((e) => e.type)).toEqual(['item', 'artifact', 'item'])
    expect(sink.envelopes.map((e) => e.seq)).toEqual([1, 2, 3])
  })
})
