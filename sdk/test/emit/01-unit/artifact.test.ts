/**
 * Unit tests for emit.artifact() envelope correctness.
 *
 * Goal: Validate envelope construction for single artifact emits.
 *
 * Constraints:
 * - No concurrency
 * - No failures
 * - No terminal events
 */
import { beforeEach, describe, expect, it } from 'vitest'
import { createEmitAPI } from '../../../src/emit-impl'
import type { ArtifactId } from '../../../src/types/events'
import { CONTRACT_VERSION } from '../../../src/types/events'
import { createRunMeta, FakeSink, validateEnvelope } from '../_harness'

describe('emit.artifact() envelope correctness', () => {
  let sink: FakeSink
  let run: ReturnType<typeof createRunMeta>

  beforeEach(() => {
    sink = new FakeSink()
    run = createRunMeta()
  })

  it('creates envelope with type === "artifact"', async () => {
    const emit = createEmitAPI(run, sink)

    await emit.artifact({
      name: 'screenshot.png',
      content_type: 'image/png',
      data: Buffer.from('image data')
    })

    expect(sink.envelopes[0].type).toBe('artifact')
  })

  it('returns generated artifact_id', async () => {
    const emit = createEmitAPI(run, sink)

    const artifactId = await emit.artifact({
      name: 'test.txt',
      content_type: 'text/plain',
      data: Buffer.from('hello')
    })

    expect(typeof artifactId).toBe('string')
    expect(artifactId.length).toBeGreaterThan(0)
  })

  it('includes artifact_id in payload', async () => {
    const emit = createEmitAPI(run, sink)

    const artifactId = await emit.artifact({
      name: 'test.txt',
      content_type: 'text/plain',
      data: Buffer.from('hello')
    })

    expect(sink.envelopes[0].payload).toMatchObject({ artifact_id: artifactId })
  })

  it('includes name in payload', async () => {
    const emit = createEmitAPI(run, sink)

    await emit.artifact({
      name: 'my-screenshot.png',
      content_type: 'image/png',
      data: Buffer.from('')
    })

    expect(sink.envelopes[0].payload).toMatchObject({ name: 'my-screenshot.png' })
  })

  it('includes content_type in payload', async () => {
    const emit = createEmitAPI(run, sink)

    await emit.artifact({
      name: 'doc.pdf',
      content_type: 'application/pdf',
      data: Buffer.from('')
    })

    expect(sink.envelopes[0].payload).toMatchObject({ content_type: 'application/pdf' })
  })

  it('includes size_bytes === data.byteLength', async () => {
    const emit = createEmitAPI(run, sink)
    const data = Buffer.from('hello world')

    await emit.artifact({
      name: 'test.txt',
      content_type: 'text/plain',
      data
    })

    expect(sink.envelopes[0].payload).toMatchObject({ size_bytes: data.byteLength })
    expect(sink.envelopes[0].payload).toMatchObject({ size_bytes: 11 })
  })

  it('works with Uint8Array data', async () => {
    const emit = createEmitAPI(run, sink)
    const data = new Uint8Array([1, 2, 3, 4, 5])

    await emit.artifact({
      name: 'binary.bin',
      content_type: 'application/octet-stream',
      data
    })

    expect(sink.envelopes[0].payload).toMatchObject({ size_bytes: 5 })
  })

  it('works with empty data', async () => {
    const emit = createEmitAPI(run, sink)

    await emit.artifact({
      name: 'empty.txt',
      content_type: 'text/plain',
      data: Buffer.from('')
    })

    expect(sink.envelopes[0].payload).toMatchObject({ size_bytes: 0 })
  })

  it('writes artifact data before artifact event', async () => {
    const emit = createEmitAPI(run, sink)

    await emit.artifact({
      name: 'test.txt',
      content_type: 'text/plain',
      data: Buffer.from('content')
    })

    // Check call order
    expect(sink.allCalls.map((c) => c.kind)).toEqual([
      'openArtifact',
      'writeArtifactRange',
      'writeEvent'
    ])
  })

  it('writes artifact data with correct artifact_id', async () => {
    const emit = createEmitAPI(run, sink)

    const artifactId = await emit.artifact({
      name: 'test.txt',
      content_type: 'text/plain',
      data: Buffer.from('content')
    })

    expect(sink.artifactOpenCalls[0].artifactId).toBe(artifactId)
    expect(sink.artifactRangeCalls[0].artifactId).toBe(artifactId)
  })

  it('writes artifact data with correct bytes', async () => {
    const emit = createEmitAPI(run, sink)
    const data = Buffer.from('test content')

    await emit.artifact({
      name: 'test.txt',
      content_type: 'text/plain',
      data
    })

    expect(Buffer.from(sink.artifactRangeCalls[0].data).toString()).toBe('test content')
  })

  it('generates unique artifact_id for each artifact', async () => {
    const emit = createEmitAPI(run, sink)

    const id1 = await emit.artifact({
      name: 'a.txt',
      content_type: 'text/plain',
      data: Buffer.from('a')
    })
    const id2 = await emit.artifact({
      name: 'b.txt',
      content_type: 'text/plain',
      data: Buffer.from('b')
    })

    expect(id1).not.toBe(id2)
  })

  it('passes full envelope validation', async () => {
    const emit = createEmitAPI(run, sink)

    await emit.artifact({
      name: 'test.pdf',
      content_type: 'application/pdf',
      data: Buffer.from('PDF content')
    })

    const errors = validateEnvelope(sink.envelopes[0])
    expect(errors).toEqual([])
  })
})
