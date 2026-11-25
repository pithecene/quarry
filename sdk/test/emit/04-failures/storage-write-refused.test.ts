/**
 * Failure semantics tests for a refused sidecar file.
 *
 * Goal: A file the runtime refuses rejects storage.put() and nothing else
 * (CONTRACT_IPC.md §File Write Acknowledgement).
 * Invariant: A transport failure still fails every later call.
 */
import { describe, expect, it } from 'vitest'
import { StorageWriteRefusedError } from '../../../src/emit'
import { createAPIs, SinkFailedError } from '../../../src/emit-impl'
import { createRunMeta, FakeSink } from '../_harness'

const file = (filename: string) => ({
  filename,
  content_type: 'text/plain',
  data: Buffer.from('data')
})

/** A sink whose first file write fails. */
function failingSink(error: Error): FakeSink {
  const sink = new FakeSink()
  const write = sink.writeFile.bind(sink)
  let failed = false
  sink.writeFile = async (...args) => {
    if (failed) return write(...args)
    failed = true
    throw error
  }
  return sink
}

describe('refused sidecar file semantics', () => {
  it('rejects the put, and later calls succeed', async () => {
    const refusal = new StorageWriteRefusedError('path already exists')
    const sink = failingSink(refusal)
    const { emit, storage } = createAPIs(createRunMeta(), sink)

    const first = storage.put(file('a.txt'))
    const queued = emit.item({ item_type: 'queued', data: {} })
    await expect(first).rejects.toBe(refusal)
    await expect(queued).resolves.toBeUndefined()
    await expect(storage.put(file('b.txt'))).resolves.toBeDefined()
    await expect(emit.runComplete()).resolves.toBeUndefined()
    expect(sink.envelopes.map((e) => e.type)).toEqual(['item', 'run_complete'])
  })

  it('a transport failure fails every later call', async () => {
    const { emit, storage } = createAPIs(createRunMeta(), failingSink(new Error('stdin closed')))

    await expect(storage.put(file('a.txt'))).rejects.toThrow('stdin closed')
    await expect(emit.item({ item_type: 'x', data: {} })).rejects.toThrow(SinkFailedError)
  })
})
