import { describe, expect, it } from 'vitest'
import type { Context } from '../src/context.ts'
import type { RunResult } from '../src/process.ts'
import { imageProbe } from '../src/registry/image.ts'
import { jsrProbe } from '../src/registry/jsr.ts'
import { npmProbe } from '../src/registry/npm.ts'

/** A context whose every command returns `result` and every fetch `status`. */
const answering = (result: Partial<RunResult>, status: number | Error = 200): Context => ({
  run: async () => ({ code: 0, stdout: '', stderr: '', timedOut: false, ...result }),
  fetch: async () => {
    if (status instanceof Error) throw status
    return { status }
  },
  sleep: async () => {},
  log: () => {}
})

describe('npmProbe', () => {
  it('is present when npm prints the version', async () => {
    expect(await npmProbe(answering({ stdout: '1.2.3\n' }), 'p', '1.2.3')).toEqual({
      kind: 'present'
    })
  })

  it('is absent only on E404', async () => {
    const ctx = answering({ code: 1, stderr: 'npm error code E404\nnpm error 404 No match found' })
    expect(await npmProbe(ctx, 'p', '1.2.3')).toEqual({ kind: 'absent' })
  })

  it.each([
    ['empty output', ''],
    ['another version', '1.2.2\n']
  ])('fails on a successful run with %s', async (_, stdout) => {
    expect((await npmProbe(answering({ stdout }), 'p', '1.2.3')).kind).toBe('failed')
  })

  it('fails on any other error', async () => {
    const ctx = answering({ code: 1, stderr: 'npm error code ECONNREFUSED' })
    expect(await npmProbe(ctx, 'p', '1.2.3')).toMatchObject({
      kind: 'failed',
      reason: /ECONNREFUSED/
    })
  })
})

describe('imageProbe', () => {
  it('is present when the manifest is found', async () => {
    expect(await imageProbe(answering({}), 'r:1')).toEqual({ kind: 'present' })
  })

  it('is absent on "not found"', async () => {
    const ctx = answering({ code: 1, stderr: 'ERROR: r:1: not found' })
    expect(await imageProbe(ctx, 'r:1')).toEqual({ kind: 'absent' })
  })

  it('fails on any other error', async () => {
    const ctx = answering({ code: 1, stderr: 'ERROR: unauthorized' })
    expect((await imageProbe(ctx, 'r:1')).kind).toBe('failed')
  })
})

describe('jsrProbe', () => {
  it.each([
    [200, 'present'],
    [404, 'absent'],
    [500, 'failed'],
    [403, 'failed']
  ] as const)('maps HTTP %i to %s', async (status, kind) => {
    expect((await jsrProbe(answering({}, status), '@s/p', '1.2.3')).kind).toBe(kind)
  })

  it('fails when there is no response', async () => {
    const ctx = answering({}, new TypeError('fetch failed'))
    expect(await jsrProbe(ctx, '@s/p', '1.2.3')).toMatchObject({
      kind: 'failed',
      reason: /fetch failed/
    })
  })
})
