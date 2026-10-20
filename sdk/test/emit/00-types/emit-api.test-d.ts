/**
 * Type-level contract tests for EmitAPI.
 *
 * Goal: Prevent invalid Emit usage from compiling.
 * Invariant: If it compiles, it is a valid Emit call.
 *
 * Checked at compile time by `vitest --typecheck` (expectTypeOf).
 */
import { describe, expectTypeOf, test } from 'vitest'
import type { EmitAPI } from '../../../src/emit'
import type { ArtifactId, CheckpointId } from '../../../src/types/events'

declare const emit: EmitAPI

describe('EmitAPI return types', () => {
  test('item() returns Promise<void>', () => {
    expectTypeOf(emit.item({ item_type: 'product', data: {} })).toEqualTypeOf<Promise<void>>()
  })

  test('artifact() returns Promise<ArtifactId>', () => {
    expectTypeOf(
      emit.artifact({ name: 'screenshot.png', content_type: 'image/png', data: Buffer.from('') })
    ).toEqualTypeOf<Promise<ArtifactId>>()
  })

  test('checkpoint() returns Promise<void>', () => {
    expectTypeOf(emit.checkpoint({ checkpoint_id: 'cp-1' as CheckpointId })).toEqualTypeOf<
      Promise<void>
    >()
  })

  test('enqueue() returns Promise<void>', () => {
    expectTypeOf(emit.enqueue({ target: 'next-page', params: {} })).toEqualTypeOf<Promise<void>>()
  })

  test('rotateProxy() returns Promise<void>', () => {
    expectTypeOf(emit.rotateProxy()).toEqualTypeOf<Promise<void>>()
    expectTypeOf(emit.rotateProxy({ reason: 'rate-limited' })).toEqualTypeOf<Promise<void>>()
  })

  test('log() and convenience log methods return Promise<void>', () => {
    expectTypeOf(emit.log({ level: 'info', message: 'test' })).toEqualTypeOf<Promise<void>>()
    expectTypeOf(emit.debug('test')).toEqualTypeOf<Promise<void>>()
    expectTypeOf(emit.info('test')).toEqualTypeOf<Promise<void>>()
    expectTypeOf(emit.warn('test')).toEqualTypeOf<Promise<void>>()
    expectTypeOf(emit.error('test')).toEqualTypeOf<Promise<void>>()
  })

  test('runError() returns Promise<void>', () => {
    expectTypeOf(emit.runError({ error_type: 'script_error', message: 'failed' })).toEqualTypeOf<
      Promise<void>
    >()
  })

  test('runComplete() returns Promise<void>', () => {
    expectTypeOf(emit.runComplete()).toEqualTypeOf<Promise<void>>()
    expectTypeOf(emit.runComplete({ summary: { items: 10 } })).toEqualTypeOf<Promise<void>>()
  })
})

describe('EmitAPI optional fields', () => {
  test('checkpoint note is optional', () => {
    expectTypeOf(emit.checkpoint({ checkpoint_id: 'cp-1' as CheckpointId })).toEqualTypeOf<
      Promise<void>
    >()
    expectTypeOf(
      emit.checkpoint({ checkpoint_id: 'cp-1' as CheckpointId, note: 'progress' })
    ).toEqualTypeOf<Promise<void>>()
  })

  test('log fields is optional', () => {
    expectTypeOf(emit.log({ level: 'info', message: 'test' })).toEqualTypeOf<Promise<void>>()
    expectTypeOf(
      emit.log({ level: 'info', message: 'test', fields: { key: 'value' } })
    ).toEqualTypeOf<Promise<void>>()
  })

  test('runError stack is optional', () => {
    expectTypeOf(emit.runError({ error_type: 'error', message: 'msg' })).toEqualTypeOf<
      Promise<void>
    >()
    expectTypeOf(
      emit.runError({ error_type: 'error', message: 'msg', stack: 'trace' })
    ).toEqualTypeOf<Promise<void>>()
  })

  test('runComplete summary is optional', () => {
    expectTypeOf(emit.runComplete()).toEqualTypeOf<Promise<void>>()
    expectTypeOf(emit.runComplete({})).toEqualTypeOf<Promise<void>>()
    expectTypeOf(emit.runComplete({ summary: { count: 1 } })).toEqualTypeOf<Promise<void>>()
  })

  test('rotateProxy reason is optional', () => {
    expectTypeOf(emit.rotateProxy()).toEqualTypeOf<Promise<void>>()
    expectTypeOf(emit.rotateProxy({})).toEqualTypeOf<Promise<void>>()
    expectTypeOf(emit.rotateProxy({ reason: 'blocked' })).toEqualTypeOf<Promise<void>>()
  })
})

describe('EmitAPI artifact data', () => {
  test('Buffer is accepted', () => {
    expectTypeOf(
      emit.artifact({
        name: 'test',
        content_type: 'application/octet-stream',
        data: Buffer.from([1, 2, 3])
      })
    ).toEqualTypeOf<Promise<ArtifactId>>()
  })

  test('Uint8Array is accepted', () => {
    expectTypeOf(
      emit.artifact({
        name: 'test',
        content_type: 'application/octet-stream',
        data: new Uint8Array([1, 2, 3])
      })
    ).toEqualTypeOf<Promise<ArtifactId>>()
  })
})

test('EmitAPI is assignable to the item-emitter shape', () => {
  expectTypeOf(emit).toExtend<{
    readonly item: (options: { item_type: string; data: Record<string, unknown> }) => Promise<void>
  }>()
})
