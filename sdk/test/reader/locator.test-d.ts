/**
 * Type tests for the notification locator, checked at compile time by
 * `vitest --typecheck`.
 */
import { describe, expectTypeOf, test } from 'vitest'
import type { NotificationLocator, ReaderStorage, RunLocator } from '../../src/reader'

describe('NotificationLocator', () => {
  test('names its dataset, which a reader may omit', () => {
    expectTypeOf<{ backend: 'fs'; path: string }>().toExtend<ReaderStorage>()
    expectTypeOf<{ backend: 'fs'; path: string }>().not.toExtend<NotificationLocator['storage']>()
  })
  test('is accepted by the reader', () => {
    expectTypeOf<NotificationLocator['run']>().toEqualTypeOf<RunLocator>()
    expectTypeOf<NotificationLocator['storage']>().toExtend<ReaderStorage>()
  })
})
