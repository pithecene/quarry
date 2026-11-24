/**
 * Unit tests for createStorageBatcher utility.
 *
 * Goal: Validate bounded-concurrency dispatch, pending counter,
 * flush semantics, fail-fast, and edge cases.
 */
import { describe, expect, it } from 'vitest'
import type { StorageAPI, StoragePutOptions, StoragePutResult } from '../../../src/emit'
import { createStorageBatcher } from '../../../src/storage-batcher'

/**
 * Mock StorageAPI that records put() calls and supports:
 * - Configurable delay per call
 * - Failure injection on Nth call
 * - Tracking concurrent inflight count
 */
type MockStorageOptions = {
  delayMs?: number
  failOnCall?: number
  failureError?: Error
}

function createMockStorage(opts: MockStorageOptions = {}): {
  storage: Pick<StorageAPI, 'put'>
  calls: StoragePutOptions[]
  maxConcurrent: () => number
} {
  const calls: StoragePutOptions[] = []
  let callCount = 0
  let inflight = 0
  let maxInflight = 0

  const storage: Pick<StorageAPI, 'put'> = {
    async put(options: StoragePutOptions): Promise<StoragePutResult> {
      callCount++
      inflight++
      if (inflight > maxInflight) maxInflight = inflight

      const currentCall = callCount

      if (opts.delayMs) {
        await new Promise((resolve) => setTimeout(resolve, opts.delayMs))
      }

      if (opts.failOnCall && currentCall === opts.failOnCall) {
        inflight--
        throw opts.failureError ?? new Error('Injected storage failure')
      }

      calls.push(options)
      inflight--
      return { key: `key/${options.filename}` }
    }
  }

  return { storage, calls, maxConcurrent: () => maxInflight }
}

describe('createStorageBatcher', () => {
  // ── Basic dispatch ──────────────────────────────────────────────

  describe('basic dispatch', () => {
    it('dispatches all added files via storage.put()', async () => {
      const { storage, calls } = createMockStorage()
      const batcher = createStorageBatcher(storage)

      batcher.add({ filename: 'a.png', content_type: 'image/png', data: Buffer.from('A') })
      batcher.add({ filename: 'b.png', content_type: 'image/png', data: Buffer.from('B') })
      batcher.add({ filename: 'c.png', content_type: 'image/png', data: Buffer.from('C') })

      await batcher.flush()

      expect(calls).toHaveLength(3)
      expect(calls.map((c) => c.filename)).toEqual(['a.png', 'b.png', 'c.png'])
    })

    it('returns PendingStoragePut with result promise', async () => {
      const { storage } = createMockStorage()
      const batcher = createStorageBatcher(storage)

      const pending = batcher.add({
        filename: 'test.json',
        content_type: 'application/json',
        data: Buffer.from('{}')
      })

      const result = await pending.result
      expect(result).toEqual({ key: 'key/test.json' })
    })

    it('flush on empty batcher is a no-op', async () => {
      const { storage, calls } = createMockStorage()
      const batcher = createStorageBatcher(storage)

      await batcher.flush()

      expect(calls).toHaveLength(0)
    })

    it('allows continued use after flush', async () => {
      const { storage, calls } = createMockStorage()
      const batcher = createStorageBatcher(storage)

      batcher.add({ filename: 'a.png', content_type: 'image/png', data: Buffer.from('A') })
      await batcher.flush()

      batcher.add({ filename: 'b.png', content_type: 'image/png', data: Buffer.from('B') })
      await batcher.flush()

      expect(calls).toHaveLength(2)
    })
  })

  // ── Concurrency bounding ────────────────────────────────────────

  describe('concurrency bounding', () => {
    it('bounds inflight calls to concurrency limit', async () => {
      const { storage, maxConcurrent } = createMockStorage({ delayMs: 10 })
      const batcher = createStorageBatcher(storage, { concurrency: 4 })

      for (let i = 0; i < 20; i++) {
        batcher.add({
          filename: `file-${i}.png`,
          content_type: 'image/png',
          data: Buffer.from(`${i}`)
        })
      }

      await batcher.flush()

      expect(maxConcurrent()).toBeLessThanOrEqual(4)
    })

    it('concurrency=1 processes sequentially', async () => {
      const { storage, maxConcurrent } = createMockStorage({ delayMs: 5 })
      const batcher = createStorageBatcher(storage, { concurrency: 1 })

      for (let i = 0; i < 5; i++) {
        batcher.add({
          filename: `file-${i}.png`,
          content_type: 'image/png',
          data: Buffer.from(`${i}`)
        })
      }

      await batcher.flush()

      expect(maxConcurrent()).toBe(1)
    })

    it('default concurrency is 16', async () => {
      const { storage, maxConcurrent } = createMockStorage({ delayMs: 10 })
      const batcher = createStorageBatcher(storage)

      for (let i = 0; i < 32; i++) {
        batcher.add({
          filename: `file-${i}.png`,
          content_type: 'image/png',
          data: Buffer.from(`${i}`)
        })
      }

      await batcher.flush()

      expect(maxConcurrent()).toBeLessThanOrEqual(16)
      // With 32 files and default concurrency 16, at least some concurrency
      expect(maxConcurrent()).toBeGreaterThan(1)
    })
  })

  // ── Pending counter ─────────────────────────────────────────────

  describe('pending counter', () => {
    it('starts at 0', () => {
      const { storage } = createMockStorage()
      const batcher = createStorageBatcher(storage)

      expect(batcher.pending).toBe(0)
    })

    it('increments on add', () => {
      const { storage } = createMockStorage({ delayMs: 100 })
      const batcher = createStorageBatcher(storage)

      batcher.add({ filename: 'a.png', content_type: 'image/png', data: Buffer.from('A') })
      expect(batcher.pending).toBe(1)

      batcher.add({ filename: 'b.png', content_type: 'image/png', data: Buffer.from('B') })
      expect(batcher.pending).toBe(2)
    })

    it('returns to 0 after flush', async () => {
      const { storage } = createMockStorage()
      const batcher = createStorageBatcher(storage)

      batcher.add({ filename: 'a.png', content_type: 'image/png', data: Buffer.from('A') })
      batcher.add({ filename: 'b.png', content_type: 'image/png', data: Buffer.from('B') })
      await batcher.flush()

      expect(batcher.pending).toBe(0)
    })
  })

  // ── Flush semantics ─────────────────────────────────────────────

  describe('flush semantics', () => {
    it('waits for all inflight writes', async () => {
      const { storage, calls } = createMockStorage({ delayMs: 10 })
      const batcher = createStorageBatcher(storage, { concurrency: 2 })

      for (let i = 0; i < 6; i++) {
        batcher.add({
          filename: `file-${i}.png`,
          content_type: 'image/png',
          data: Buffer.from(`${i}`)
        })
      }

      await batcher.flush()

      expect(calls).toHaveLength(6)
    })

    it('multiple empty flushes are no-ops', async () => {
      const { storage, calls } = createMockStorage()
      const batcher = createStorageBatcher(storage)

      await batcher.flush()
      await batcher.flush()
      await batcher.flush()

      expect(calls).toHaveLength(0)
    })
  })

  // ── Fail-fast ───────────────────────────────────────────────────

  describe('fail-fast', () => {
    it('first put error causes flush to throw', async () => {
      const { storage } = createMockStorage({
        failOnCall: 2,
        failureError: new Error('disk full')
      })
      const batcher = createStorageBatcher(storage, { concurrency: 1 })

      // Catch result promises to prevent unhandled rejections
      const p1 = batcher.add({
        filename: 'a.png',
        content_type: 'image/png',
        data: Buffer.from('A')
      })
      const p2 = batcher.add({
        filename: 'b.png',
        content_type: 'image/png',
        data: Buffer.from('B')
      })
      p1.result.catch(() => {})
      p2.result.catch(() => {})

      await expect(batcher.flush()).rejects.toThrow('disk full')
    })

    it('subsequent add after failure rejects immediately', async () => {
      const { storage } = createMockStorage({
        failOnCall: 1,
        failureError: new Error('fail')
      })
      const batcher = createStorageBatcher(storage, { concurrency: 1 })

      // Catch result promise to prevent unhandled rejection
      const p1 = batcher.add({
        filename: 'a.png',
        content_type: 'image/png',
        data: Buffer.from('A')
      })
      p1.result.catch(() => {})

      // Wait for the failure to propagate
      await expect(batcher.flush()).rejects.toThrow('fail')

      // Subsequent add should reject immediately
      const pending = batcher.add({
        filename: 'b.png',
        content_type: 'image/png',
        data: Buffer.from('B')
      })
      await expect(pending.result).rejects.toThrow('fail')
    })

    it('queued items are rejected when an inflight item fails', async () => {
      const { storage } = createMockStorage({
        delayMs: 5,
        failOnCall: 1,
        failureError: new Error('boom')
      })
      const batcher = createStorageBatcher(storage, { concurrency: 1 })

      const p1 = batcher.add({
        filename: 'a.png',
        content_type: 'image/png',
        data: Buffer.from('A')
      })
      const p2 = batcher.add({
        filename: 'b.png',
        content_type: 'image/png',
        data: Buffer.from('B')
      })

      await expect(p1.result).rejects.toThrow('boom')
      await expect(p2.result).rejects.toThrow('boom')
    })

    it('flush waits for all in-flight writes to settle before rejecting (concurrency > 1)', async () => {
      const { storage } = createMockStorage({
        delayMs: 20,
        failOnCall: 1,
        failureError: new Error('first fails')
      })
      const batcher = createStorageBatcher(storage, { concurrency: 4 })

      // Dispatch 4 concurrent writes; first will fail, other 3 are in-flight
      for (let i = 0; i < 4; i++) {
        const p = batcher.add({
          filename: `file-${i}.png`,
          content_type: 'image/png',
          data: Buffer.from(`${i}`)
        })
        p.result.catch(() => {})
      }

      // flush() must wait for ALL in-flight writes to settle, then reject
      await expect(batcher.flush()).rejects.toThrow('first fails')

      // After flush rejection, pending must be 0 — all writes settled
      expect(batcher.pending).toBe(0)
    })

    it('pending settles to 0 after failure with queued items', async () => {
      const { storage } = createMockStorage({
        delayMs: 5,
        failOnCall: 1,
        failureError: new Error('fail')
      })
      const batcher = createStorageBatcher(storage, { concurrency: 1 })

      const p1 = batcher.add({
        filename: 'a.png',
        content_type: 'image/png',
        data: Buffer.from('A')
      })
      const p2 = batcher.add({
        filename: 'b.png',
        content_type: 'image/png',
        data: Buffer.from('B')
      })

      // Wait for both rejections
      await expect(p1.result).rejects.toThrow('fail')
      await expect(p2.result).rejects.toThrow('fail')

      expect(batcher.pending).toBe(0)
    })
  })

  // ── Edge cases ──────────────────────────────────────────────────

  describe('edge cases', () => {
    it('concurrency < 1 throws RangeError', () => {
      const { storage } = createMockStorage()

      expect(() => createStorageBatcher(storage, { concurrency: 0 })).toThrow(RangeError)
      expect(() => createStorageBatcher(storage, { concurrency: -1 })).toThrow(RangeError)
    })

    it('fractional concurrency throws RangeError', () => {
      const { storage } = createMockStorage()

      expect(() => createStorageBatcher(storage, { concurrency: 1.5 })).toThrow(RangeError)
    })

    it('NaN concurrency throws RangeError', () => {
      const { storage } = createMockStorage()

      expect(() => createStorageBatcher(storage, { concurrency: NaN })).toThrow(RangeError)
    })

    it('Infinity concurrency throws RangeError', () => {
      const { storage } = createMockStorage()

      expect(() => createStorageBatcher(storage, { concurrency: Infinity })).toThrow(RangeError)
    })

    it('large batch (100 files) at concurrency=4 completes', async () => {
      const { storage, calls } = createMockStorage()
      const batcher = createStorageBatcher(storage, { concurrency: 4 })

      for (let i = 0; i < 100; i++) {
        batcher.add({
          filename: `file-${i}.dat`,
          content_type: 'application/octet-stream',
          data: Buffer.from(`data-${i}`)
        })
      }

      await batcher.flush()

      expect(calls).toHaveLength(100)
      expect(batcher.pending).toBe(0)
    })
  })
})

describe('concurrent flush calls', () => {
  it.each([
    ['resolve', undefined],
    ['reject', new Error('refused')]
  ])('all %s when the writes settle', async (_, failure) => {
    let settle = () => {}
    const written = new Promise<void>((resolve) => {
      settle = resolve
    })
    const batcher = createStorageBatcher({
      put: async () => {
        await written
        if (failure) throw failure
        return { key: 'k' }
      }
    })
    batcher
      .add({ filename: 'a', content_type: 'text/plain', data: Buffer.from('a') })
      .result.catch(() => {})
    const flushes = [batcher.flush(), batcher.flush(), batcher.flush()]
    settle()
    const outcome = failure
      ? { status: 'rejected', reason: failure }
      : { status: 'fulfilled', value: undefined }
    expect(await Promise.allSettled(flushes)).toEqual([outcome, outcome, outcome])
  })
})
