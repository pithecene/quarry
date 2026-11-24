import type { StorageAPI, StoragePutOptions, StoragePutResult } from './emit'

/**
 * Options for creating a storage batcher.
 */
export type StorageBatcherOptions = {
  /** Max queued storage.put() calls dispatched concurrently. Default: 16 */
  readonly concurrency?: number
}

/**
 * A pending storage put operation tracked by the batcher.
 */
export type PendingStoragePut = {
  /** Resolves to the storage result when the write completes. */
  readonly result: Promise<StoragePutResult>
}

/**
 * Batches multiple storage.put() calls with bounded concurrency.
 *
 * Each call still flows through the shared serialization chain
 * (back-to-back with zero user-code latency between puts), but the
 * batcher bounds how many promises are in-flight at once to avoid
 * creating N promises for N files.
 */
export type StorageBatcher = {
  /** Queue a file for writing. Dispatches immediately if concurrency permits. */
  readonly add: (options: StoragePutOptions) => PendingStoragePut
  /** Wait for all queued/in-flight writes to complete. Must call before terminal events. */
  readonly flush: () => Promise<void>
  /** Number of writes not yet completed (queued + in-flight). */
  readonly pending: number
}

/**
 * Create a storage batcher that dispatches storage.put() calls with
 * bounded concurrency.
 *
 * Each file write still produces one file_write IPC frame — optimization
 * is SDK-level dispatch only.
 *
 * @param storage - The storage API (typically ctx.storage)
 * @param options - Batcher options (concurrency limit)
 */
export function createStorageBatcher(
  storage: Pick<StorageAPI, 'put'>,
  options?: StorageBatcherOptions
): StorageBatcher {
  const concurrency = options?.concurrency ?? 16
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new RangeError(
      `StorageBatcher concurrency must be a positive integer, got ${concurrency}`
    )
  }

  let inflight = 0
  let failed: unknown = null
  const queue: Array<{
    options: StoragePutOptions
    resolve: (value: StoragePutResult) => void
    reject: (reason: unknown) => void
  }> = []

  // Completed count tracked separately from inflight for pending getter
  let completed = 0
  let totalAdded = 0

  // Flush waiters — resolved when all items have settled
  const flushWaiters: Array<() => void> = []

  function checkFlushComplete(): void {
    if (completed < totalAdded) return
    for (const resolve of flushWaiters.splice(0)) {
      resolve()
    }
  }

  function drain(): void {
    while (inflight < concurrency && queue.length > 0 && failed === null) {
      const entry = queue.shift()
      if (entry === undefined) break
      inflight++
      storage
        .put(entry.options)
        .then((result) => {
          inflight--
          completed++
          entry.resolve(result)
          drain()
          checkFlushComplete()
        })
        .catch((err: unknown) => {
          inflight--
          completed++
          if (failed === null) {
            failed = err
          }
          entry.reject(err)
          // Reject remaining queued entries and count them as completed
          const remaining = queue.splice(0)
          completed += remaining.length
          for (const queued of remaining) {
            queued.reject(err)
          }
          checkFlushComplete()
        })
    }
  }

  return {
    add(opts: StoragePutOptions): PendingStoragePut {
      if (failed !== null) {
        return { result: Promise.reject(failed) }
      }
      totalAdded++
      let resolve!: (value: StoragePutResult) => void
      let reject!: (reason: unknown) => void
      const result = new Promise<StoragePutResult>((res, rej) => {
        resolve = res
        reject = rej
      })
      queue.push({ options: opts, resolve, reject })
      drain()
      return { result }
    },

    async flush(): Promise<void> {
      // Already drained — nothing to wait for
      if (completed >= totalAdded) {
        if (failed !== null) {
          throw failed
        }
        return
      }
      // Wait for all items to settle via promise (no polling)
      await new Promise<void>((resolve) => {
        flushWaiters.push(resolve)
      })
      if (failed !== null) {
        throw failed
      }
    },

    get pending(): number {
      return totalAdded - completed
    }
  }
}
