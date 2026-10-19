/**
 * Parse optional storage partition metadata from run input.
 *
 * Extracted from the executor entrypoint so it can be tested
 * without triggering main() side-effects.
 *
 * @module
 */
import type { StoragePartitionMeta } from '@pithecene/quarry-sdk'

/**
 * Parse optional storage partition metadata from input.
 * Returns undefined if no storage field is present or null.
 * Calls `onWarning` when the field is present but malformed.
 */
export function parseStoragePartition(
  input: Record<string, unknown>,
  onWarning: (msg: string) => void
): StoragePartitionMeta | undefined {
  if (!('storage' in input) || input.storage === null || input.storage === undefined) {
    return undefined
  }

  if (typeof input.storage !== 'object' || Array.isArray(input.storage)) {
    onWarning(
      `storage partition metadata must be an object, got ${Array.isArray(input.storage) ? 'array' : typeof input.storage}; storage.put() will return empty key`
    )
    return undefined
  }

  const sp = input.storage as Record<string, unknown>
  const requiredFields = ['dataset', 'source', 'category', 'day', 'run_id'] as const
  const missing = requiredFields.filter(
    (f) => typeof sp[f] !== 'string' || (sp[f] as string) === ''
  )
  if (missing.length > 0) {
    onWarning(
      `storage partition metadata present but malformed (missing/empty: ${missing.join(', ')}); storage.put() will return empty key`
    )
    return undefined
  }

  return {
    dataset: sp.dataset as string,
    source: sp.source as string,
    category: sp.category as string,
    day: sp.day as string,
    run_id: sp.run_id as string
  }
}
