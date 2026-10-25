import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { describeError, ReleaseError } from './errors.ts'

/**
 * Reads string fields from a package manifest (`package.json` or
 * `jsr.json`) in `dir`, failing if any is missing or not a string.
 */
export const readManifestFields = async <const K extends string>(
  dir: string,
  file: string,
  fields: readonly K[]
): Promise<Record<K, string>> => {
  const path = join(dir, file)
  let parsed: unknown
  try {
    parsed = JSON.parse(await readFile(path, 'utf8'))
  } catch (error) {
    throw new ReleaseError(`could not read ${path}: ${describeError(error)}`)
  }
  if (typeof parsed !== 'object' || parsed === null) {
    throw new ReleaseError(`${path} is not a JSON object`)
  }
  const record = parsed as Record<string, unknown>
  const out = {} as Record<K, string>
  for (const field of fields) {
    const value = record[field]
    if (typeof value !== 'string' || value === '') {
      throw new ReleaseError(`${path} has no "${field}"`)
    }
    out[field] = value
  }
  return out
}
