import type { Context } from '../context.ts'
import { describeError, ReleaseError } from '../errors.ts'
import { lastLine, type RunResult } from '../process.ts'

export type ReleaseState = 'published' | 'draft' | 'prerelease' | 'missing'

/** A release's state with the digest of each complete asset, by name. */
export type Release = {
  readonly state: ReleaseState
  readonly digests: ReadonlyMap<string, string>
}

/** A read that failed says nothing about the release, least of all that it is missing. */
export type ReleaseRead = { readonly release: Release } | { readonly failed: string }

/**
 * Reads the GitHub release for `tag` with `gh` (which needs `GH_TOKEN` in CI).
 * Only gh's own "release not found" answer is `missing`.
 */
export const readRelease = async (ctx: Context, tag: string): Promise<ReleaseRead> => {
  const failed = (why: string) => ({
    failed: `could not read the GitHub release for ${tag}: ${why}`
  })
  const fields = 'isDraft,isPrerelease,assets'
  let result: RunResult
  try {
    result = await ctx.run('gh', ['release', 'view', tag, '--json', fields])
  } catch (error) {
    return failed(describeError(error))
  }
  if (result.code !== 0) {
    return result.stderr.includes('release not found')
      ? { release: { state: 'missing', digests: new Map() } }
      : failed(lastLine(result.stderr))
  }
  const release = parseRelease(result.stdout)
  return release ? { release } : failed(`unexpected output ${lastLine(result.stdout)}`)
}

/** The state of the GitHub release for `tag`; a failed read is an error. */
export const githubReleaseState = async (ctx: Context, tag: string): Promise<ReleaseState> => {
  const read = await readRelease(ctx, tag)
  if ('failed' in read) throw new ReleaseError(read.failed)
  return read.release.state
}

const DIGEST = /^sha256:[0-9a-f]{64}$/i

// Only a complete asset has a digest, and without one nothing can be decided.
type Asset =
  | { name: string; state: 'uploaded'; digest: string }
  | { name: string; state: 'incomplete' }

const parseAsset = (value: unknown): Asset | undefined => {
  const { name, state, digest } = (value ?? {}) as Record<string, unknown>
  if (typeof name !== 'string' || typeof state !== 'string') return undefined
  if (state !== 'uploaded') return { name, state: 'incomplete' }
  return typeof digest === 'string' && DIGEST.test(digest)
    ? { name, state, digest: digest.toLowerCase() }
    : undefined
}

const parseRelease = (stdout: string): Release | undefined => {
  let parsed: unknown
  try {
    parsed = JSON.parse(stdout)
  } catch {
    return undefined
  }
  const { isDraft, isPrerelease, assets } = (parsed ?? {}) as Record<string, unknown>
  if (typeof isDraft !== 'boolean' || typeof isPrerelease !== 'boolean') return undefined
  if (!Array.isArray(assets)) return undefined
  const records = assets.map(parseAsset)
  if (!records.every((a) => a !== undefined)) return undefined
  const complete = records.filter((a) => a.state === 'uploaded')
  const digests = new Map(complete.map((a) => [a.name, a.digest] as const))
  // A draft can also be a prerelease, which no stable tag may publish.
  const state = isPrerelease ? 'prerelease' : isDraft ? 'draft' : 'published'
  return { state, digests }
}
