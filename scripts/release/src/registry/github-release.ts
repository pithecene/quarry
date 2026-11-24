import type { Context } from '../context.ts'
import { ReleaseError } from '../errors.ts'
import { lastLine } from '../process.ts'

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
  const fields = 'isDraft,isPrerelease,assets'
  const result = await ctx.run('gh', ['release', 'view', tag, '--json', fields])
  if (result.code !== 0) {
    if (result.stderr.includes('release not found')) {
      return { release: { state: 'missing', digests: new Map() } }
    }
    return { failed: `could not read the GitHub release for ${tag}: ${lastLine(result.stderr)}` }
  }
  const release = parseRelease(result.stdout)
  return release
    ? { release }
    : {
        failed: `could not read the GitHub release for ${tag}: unexpected output ${lastLine(result.stdout)}`
      }
}

/** The state of the GitHub release for `tag`; a failed read is an error. */
export const githubReleaseState = async (ctx: Context, tag: string): Promise<ReleaseState> => {
  const read = await readRelease(ctx, tag)
  if ('failed' in read) throw new ReleaseError(read.failed)
  return read.release.state
}

type Asset = { name: string; state: string; digest?: string | null }

const isAsset = (value: unknown): value is Asset => {
  const { name, state } = (value ?? {}) as Record<string, unknown>
  return typeof name === 'string' && typeof state === 'string'
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
  if (!Array.isArray(assets) || !assets.every(isAsset)) return undefined
  const digests = assets
    .filter((a) => a.state === 'uploaded')
    .map((a) => [a.name, a.digest ?? ''] as const)
  // A draft can also be a prerelease, which no stable tag may publish.
  const state = isPrerelease ? 'prerelease' : isDraft ? 'draft' : 'published'
  return { state, digests: new Map(digests) }
}
