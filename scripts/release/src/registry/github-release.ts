import type { Context } from '../context.ts'
import { ReleaseError } from '../errors.ts'
import { lastLine } from '../process.ts'

export type ReleaseState = 'published' | 'draft' | 'prerelease' | 'missing'

/**
 * The state of the GitHub release for `tag`, read with `gh` (which needs
 * `GH_TOKEN` in CI). Only gh's own "release not found" answer is
 * `missing`; any other failure is an error, never a missing release.
 */
export const githubReleaseState = async (ctx: Context, tag: string): Promise<ReleaseState> => {
  const result = await ctx.run('gh', ['release', 'view', tag, '--json', 'isDraft,isPrerelease'])
  if (result.code !== 0) {
    if (result.stderr.includes('release not found')) return 'missing'
    throw new ReleaseError(
      `could not read the GitHub release for ${tag}: ${lastLine(result.stderr)}`
    )
  }
  const state = parseState(result.stdout)
  if (state === undefined) {
    throw new ReleaseError(
      `could not read the GitHub release for ${tag}: unexpected output ${lastLine(result.stdout)}`
    )
  }
  return state
}

const parseState = (stdout: string): ReleaseState | undefined => {
  let parsed: unknown
  try {
    parsed = JSON.parse(stdout)
  } catch {
    return undefined
  }
  if (typeof parsed !== 'object' || parsed === null) return undefined
  const { isDraft, isPrerelease } = parsed as Record<string, unknown>
  if (typeof isDraft !== 'boolean' || typeof isPrerelease !== 'boolean') return undefined
  if (isDraft) return 'draft'
  if (isPrerelease) return 'prerelease'
  return 'published'
}
