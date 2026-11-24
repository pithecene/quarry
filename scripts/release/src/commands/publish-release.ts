import type { Context } from '../context.ts'
import { ReleaseError } from '../errors.ts'
import { lastLine } from '../process.ts'
import { githubReleaseState } from '../registry/github-release.ts'

/**
 * Publishes the draft GitHub release for `tag`, the last step of a release. A
 * release that is already published is left alone, so a re-run is safe.
 */
export const publishRelease = async (ctx: Context, tag: string): Promise<void> => {
  const state = await githubReleaseState(ctx, tag)
  if (state === 'published') {
    ctx.log(`The GitHub release for ${tag} is already published`)
    return
  }
  if (state !== 'draft') throw new ReleaseError(`the GitHub release for ${tag} is ${state}`)
  const result = await ctx.run('gh', ['release', 'edit', tag, '--draft=false'])
  if (result.code !== 0) {
    throw new ReleaseError(`could not publish the release ${tag}: ${lastLine(result.stderr)}`)
  }
  ctx.log(`Published the GitHub release for ${tag}`)
}
