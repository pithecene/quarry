import type { Context } from '../context.ts'
import { ReleaseError } from '../errors.ts'
import { lastLine } from '../process.ts'
import { githubReleaseState } from '../registry/github-release.ts'
import { tagNotes } from '../tag-notes.ts'

/**
 * Makes the GitHub release for `tag` a draft with the notes that the tag holds.
 * A draft is not public, so its assets upload and verify before anyone sees the
 * release. A published release is left alone, so a re-run is safe.
 */
export const draftRelease = async (ctx: Context, tag: string): Promise<void> => {
  const state = await githubReleaseState(ctx, tag)
  if (state === 'prerelease')
    throw new ReleaseError(`the GitHub release for ${tag} is a prerelease`)
  if (state === 'published') {
    ctx.log(`The GitHub release for ${tag} is already published`)
    return
  }
  const create = state === 'missing' ? ['create', tag, '--draft', '--verify-tag'] : ['edit', tag]
  const notes = await tagNotes(ctx, tag)
  const result = await ctx.run('gh', ['release', ...create, '--title', tag, '--notes', notes])
  if (result.code !== 0) {
    throw new ReleaseError(`could not draft the release ${tag}: ${lastLine(result.stderr)}`)
  }
  ctx.log(`Drafted the GitHub release for ${tag}`)
}
