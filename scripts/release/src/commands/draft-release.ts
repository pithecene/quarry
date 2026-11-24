import type { Context } from '../context.ts'
import { ReleaseError } from '../errors.ts'
import { lastLine } from '../process.ts'
import { githubReleaseState } from '../registry/github-release.ts'
import { tagNotes } from '../tag-notes.ts'

/**
 * Creates the GitHub release for `tag` as a draft, with the notes that the tag
 * holds. A draft is not public, so its assets can be uploaded and verified
 * before anyone sees the release. A re-run keeps a release that exists.
 */
export const draftRelease = async (ctx: Context, tag: string): Promise<void> => {
  const state = await githubReleaseState(ctx, tag)
  if (state === 'prerelease')
    throw new ReleaseError(`the GitHub release for ${tag} is a prerelease`)
  if (state !== 'missing') {
    ctx.log(`The GitHub release for ${tag} exists (${state})`)
    return
  }
  const notes = await tagNotes(ctx, tag)
  const result = await ctx.run('gh', [
    'release',
    'create',
    tag,
    '--draft',
    '--verify-tag',
    '--title',
    tag,
    '--notes',
    notes
  ])
  if (result.code !== 0) {
    throw new ReleaseError(`could not draft the release ${tag}: ${lastLine(result.stderr)}`)
  }
  ctx.log(`Drafted the GitHub release for ${tag}`)
}
