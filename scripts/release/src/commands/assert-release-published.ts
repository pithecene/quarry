import type { Context } from '../context.ts'
import { ReleaseError } from '../errors.ts'
import { githubReleaseState } from '../registry/github-release.ts'

const DESCRIPTION = {
  draft: 'a draft',
  prerelease: 'a prerelease',
  missing: 'missing'
} as const

/**
 * Fails unless the GitHub release for `tag` exists, is published, and is
 * not a prerelease.
 *
 * A release is published by creating its GitHub release, notes and all,
 * which pushes the tag. A tag without a published, stable release is
 * rejected before anything is published.
 */
export const assertReleasePublished = async (ctx: Context, tag: string): Promise<void> => {
  const state = await githubReleaseState(ctx, tag)
  if (state !== 'published') {
    throw new ReleaseError(
      `the GitHub release for ${tag} is ${DESCRIPTION[state]}; create and publish the release with its notes, which pushes the tag`
    )
  }
  ctx.log(`The GitHub release for ${tag} is published`)
}
