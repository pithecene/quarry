import type { Context } from '../context.ts'
import { ReleaseError } from '../errors.ts'
import {
  compareVersions,
  formatTag,
  releaseVersionsDescending,
  remoteTags,
  type Version
} from '../versions.ts'

/**
 * Fails unless `tag` is at least as high as every released tag.
 *
 * Releases are monotonic: a tag lower than an already-released tag is
 * rejected before anything is published. Equal passes, so a re-run of the
 * current release passes. Republishing an older version is a deliberate
 * manual operation, not a workflow path.
 *
 * This is a policy gate on new tags. The moving pointers (npm `latest`,
 * GHCR `:latest` and `:slim`) do not depend on it: `converge-pointers`
 * points them at the highest published release whichever run invokes it.
 */
export const assertHighestTag = async (ctx: Context, tag: Version): Promise<void> => {
  const [highest] = releaseVersionsDescending(await remoteTags(ctx))
  if (highest !== undefined && compareVersions(tag, highest) < 0) {
    throw new ReleaseError(
      `${formatTag(tag)} is lower than the released ${formatTag(highest)}; releases must be monotonic (republishing an older version is a manual operation)`
    )
  }
  ctx.log(`${formatTag(tag)} is the highest release`)
}
