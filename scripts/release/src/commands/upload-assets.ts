import { createHash } from 'node:crypto'
import { readdir, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import type { Context } from '../context.ts'
import { describeError, ReleaseError } from '../errors.ts'
import { type RetryPolicy, retryDelay } from '../lookup.ts'
import { lastLine } from '../process.ts'
import { githubReleaseState, releaseDigests } from '../registry/github-release.ts'

export type UploadPolicy = RetryPolicy & {
  /** Each upload is killed after this long, so a stall withholds no other file. */
  readonly attemptTimeoutMs: number
}

/**
 * Makes the GitHub release for `tag` hold every file in `dir`, by SHA-256.
 *
 * Each attempt uploads only the files that the release does not hold, so
 * GitHub's record decides every outcome: one failed file does not withhold the
 * others, a response lost after a stored upload costs nothing, and a re-run
 * uploads nothing. Uploads replace, so only a draft takes them.
 */
export const uploadAssets = async (
  ctx: Context,
  tag: string,
  dir: string,
  policy: UploadPolicy
): Promise<void> => {
  const files = await localDigests(dir)
  const draft = (await githubReleaseState(ctx, tag)) === 'draft'

  for (let attempt = 1; ; attempt++) {
    const held = await releaseDigests(ctx, tag)
    const missing = [...files.keys()].filter((name) => held.get(name) !== files.get(name))
    if (missing.length === 0) break
    if (!draft || attempt > policy.attempts) {
      throw new ReleaseError(
        `the release ${tag}${draft ? '' : ' is not a draft and'} does not hold as built: ${missing.join(', ')}`
      )
    }
    if (attempt > 1) await ctx.sleep(retryDelay(policy, attempt - 1))
    for (const name of missing) await upload(ctx, tag, join(dir, name), policy)
  }
  ctx.log(`The release ${tag} holds all ${files.size} files as built`)
}

const localDigests = async (dir: string): Promise<ReadonlyMap<string, string>> => {
  const names = (await readdir(dir)).toSorted()
  if (names.length === 0) throw new ReleaseError(`no files to upload in ${dir}`)
  return new Map(
    await Promise.all(
      names.map(async (name) => {
        const digest = createHash('sha256').update(await readFile(join(dir, name)))
        return [name, `sha256:${digest.digest('hex')}`] as const
      })
    )
  )
}

const upload = async (
  ctx: Context,
  tag: string,
  path: string,
  { attemptTimeoutMs: timeoutMs }: UploadPolicy
): Promise<void> => {
  try {
    const args = ['release', 'upload', '--clobber', '--', tag, path]
    const { code, stderr, timedOut } = await ctx.run('gh', args, { timeoutMs })
    if (timedOut) ctx.log(`uploading ${path} timed out`)
    else if (code !== 0) ctx.log(`uploading ${path} failed: ${lastLine(stderr)}`)
  } catch (error) {
    ctx.log(`uploading ${path} failed: ${describeError(error)}`)
  }
}
