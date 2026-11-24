import { readdir } from 'node:fs/promises'
import { join } from 'node:path'
import type { Context } from '../context.ts'
import { describeError, ReleaseError } from '../errors.ts'
import { type RetryPolicy, retryDelay } from '../lookup.ts'
import { lastLine } from '../process.ts'
import { githubReleaseState, releaseDigests } from '../registry/github-release.ts'
import { sha256 } from '../sha256.ts'

export type UploadPolicy = RetryPolicy & {
  /** Each upload is killed after this long, so a stall withholds no other file. */
  readonly attemptTimeoutMs: number
}

/**
 * Makes the draft GitHub release for `tag` hold every file in `dir`, by SHA-256.
 *
 * Each attempt uploads only the files that the release does not hold, so
 * GitHub's record decides every outcome: one failed file does not withhold the
 * others, and a response lost after a stored upload costs nothing. Uploads
 * replace, so only a draft takes them. A release publishes only after its files
 * verify, so a published one needs nothing.
 */
export const uploadAssets = async (
  ctx: Context,
  tag: string,
  dir: string,
  policy: UploadPolicy
): Promise<void> => {
  const files = await localDigests(dir)
  for (let attempt = 1; ; attempt++) {
    if (attempt > 1) await ctx.sleep(retryDelay(policy, attempt - 1))
    // Read after the delay: what it finds authorizes the uploads.
    const state = await githubReleaseState(ctx, tag)
    if (state === 'published') {
      ctx.log(`The release ${tag} is already published`)
      return
    }
    if (state !== 'draft') throw new ReleaseError(`the release ${tag} is ${state}, not a draft`)
    const held = await releaseDigests(ctx, tag)
    const missing = [...files.keys()].filter((name) => held?.get(name) !== files.get(name))
    if (missing.length === 0) {
      ctx.log(`The release ${tag} holds all ${files.size} files as built`)
      return
    }
    const reason = held
      ? `the release ${tag} does not hold as built: ${missing.join(', ')}`
      : `could not read the assets of ${tag}`
    if (attempt > policy.attempts) throw new ReleaseError(reason)
    ctx.log(`attempt ${attempt}/${policy.attempts}: ${reason}`)
    // An unreadable list says nothing about the files, so it uploads none.
    if (held) for (const name of missing) await upload(ctx, tag, join(dir, name), policy)
  }
}

const localDigests = async (dir: string): Promise<ReadonlyMap<string, string>> => {
  const names = (await readdir(dir)).toSorted()
  if (names.length === 0) throw new ReleaseError(`no files to upload in ${dir}`)
  const digests = new Map<string, string>()
  for (const name of names) digests.set(name, `sha256:${await sha256(join(dir, name))}`)
  return digests
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
