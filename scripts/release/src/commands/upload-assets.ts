import { readdir } from 'node:fs/promises'
import { join } from 'node:path'
import type { Context } from '../context.ts'
import { describeError, ReleaseError } from '../errors.ts'
import { type RetryPolicy, retryDelay } from '../lookup.ts'
import { lastLine } from '../process.ts'
import { readRelease } from '../registry/github-release.ts'
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
 * replace, so only a draft takes them. A published release is read-only and
 * needs each file by name, because a re-run rebuilds archives with other bytes.
 */
export const uploadAssets = async (
  ctx: Context,
  tag: string,
  dir: string,
  policy: UploadPolicy
): Promise<void> => {
  const files = await localDigests(dir)
  for (let attempt = 1; ; attempt++) {
    const { names, reason } = await findPending(ctx, tag, files)
    if (reason === undefined) break
    if (attempt > policy.attempts) throw new ReleaseError(reason)
    ctx.log(`attempt ${attempt}/${policy.attempts}: ${reason}`)
    // The read above authorizes these uploads, so any delay comes after them.
    let failed = names.length === 0
    for (const name of names) {
      if (!(await upload(ctx, tag, join(dir, name), policy))) failed = true
    }
    if (failed && attempt < policy.attempts) await ctx.sleep(retryDelay(policy, attempt))
  }
  ctx.log(`The release ${tag} holds all ${files.size} files`)
}

/** The files to upload (none after a failed read) and why the release is incomplete, with no reason when it is complete. */
type Pending = { readonly names: readonly string[]; readonly reason?: string }

const findPending = async (
  ctx: Context,
  tag: string,
  files: ReadonlyMap<string, string>
): Promise<Pending> => {
  const read = await readRelease(ctx, tag)
  if ('failed' in read) return { names: [], reason: read.failed }
  const { state, digests } = read.release
  const lacks =
    state === 'published'
      ? (name: string) => !digests.has(name)
      : (name: string) => digests.get(name) !== files.get(name)
  const names = [...files.keys()].filter(lacks)
  if (state !== 'draft' && state !== 'published') {
    throw new ReleaseError(`the GitHub release for ${tag} is ${state}, not a draft`)
  }
  if (state === 'published' && names.length > 0) {
    throw new ReleaseError(`the release ${tag} is published, and lacks: ${names.join(', ')}`)
  }
  return names.length === 0
    ? { names }
    : { names, reason: `the release ${tag} does not hold as built: ${names.join(', ')}` }
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
): Promise<boolean> => {
  try {
    const args = ['release', 'upload', '--clobber', '--', tag, path]
    const { code, stderr, timedOut } = await ctx.run('gh', args, { timeoutMs })
    if (code === 0 && !timedOut) return true
    ctx.log(`uploading ${path} failed: ${timedOut ? 'timed out' : lastLine(stderr)}`)
  } catch (error) {
    ctx.log(`uploading ${path} failed: ${describeError(error)}`)
  }
  return false
}
