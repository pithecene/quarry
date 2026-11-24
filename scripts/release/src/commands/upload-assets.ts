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
    let pending = await findPending(ctx, tag, files)
    if (pending.reason === undefined) break
    if (attempt > policy.attempts) throw new ReleaseError(pending.reason)
    ctx.log(`attempt ${attempt}/${policy.attempts}: ${pending.reason}`)
    if (attempt > 1) {
      await ctx.sleep(retryDelay(policy, attempt - 1))
      // The release may have changed meanwhile, and this read authorizes the uploads.
      pending = await findPending(ctx, tag, files)
      if (pending.reason === undefined) break
    }
    for (const name of pending.names) await upload(ctx, tag, join(dir, name), policy)
  }
  ctx.log(`The release ${tag} holds all ${files.size} files`)
}

/** The files to upload and why the release is incomplete, with no reason when it is complete. */
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
  if (state !== 'draft' && (state !== 'published' || names.length > 0)) {
    throw new ReleaseError(`the release ${tag} is ${state}, and lacks: ${names.join(', ')}`)
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
