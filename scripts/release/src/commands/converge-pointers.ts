import type { Context } from '../context.ts'
import { describeError, ReleaseError } from '../errors.ts'
import { type Lookup, lookupWithRetry, type RetryPolicy } from '../lookup.ts'
import { readManifestFields } from '../package-manifest.ts'
import { lastLine } from '../process.ts'
import { imageProbe } from '../registry/image.ts'
import { npmProbe } from '../registry/npm.ts'
import { formatVersion, releaseVersionsDescending, remoteTags } from '../versions.ts'

/** The outcome of searching versions, highest first, for one that qualifies. */
type Search =
  | { readonly kind: 'found'; readonly version: string }
  | { readonly kind: 'none' }
  | { readonly kind: 'unknown'; readonly reason: string }

/**
 * The highest of `versions` (ordered highest first) for which `isPublished`
 * answers `present`. Stops at the first failed lookup: a lower version is
 * never chosen over one whose state is unknown.
 */
const highestPublished = async (
  versions: readonly string[],
  isPublished: (version: string) => Promise<Lookup>
): Promise<Search> => {
  for (const version of versions) {
    const result = await isPublished(version)
    if (result.kind === 'present') return { kind: 'found', version }
    if (result.kind === 'failed') return { kind: 'unknown', reason: result.reason }
  }
  return { kind: 'none' }
}

/** One moving pointer and how to find and set its target. */
type Pointer = {
  /** Names the pointer in messages, e.g. `npm @scope/pkg latest`. */
  readonly label: string
  /** Names what a published version is, e.g. `published release of @scope/pkg`. */
  readonly target: string
  readonly isPublished: (version: string) => Promise<Lookup>
  /** Points the pointer at `version`; a rejection or failure leaves it unchanged. */
  readonly point: (version: string) => Promise<void>
}

/**
 * Points `pointer` at the highest published version. Returns false if the
 * pointer could not be converged; it is then unchanged.
 */
const converge = async (
  ctx: Context,
  versions: readonly string[],
  pointer: Pointer
): Promise<boolean> => {
  const search = await highestPublished(versions, pointer.isPublished)
  switch (search.kind) {
    case 'none':
      ctx.log(`no ${pointer.target} exists; ${pointer.label} unchanged`)
      return true
    case 'unknown':
      ctx.log(
        `could not determine the highest ${pointer.target} (${search.reason}); ${pointer.label} unchanged`
      )
      return false
    case 'found':
      try {
        await pointer.point(search.version)
      } catch (error) {
        ctx.log(`could not point ${pointer.label} at ${search.version}: ${describeError(error)}`)
        return false
      }
      ctx.log(`${pointer.label} -> ${search.version}`)
      return true
  }
}

/** Runs a command whose failure means a pointer write failed. */
const runOrThrow = async (
  ctx: Context,
  command: string,
  args: readonly string[]
): Promise<void> => {
  const result = await ctx.run(command, args)
  if (result.code !== 0)
    throw new ReleaseError(`${command} ${args.join(' ')}: ${lastLine(result.stderr)}`)
}

/**
 * Points the moving release pointers at the highest published release:
 * npm `latest` for the package in `pkgDir`, and `<image>:latest` and
 * `<image>:slim`.
 *
 * The result depends only on registry state, not on which release run
 * invokes it, so any invocation after a release's artifacts are published
 * leaves the pointers correct. Neither registry offers compare-and-swap on
 * these pointers, so callers must serialize invocations (the release
 * workflow runs this in a job under a `concurrency` group). Under
 * serialization a dropped pending invocation is harmless: the one that
 * replaces it converges to state that includes everything the dropped one
 * would have seen.
 *
 * Each registry is converged independently, to the highest release
 * published on it, and each is attempted even if the other fails. Any
 * failure fails the step, so a stale pointer is visible and a re-run
 * repairs it.
 */
export const convergePointers = async (
  ctx: Context,
  pkgDir: string,
  image: string,
  lookups: RetryPolicy
): Promise<void> => {
  const { name } = await readManifestFields(pkgDir, 'package.json', ['name'])
  const versions = releaseVersionsDescending(await remoteTags(ctx)).map(formatVersion)
  if (versions.length === 0) throw new ReleaseError('no release tags on origin')

  const lookup = (probe: () => Promise<Lookup>) =>
    lookupWithRetry(probe, lookups, ctx.sleep, ctx.log)

  const npm: Pointer = {
    label: `npm ${name} latest`,
    target: `published release of ${name}`,
    isPublished: (version) => lookup(() => npmProbe(ctx, name, version)),
    point: (version) => runOrThrow(ctx, 'npm', ['dist-tag', 'add', `${name}@${version}`, 'latest'])
  }

  const images: Pointer = {
    label: `${image}:latest and :slim`,
    target: 'pushed pair of release images',
    // Both images of a release must be pushed before either pointer moves.
    isPublished: async (version) => {
      const full = await lookup(() => imageProbe(ctx, `${image}:${version}`))
      if (full.kind !== 'present') return full
      return lookup(() => imageProbe(ctx, `${image}:${version}-slim`))
    },
    point: async (version) => {
      await runOrThrow(ctx, 'docker', [
        'buildx',
        'imagetools',
        'create',
        '-t',
        `${image}:latest`,
        `${image}:${version}`
      ])
      await runOrThrow(ctx, 'docker', [
        'buildx',
        'imagetools',
        'create',
        '-t',
        `${image}:slim`,
        `${image}:${version}-slim`
      ])
    }
  }

  const failures: string[] = []
  if (!(await converge(ctx, versions, npm))) failures.push('npm latest')
  if (!(await converge(ctx, versions, images))) failures.push('GHCR :latest/:slim')
  if (failures.length > 0) throw new ReleaseError(`failed to converge: ${failures.join(', ')}`)
}
