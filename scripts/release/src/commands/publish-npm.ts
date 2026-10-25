import type { Context } from '../context.ts'
import { ReleaseError } from '../errors.ts'
import { lookupWithRetry, type RetryPolicy } from '../lookup.ts'
import { readManifestFields } from '../package-manifest.ts'
import { npmProbe } from '../registry/npm.ts'

/**
 * Publishes the npm package in `dir` idempotently, under the `release`
 * dist-tag. `latest` is owned by `converge-pointers`, which runs serialized
 * after publishing, so overlapping release runs never race on it.
 *
 * The registry and its auth come from the caller's npm config
 * (actions/setup-node in CI). A version that is already published, by a
 * re-run or by a concurrent run that won the race, is a success.
 */
export const publishNpm = async (
  ctx: Context,
  dir: string,
  lookups: RetryPolicy
): Promise<void> => {
  const { name, version } = await readManifestFields(dir, 'package.json', ['name', 'version'])
  const spec = `${name}@${version}`
  const check = () =>
    lookupWithRetry(() => npmProbe(ctx, name, version), lookups, ctx.sleep, ctx.log)

  const before = await check()
  if (before.kind === 'present') {
    ctx.log(`${spec} is already published`)
    return
  }
  // An unreadable registry does not stop the publish: the registry itself
  // rejects a duplicate version, so publishing cannot overwrite anything.
  if (before.kind === 'failed') ctx.log(`could not check whether ${spec} is published; publishing`)

  const publish = await ctx.run(
    'pnpm',
    ['publish', '--access', 'restricted', '--no-git-checks', '--tag', 'release'],
    { cwd: dir, output: 'inherit' }
  )
  if (publish.code === 0) {
    ctx.log(`Published ${spec}`)
    return
  }

  // The registry rejects a duplicate version. If another run published it
  // between the check and the publish, the release is still complete; only
  // a confirmed answer says so.
  const after = await check()
  switch (after.kind) {
    case 'present':
      ctx.log(`${spec} was published by a concurrent run`)
      return
    case 'absent':
      throw new ReleaseError(`failed to publish ${spec}`)
    case 'failed':
      throw new ReleaseError(
        `failed to publish ${spec}, and could not check the registry for it: ${after.reason}`
      )
  }
}
