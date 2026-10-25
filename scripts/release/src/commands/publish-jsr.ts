import type { Context } from '../context.ts'
import { ReleaseError } from '../errors.ts'
import { lookupWithRetry, type RetryPolicy } from '../lookup.ts'
import { readManifestFields } from '../package-manifest.ts'
import { jsrProbe } from '../registry/jsr.ts'

export type JsrPublishPolicy = {
  /** Publish attempts before giving up. */
  readonly attempts: number
  /** Each attempt is killed after this long. */
  readonly attemptTimeoutMs: number
  /** Delay before the second attempt, doubled for each later one. */
  readonly retryDelayMs: number
}

/**
 * Publishes the JSR package in `dir` at `version` idempotently, with
 * bounded attempts.
 *
 * Authentication is GitHub OIDC (the job needs `id-token: write` and the
 * package linked to this repository). JSR processes uploads server-side and
 * that processing can stall indefinitely, so each attempt is time-limited.
 * After a failed attempt JSR's own record decides the outcome, because the
 * upload may have succeeded while the client's status polling failed. Only
 * a confirmed `present` counts as published.
 */
export const publishJsr = async (
  ctx: Context,
  dir: string,
  version: string,
  policy: JsrPublishPolicy,
  lookups: RetryPolicy
): Promise<void> => {
  const { name } = await readManifestFields(dir, 'jsr.json', ['name'])
  const spec = `${name}@${version}`
  const check = () =>
    lookupWithRetry(() => jsrProbe(ctx, name, version), lookups, ctx.sleep, ctx.log)

  const before = await check()
  if (before.kind === 'present') {
    ctx.log(`${spec} is already on JSR`)
    return
  }
  // JSR refuses to publish a version that exists, so an unreadable
  // registry does not stop the attempt.
  if (before.kind === 'failed') ctx.log(`could not check whether ${spec} is on JSR; publishing`)

  let delayMs = policy.retryDelayMs
  for (let attempt = 1; attempt <= policy.attempts; attempt++) {
    ctx.log(`Attempt ${attempt}/${policy.attempts}: jsr publish ${spec}`)
    const result = await ctx.run('npx', ['--yes', 'jsr', 'publish', '--set-version', version], {
      cwd: dir,
      output: 'inherit',
      timeoutMs: policy.attemptTimeoutMs
    })
    // An attempt that timed out is unconfirmed even if the client exited 0
    // on SIGTERM; only the registry can say whether it published.
    if (result.code === 0 && !result.timedOut) {
      ctx.log(`Published ${spec} to JSR`)
      return
    }
    if (result.timedOut) {
      ctx.log(`attempt ${attempt} timed out after ${policy.attemptTimeoutMs / 1000}s`)
    }

    const after = await check()
    if (after.kind === 'present') {
      ctx.log(`${spec} is on JSR despite the client error`)
      return
    }
    if (attempt < policy.attempts) {
      ctx.log(`Retrying in ${delayMs / 1000}s`)
      await ctx.sleep(delayMs)
      delayMs *= 2
    }
  }
  throw new ReleaseError(`failed to publish ${spec} to JSR after ${policy.attempts} attempts`)
}
