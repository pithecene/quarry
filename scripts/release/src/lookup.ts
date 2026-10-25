import type { Log, Sleep } from './context.ts'

/**
 * The answer to "is this published?". Only the registry's own not-found
 * answer is `absent`. A lookup that could not be completed (network, auth,
 * an unexpected response) is `failed` and must never be read as `absent`,
 * or a step would act on a version whose state it does not know.
 */
export type Lookup =
  | { readonly kind: 'present' }
  | { readonly kind: 'absent' }
  | { readonly kind: 'failed'; readonly reason: string }

export const present: Lookup = { kind: 'present' }
export const absent: Lookup = { kind: 'absent' }
export const failed = (reason: string): Lookup => ({ kind: 'failed', reason })

export type Probe = () => Promise<Lookup>

/** How often a failed lookup is retried, with the delay doubled each time. */
export type RetryPolicy = {
  readonly attempts: number
  readonly delayMs: number
}

/**
 * Runs `probe` until it answers `present` or `absent`, or the attempts run
 * out. Only `failed` is retried: a definite answer is final.
 */
export const lookupWithRetry = async (
  probe: Probe,
  policy: RetryPolicy,
  sleep: Sleep,
  log: Log
): Promise<Lookup> => {
  let delayMs = policy.delayMs
  for (let attempt = 1; ; attempt++) {
    const result = await probe()
    if (result.kind !== 'failed' || attempt >= policy.attempts) return result
    log(`${result.reason}; retrying in ${delayMs / 1000}s`)
    await sleep(delayMs)
    delayMs *= 2
  }
}
