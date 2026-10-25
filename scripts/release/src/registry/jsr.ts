import type { Context } from '../context.ts'
import { describeError } from '../errors.ts'
import { absent, failed, type Lookup, present } from '../lookup.ts'

const REQUEST_TIMEOUT_MS = 20_000

/**
 * Is `name@version` on JSR? Per-version metadata on jsr.io is the record
 * the registry serves to installers; the api.jsr.io version endpoint can
 * lag behind it. 200 is present and 404 absent; any other status, or no
 * response, is a failed lookup.
 */
export const jsrProbe = async (ctx: Context, name: string, version: string): Promise<Lookup> => {
  const url = `https://jsr.io/${name}/${version}_meta.json`
  try {
    const { status } = await ctx.fetch(url, { signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) })
    if (status === 200) return present
    if (status === 404) return absent
    return failed(`JSR lookup of ${name}@${version} failed: HTTP ${status}`)
  } catch (error) {
    return failed(`JSR lookup of ${name}@${version} failed: ${describeError(error)}`)
  }
}
