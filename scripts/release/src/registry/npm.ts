import type { Context } from '../context.ts'
import { absent, failed, type Lookup, present } from '../lookup.ts'
import { lastLine } from '../process.ts'

/**
 * Is `name@version` on the npm registry the caller's npm config points at?
 * `npm view` prints the version when it exists, and fails with E404 when
 * the package or the version does not. Only that E404 is `absent`: a
 * successful run that prints anything else is an unexpected answer, so it
 * is a failed lookup, never a reason to fall back to an older version.
 */
export const npmProbe = async (ctx: Context, name: string, version: string): Promise<Lookup> => {
  const result = await ctx.run('npm', ['view', `${name}@${version}`, 'version'])
  const output = `${result.stdout}\n${result.stderr}`
  if (result.code === 0) {
    if (lastLine(result.stdout) === version) return present
    return failed(`npm view ${name}@${version} printed an unexpected answer: "${lastLine(output)}"`)
  }
  if (output.includes('E404')) return absent
  return failed(`npm lookup of ${name}@${version} failed: ${lastLine(output)}`)
}
