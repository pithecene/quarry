import type { Context } from '../context.ts'
import { absent, failed, type Lookup, present } from '../lookup.ts'
import { lastLine } from '../process.ts'

/** Is the image `ref` (`name:tag`) in its registry? */
export const imageProbe = async (ctx: Context, ref: string): Promise<Lookup> => {
  const result = await ctx.run('docker', ['buildx', 'imagetools', 'inspect', ref])
  if (result.code === 0) return present
  if (result.stderr.includes(': not found')) return absent
  return failed(`lookup of ${ref} failed: ${lastLine(result.stderr)}`)
}
