import type { Context } from './context.ts'
import { ReleaseError } from './errors.ts'
import { lastLine } from './process.ts'

const SIGNATURE = /^-----BEGIN (?:PGP|SSH) SIGNATURE-----/m

/**
 * The release notes that the annotated tag `tag` holds as its message, as
 * `bosun release` writes them. The tag is fetched again first, because
 * actions/checkout replaces an annotated tag with a lightweight one.
 */
export const tagNotes = async (ctx: Context, tag: string): Promise<string> => {
  const git = async (args: readonly string[]): Promise<string> => {
    const result = await ctx.run('git', args)
    if (result.code !== 0) {
      throw new ReleaseError(`could not read the tag ${tag}: ${lastLine(result.stderr)}`)
    }
    return result.stdout
  }
  const ref = `refs/tags/${tag}`
  await git(['fetch', '--quiet', '--force', 'origin', `${ref}:${ref}`])
  if ((await git(['cat-file', '-t', ref])).trim() !== 'tag') {
    throw new ReleaseError(
      `${tag} is a lightweight tag, which holds no release notes; tag with bosun release`
    )
  }
  const message = await git([
    'tag',
    '-l',
    '--format=%(contents:subject)%0a%0a%(contents:body)',
    tag
  ])
  const notes = (message.split(SIGNATURE)[0] ?? '').trim()
  if (notes === '') throw new ReleaseError(`the tag ${tag} holds no release notes`)
  return notes
}
