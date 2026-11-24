import type { Context } from '../context.ts'
import { tagNotes } from '../tag-notes.ts'

/** Fails unless `tag` is an annotated tag that holds release notes. */
export const assertReleaseNotes = async (ctx: Context, tag: string): Promise<void> => {
  await tagNotes(ctx, tag)
  ctx.log(`The tag ${tag} holds its release notes`)
}
