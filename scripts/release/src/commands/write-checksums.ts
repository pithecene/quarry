import { createHash } from 'node:crypto'
import { readdir, readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import type { Context } from '../context.ts'
import { ReleaseError } from '../errors.ts'

/**
 * Writes `<file>.sha256` beside every file in `dir`, in the format of
 * sha256sum. It names the bare file, so it verifies beside a download.
 */
export const writeChecksums = async (ctx: Context, dir: string): Promise<void> => {
  const files = (await readdir(dir)).filter((name) => !name.endsWith('.sha256'))
  if (files.length === 0) throw new ReleaseError(`no files to checksum in ${dir}`)
  for (const name of files) {
    const digest = createHash('sha256')
      .update(await readFile(join(dir, name)))
      .digest('hex')
    await writeFile(join(dir, `${name}.sha256`), `${digest}  ${name}\n`)
  }
  ctx.log(`Wrote ${files.length} checksum files in ${dir}`)
}
