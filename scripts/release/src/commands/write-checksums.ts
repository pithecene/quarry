import { readdir, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import type { Context } from '../context.ts'
import { ReleaseError } from '../errors.ts'
import { sha256 } from '../sha256.ts'

/**
 * Writes `<file>.sha256` beside every file in `dir`, in the format of
 * sha256sum. It names the bare file, so it verifies beside a download.
 */
export const writeChecksums = async (ctx: Context, dir: string): Promise<void> => {
  const files = (await readdir(dir)).filter((name) => !name.endsWith('.sha256'))
  if (files.length === 0) throw new ReleaseError(`no files to checksum in ${dir}`)
  for (const name of files) {
    const digest = await sha256(join(dir, name))
    await writeFile(join(dir, `${name}.sha256`), `${digest}  ${name}\n`)
  }
  ctx.log(`Wrote ${files.length} checksum files in ${dir}`)
}
