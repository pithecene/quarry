import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import type { QuarryContext } from '@pithecene/quarry-sdk'

const __dirname = dirname(fileURLToPath(import.meta.url))

export default async function run(ctx: QuarryContext): Promise<void> {
  const html = await readFile(resolve(__dirname, 'page.html'), 'utf8')
  await ctx.page.setContent(html, { waitUntil: 'domcontentloaded' })

  const data = await ctx.page.screenshot({ type: 'png' })

  await ctx.emit.artifact({
    name: 'snapshot.png',
    content_type: 'image/png',
    data
  })
}
