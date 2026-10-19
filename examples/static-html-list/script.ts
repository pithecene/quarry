import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import type { QuarryContext } from '@pithecene/quarry-sdk'

const __dirname = dirname(fileURLToPath(import.meta.url))

export default async function run(ctx: QuarryContext): Promise<void> {
  const html = await readFile(resolve(__dirname, 'data.html'), 'utf8')
  await ctx.page.setContent(html, { waitUntil: 'domcontentloaded' })

  const items = await ctx.page.$$eval('li.item', (els) =>
    els.map((el) => ({
      title: el.textContent?.trim() ?? ''
    }))
  )

  for (const item of items) {
    await ctx.emit.item({
      item_type: 'static_list_item',
      data: item
    })
  }
}
