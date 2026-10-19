import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import type { QuarryContext } from '@pithecene/quarry-sdk'

const __dirname = dirname(fileURLToPath(import.meta.url))
const pages = ['page1.html', 'page2.html']

export default async function run(ctx: QuarryContext): Promise<void> {
  for (const [idx, pageName] of pages.entries()) {
    const html = await readFile(resolve(__dirname, pageName), 'utf8')
    await ctx.page.setContent(html, { waitUntil: 'domcontentloaded' })

    const items = await ctx.page.$$eval('li.item', (els) =>
      els.map((el) => ({
        title: el.textContent?.trim() ?? ''
      }))
    )

    for (const item of items) {
      await ctx.emit.item({
        item_type: 'toy_page_item',
        data: { ...item, page: idx + 1 }
      })
    }

    await ctx.emit.checkpoint({
      checkpoint_id: `page-${idx + 1}`
    })
  }
}
