import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import type { QuarryContext } from '@pithecene/quarry-sdk'

const __dirname = dirname(fileURLToPath(import.meta.url))

/**
 * Root script: parses a product listing fixture and enqueues a detail
 * script for each product found.
 */
export default async function run(ctx: QuarryContext): Promise<void> {
  const html = await readFile(resolve(__dirname, 'fixtures/listing.html'), 'utf8')
  await ctx.page.setContent(html, { waitUntil: 'domcontentloaded' })

  const products = await ctx.page.$$eval('li.product', (els) =>
    els.map((el) => ({
      id: el.getAttribute('data-id') ?? '',
      name: el.querySelector('a')?.textContent?.trim() ?? ''
    }))
  )

  for (const product of products) {
    await ctx.emit.enqueue({
      target: './examples/fan-out-chain/detail.ts',
      params: {
        product_id: product.id,
        fixture: `detail-${product.id}.html`
      }
    })
  }
}
