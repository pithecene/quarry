import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import type { QuarryContext } from '@pithecene/quarry-sdk'

const __dirname = dirname(fileURLToPath(import.meta.url))

type DetailJob = {
  product_id: string
  fixture: string
}

/**
 * Child script: reads a product detail fixture and emits a single item
 * with the extracted product data.
 */
export default async function run(ctx: QuarryContext): Promise<void> {
  const job = ctx.job as DetailJob

  const html = await readFile(resolve(__dirname, 'fixtures', job.fixture), 'utf8')
  await ctx.page.setContent(html, { waitUntil: 'domcontentloaded' })

  const product = await ctx.page.$eval('.product-detail', (el) => ({
    id: el.getAttribute('data-id') ?? '',
    title: el.querySelector('.title')?.textContent?.trim() ?? '',
    price: el.querySelector('.price')?.textContent?.trim() ?? '',
    description: el.querySelector('.description')?.textContent?.trim() ?? ''
  }))

  await ctx.emit.item({
    item_type: 'product',
    data: product
  })
}
