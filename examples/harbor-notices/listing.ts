import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import type { QuarryContext } from '@pithecene/quarry-sdk'

const __dirname = dirname(fileURLToPath(import.meta.url))

/**
 * Root script: parses the local harbor-notice index fixture and enqueues one
 * child run per notice page.
 */
export default async function run(ctx: QuarryContext): Promise<void> {
  const html = await readFile(resolve(__dirname, 'fixtures/index.html'), 'utf8')
  await ctx.page.setContent(html, { waitUntil: 'domcontentloaded' })

  const files = await ctx.page.$$eval('#notices li.notice a', (anchors) =>
    anchors.map((a) => a.getAttribute('href') ?? '').filter((href) => href.length > 0)
  )

  for (const file of files) {
    await ctx.emit.enqueue({
      target: './examples/harbor-notices/notice.ts',
      params: { file }
    })
  }
}
