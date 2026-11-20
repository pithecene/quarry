import { readFile } from 'node:fs/promises'

import type { QuarryContext } from '@pithecene/quarry-sdk'

const indexFixture = new URL('./fixtures/index.html', import.meta.url)

/**
 * Root script: parses the local harbor-notice index fixture and enqueues one
 * child run per notice page.
 */
export default async function run(ctx: QuarryContext): Promise<void> {
  await ctx.page.setContent(await readFile(indexFixture, 'utf8'), {
    waitUntil: 'domcontentloaded'
  })

  const files = await ctx.page.$$eval('#notices li.notice a', (anchors) =>
    anchors.flatMap((a) => {
      const href = a.getAttribute('href')?.trim()
      return href ? [href] : []
    })
  )

  for (const file of files) {
    await ctx.emit.enqueue({
      target: './examples/harbor-notices/notice.ts',
      params: { file }
    })
  }
}
