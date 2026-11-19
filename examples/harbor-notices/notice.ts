import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import type { QuarryContext } from '@pithecene/quarry-sdk'

const __dirname = dirname(fileURLToPath(import.meta.url))

type NoticeJob = {
  file: string
}

type Notice = {
  area: string
  effective: string
  notice: string
  subject: string
  text: string
}

/**
 * Child script: parses one harbor-notice fixture and emits a typed notice item.
 */
export default async function run(ctx: QuarryContext): Promise<void> {
  const job = ctx.job as NoticeJob
  const html = await readFile(resolve(__dirname, 'fixtures', job.file), 'utf8')
  await ctx.page.setContent(html, { waitUntil: 'domcontentloaded' })

  const data: Notice = await ctx.page.$eval('article.harbor-notice', (el) => ({
    area: el.querySelector('.area')?.textContent?.trim() ?? '',
    effective: el.querySelector('.effective')?.getAttribute('datetime') ?? '',
    notice: el.getAttribute('data-notice') ?? '',
    subject: el.querySelector('.subject')?.textContent?.trim() ?? '',
    text: el.querySelector('.text')?.textContent?.trim() ?? ''
  }))

  await ctx.emit.item({
    item_type: 'notice',
    data
  })
}
