/**
 * Example: `prepare` hook for job filtering.
 *
 * Demonstrates skipping stale jobs before browser launch.
 * Pass `--job '{"url":"https://example.com","stale":true}'` to trigger skip.
 * Pass `--job '{"url":"https://example.com"}'` for normal execution.
 */
import type { PrepareResult, QuarryContext, RunMeta } from '@pithecene/quarry-sdk'

type Job = { url: string; stale?: boolean }

export function prepare(job: Job, _run: RunMeta): PrepareResult<Job> {
  if (job.stale) {
    return { action: 'skip', reason: 'job marked as stale' }
  }
  return { action: 'continue' }
}

export default async function run(ctx: QuarryContext<Job>): Promise<void> {
  await ctx.page.setContent(`<h1>${ctx.job.url}</h1>`)
  const title = await ctx.page.title()
  await ctx.emit.item({
    item_type: 'page',
    data: { url: ctx.job.url, title }
  })
}
