import type { QuarryContext } from '@pithecene/quarry-sdk'

export default async function run(ctx: QuarryContext): Promise<void> {
  await ctx.page.goto('data:text/html,<title>container</title>')
  await ctx.emit.item({ item_type: 'title', data: { title: await ctx.page.title() } })
}
