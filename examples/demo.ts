import type { QuarryContext } from '@pithecene/quarry-sdk'

export default async function run(ctx: QuarryContext): Promise<void> {
  await ctx.emit.item({
    item_type: 'demo',
    data: { message: 'hello from quarry' }
  })
}
