/**
 * Example: `beforeTerminal` hook for post-execution summary.
 *
 * Emits a summary item after all script work completes but before
 * the terminal event closes the emit channel.
 */
import type { QuarryContext, TerminalSignal } from '@pithecene/quarry-sdk'

let itemCount = 0

export default async function run(ctx: QuarryContext): Promise<void> {
  const products = ['Widget', 'Gadget', 'Gizmo']
  for (const name of products) {
    await ctx.emit.item({
      item_type: 'product',
      data: { name, price: Math.floor(Math.random() * 100) }
    })
    itemCount++
  }
}

export async function beforeTerminal(signal: TerminalSignal, ctx: QuarryContext): Promise<void> {
  if (signal.outcome === 'completed') {
    await ctx.emit.item({
      item_type: 'run_summary',
      data: { total_items: itemCount }
    })
  }
}
