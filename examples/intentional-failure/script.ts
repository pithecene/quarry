import type { QuarryContext } from '@pithecene/quarry-sdk'

/**
 * Intentional failure example for testing error paths.
 * This script throws an error after emitting one item to verify
 * that run_error terminal state is correctly reported.
 */
export default async function run(ctx: QuarryContext): Promise<void> {
  // Emit one item first to show partial progress is possible
  await ctx.emit.item({
    item_type: 'before_failure',
    data: { message: 'this item should be emitted' }
  })

  // Intentionally throw an error
  throw new Error('Intentional failure for testing')
}
