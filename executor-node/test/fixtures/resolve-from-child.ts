/**
 * Child process fixture for resolve-from integration test.
 *
 * Exercises the ESM resolve hook end-to-end using the production
 * registerResolveFromHook function (not a copy):
 * 1. Reads QUARRY_RESOLVE_FROM from env (set by parent test)
 * 2. Calls registerResolveFromHook() — same code path as bin/executor.ts
 * 3. Dynamically imports a bare specifier ("@test/greet") that only
 *    exists under the QUARRY_RESOLVE_FROM directory
 * 4. Writes the resolved value to stdout
 *
 * The parent test creates the fake package, spawns this fixture, and
 * asserts that the import succeeds and uses ESM import conditions.
 *
 * Exit codes:
 *   0 = import succeeded via fallback
 *   1 = import failed (hook didn't work)
 *   2 = QUARRY_RESOLVE_FROM not set
 */
import { registerResolveFromHook } from '../../src/resolve-from.js'

const resolveFrom = process.env.QUARRY_RESOLVE_FROM
if (!resolveFrom) {
  process.stderr.write('QUARRY_RESOLVE_FROM not set\n')
  process.exit(2)
}

// Register the production hook — same call as bin/executor.ts
await registerResolveFromHook(resolveFrom)

// Now try to import a bare specifier that only exists in QUARRY_RESOLVE_FROM
try {
  const mod = await import('@test/greet')
  // Write the resolved value so the parent can verify
  process.stdout.write(`${JSON.stringify({ greeting: mod.greet() })}\n`)
  process.exit(0)
} catch (err: unknown) {
  const message = err instanceof Error ? err.message : String(err)
  process.stderr.write(`Import failed: ${message}\n`)
  process.exit(1)
}
