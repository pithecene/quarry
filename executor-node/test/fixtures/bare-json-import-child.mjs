/**
 * Child process fixture for JSON import attribute integration test.
 *
 * Must run under plain `node` (not tsx/vitest) so the native ESM loader
 * enforces the import attribute requirement for JSON modules.
 *
 * Expects QUARRY_FIXTURE_DIR env var pointing to a temp directory
 * containing a .json file and a .mjs script with a bare JSON import.
 *
 * Exit codes:
 *   0 = loadScript threw expected error (stdout contains message)
 *   1 = loadScript succeeded unexpectedly or missing env
 */
import { writeSync } from 'node:fs'
import { join } from 'node:path'
import { loadScript } from '../../dist/loader.js'

const fixtureDir = process.env.QUARRY_FIXTURE_DIR
if (!fixtureDir) {
  process.stderr.write('QUARRY_FIXTURE_DIR not set\n')
  process.exit(1)
}

const scriptPath = join(fixtureDir, 'bare-import.mjs')

try {
  await loadScript(scriptPath)
  process.stderr.write('loadScript succeeded unexpectedly\n')
  process.exit(1)
} catch (err) {
  const message = err instanceof Error ? err.message : String(err)
  // Synchronous write to fd 1 (stdout). Node's stream-based
  // process.stdout.write() does not reliably flush to a pipe before
  // the process exits — even when awaiting the callback or setting
  // process.exitCode. writeSync bypasses the stream layer entirely.
  writeSync(1, `${message}\n`)
  process.exitCode = 0
}
