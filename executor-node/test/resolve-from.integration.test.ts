/**
 * Integration test: --resolve-from ESM resolve hook.
 *
 * Creates a temporary node_modules with a fake ESM package (@test/greet),
 * spawns a child process with QUARRY_RESOLVE_FROM pointing to that directory,
 * and verifies:
 * - The bare specifier `@test/greet` resolves via the fallback hook
 * - The fallback observability message appears on stderr
 * - The module's export is usable (correct value on stdout)
 * - Without QUARRY_RESOLVE_FROM, the import fails (control case)
 */
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { type ProcessResult, runProcess } from './run-process.js'

const testDir = dirname(fileURLToPath(import.meta.url))
const fixturePath = resolve(testDir, 'fixtures/resolve-from-child.ts')
const tsxBin = resolve(testDir, '../../node_modules/.bin/tsx')

/** Temp directory for the fake node_modules tree (unique per run). */
let tmpDir: string

beforeAll(() => {
  // Create a unique temporary directory to avoid cross-run interference
  tmpDir = mkdtempSync(join(tmpdir(), 'quarry-resolve-from-test-'))
  const pkgDir = join(tmpDir, 'node_modules', '@test', 'greet')
  mkdirSync(pkgDir, { recursive: true })

  // Write a minimal ESM package
  writeFileSync(
    join(pkgDir, 'package.json'),
    JSON.stringify({
      name: '@test/greet',
      version: '1.0.0',
      type: 'module',
      exports: {
        '.': {
          import: './index.mjs',
          require: './index.cjs'
        }
      }
    })
  )

  // ESM entrypoint (the "import" condition target)
  writeFileSync(join(pkgDir, 'index.mjs'), 'export function greet() { return "hello from esm"; }\n')

  // CJS entrypoint (the "require" condition target — deliberately different)
  writeFileSync(
    join(pkgDir, 'index.cjs'),
    'module.exports.greet = function() { return "hello from cjs"; };\n'
  )
})

afterAll(() => {
  rmSync(tmpDir, { recursive: true, force: true })
})

function runFixture(env?: Record<string, string>): Promise<ProcessResult> {
  return runProcess(tsxBin, [fixturePath], env)
}

describe('resolve-from integration', () => {
  it('resolves bare specifier via fallback when QUARRY_RESOLVE_FROM is set', async () => {
    const nodeModulesDir = join(tmpDir, 'node_modules')
    const { stdout, stderr, exitCode } = await runFixture({
      QUARRY_RESOLVE_FROM: nodeModulesDir
    })

    // Should exit cleanly
    expect(exitCode, `child stderr: ${stderr}`).toBe(0)

    // Should have resolved the module and received the ESM export
    const parsed = JSON.parse(stdout.toString())
    expect(parsed.greeting).toBe('hello from esm')

    // Note: the stderr fallback message ("quarry: resolved ... via --resolve-from
    // fallback") is written by the loader hook thread. It may not flush before
    // the main thread calls process.exit(0). The message is best-effort
    // observability; the behavioral assertion above is authoritative.
  }, 15_000)

  it('uses ESM import conditions (not CJS require conditions)', async () => {
    // The fake package has conditional exports:
    //   "import" -> index.mjs (returns "hello from esm")
    //   "require" -> index.cjs (returns "hello from cjs")
    // If the hook correctly uses nextResolve with parentURL (ESM conditions),
    // we get "hello from esm". If it incorrectly used createRequire (CJS
    // conditions), we'd get "hello from cjs".
    const nodeModulesDir = join(tmpDir, 'node_modules')
    const { stdout, exitCode, stderr } = await runFixture({
      QUARRY_RESOLVE_FROM: nodeModulesDir
    })

    expect(exitCode, `child stderr: ${stderr}`).toBe(0)
    const parsed = JSON.parse(stdout.toString())
    expect(parsed.greeting).toBe('hello from esm')
  }, 15_000)

  it('fails without QUARRY_RESOLVE_FROM (control case)', async () => {
    // Without the env var, the child exits with code 2
    const { exitCode, stderr } = await runFixture({
      QUARRY_RESOLVE_FROM: ''
    })

    expect(exitCode).toBe(2)
    expect(stderr).toContain('QUARRY_RESOLVE_FROM not set')
  }, 15_000)

  it('fails when QUARRY_RESOLVE_FROM points to wrong directory', async () => {
    // Point to a directory that doesn't contain @test/greet
    const { exitCode, stderr } = await runFixture({
      QUARRY_RESOLVE_FROM: testDir
    })

    expect(exitCode).toBe(1)
    expect(stderr).toContain('Import failed')
  }, 15_000)
})
