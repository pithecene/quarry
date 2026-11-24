/**
 * Integration test: executor --validate mode.
 *
 * Spawns the executor entrypoint with --validate and verifies:
 * - Valid script → exit 0, valid JSON with exports info
 * - Valid script with hooks → reports hook names
 * - Missing module → exit 1, JSON with error
 * - Missing default export → exit 1, JSON with error
 * - Bad hook type → exit 1, JSON with error
 */
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import { type ProcessResult, runProcess } from './run-process.js'

const testDir = dirname(fileURLToPath(import.meta.url))
const tsxBin = resolve(testDir, '../../node_modules/.bin/tsx')
const entrypoint = resolve(testDir, '../src/bin/executor.ts')

/**
 * Spawn the executor entrypoint with --validate and a script path.
 */
function runValidate(...args: string[]): Promise<ProcessResult> {
  return runProcess(tsxBin, [entrypoint, '--validate', ...args])
}

describe('executor --validate mode', () => {
  it('validates a valid script with default export only', async () => {
    const fixture = resolve(testDir, 'fixtures/validate-valid.ts')
    const { stdout, exitCode, stderr } = await runValidate(fixture)

    expect(exitCode, `stderr: ${stderr}`).toBe(0)

    const result = JSON.parse(stdout.toString())
    expect(result.valid).toBe(true)
    expect(result.exports.default).toBe(true)
    expect(result.exports.hooks).toEqual([])
  }, 15_000)

  it('reports hook names for a script with hooks', async () => {
    const fixture = resolve(testDir, 'fixtures/validate-with-hooks.ts')
    const { stdout, exitCode, stderr } = await runValidate(fixture)

    expect(exitCode, `stderr: ${stderr}`).toBe(0)

    const result = JSON.parse(stdout.toString())
    expect(result.valid).toBe(true)
    expect(result.exports.default).toBe(true)
    expect(result.exports.hooks).toContain('prepare')
    expect(result.exports.hooks).toContain('cleanup')
  }, 15_000)

  it('fails for a missing module', async () => {
    const { stdout, exitCode } = await runValidate('/nonexistent/script.ts')

    expect(exitCode).toBe(1)

    const result = JSON.parse(stdout.toString())
    expect(result.valid).toBe(false)
    expect(result.error).toBeTruthy()
  }, 15_000)

  it('fails when default export is missing', async () => {
    const fixture = resolve(testDir, 'fixtures/validate-no-default.ts')
    const { stdout, exitCode } = await runValidate(fixture)

    expect(exitCode).toBe(1)

    const result = JSON.parse(stdout.toString())
    expect(result.valid).toBe(false)
    expect(result.error).toContain('missing default export')
  }, 15_000)

  it('fails when a hook is not a function', async () => {
    const fixture = resolve(testDir, 'fixtures/validate-bad-hook.ts')
    const { stdout, exitCode } = await runValidate(fixture)

    expect(exitCode).toBe(1)

    const result = JSON.parse(stdout.toString())
    expect(result.valid).toBe(false)
    expect(result.error).toContain('prepare')
    expect(result.error).toContain('not a function')
  }, 15_000)

  it('exits 3 when no script path provided', async () => {
    const result = await runValidate()

    expect(result.exitCode).toBe(3)
    expect(result.stderr).toContain('Usage')
  }, 15_000)
})
