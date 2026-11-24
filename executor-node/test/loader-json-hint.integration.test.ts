/**
 * Integration test: JSON import attribute error annotation.
 *
 * Spawns a child process under real Node ESM to verify that loadScript()
 * produces the annotated hint when a script uses a bare JSON import.
 * Vitest's Vite transform silently handles bare JSON imports, so only
 * a child process can exercise the actual Node ESM failure path.
 */
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { type ProcessResult, runProcess } from './run-process.js'

const testDir = dirname(fileURLToPath(import.meta.url))
const fixturePath = resolve(testDir, 'fixtures/bare-json-import-child.mjs')

let tmpDir: string

beforeAll(() => {
  tmpDir = mkdtempSync(join(tmpdir(), 'quarry-json-hint-test-'))

  // A real JSON file so Node reaches the attribute check (not "module not found")
  writeFileSync(join(tmpDir, 'data.json'), JSON.stringify({ key: 'value' }))

  // A script with a bare JSON import (missing `with { type: 'json' }`)
  writeFileSync(
    join(tmpDir, 'bare-import.mjs'),
    ["import data from './data.json'", 'export default async function() { return data }', ''].join(
      '\n'
    )
  )
})

afterAll(() => {
  rmSync(tmpDir, { recursive: true, force: true })
})

function runFixture(env?: Record<string, string>): Promise<ProcessResult> {
  return runProcess(process.execPath, [fixturePath], env)
}

describe('JSON import attribute hint (integration)', () => {
  it('annotates ScriptLoadError with actionable hint for bare JSON imports', async () => {
    const { stdout, stderr, exitCode } = await runFixture({
      QUARRY_FIXTURE_DIR: tmpDir
    })

    expect(exitCode, `child stderr: ${stderr}`).toBe(0)
    expect(stdout.toString()).toContain('Failed to load script')
    expect(stdout.toString()).toContain('import failed:')
    expect(stdout.toString()).toContain(
      'Hint: Node ESM requires an import attribute for JSON modules.'
    )
    expect(stdout.toString()).toContain("with { type: 'json' }")
  }, 15_000)
})
