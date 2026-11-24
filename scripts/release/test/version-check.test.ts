import { spawnSync } from 'node:child_process'
import { cpSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { expect, it, onTestFinished } from 'vitest'

const scripts = resolve(import.meta.dirname, '../..')

const check = (manifest: string, ...args: string[]) => {
  const root = mkdtempSync(join(tmpdir(), 'version-check-'))
  onTestFinished(() => rmSync(root, { recursive: true, force: true }))
  for (const dir of ['scripts', 'quarry/types', 'sdk'])
    mkdirSync(join(root, dir), { recursive: true })
  for (const file of ['version-check.sh', 'version-targets.sh']) {
    cpSync(join(scripts, file), join(root, 'scripts', file))
  }
  writeFileSync(join(root, 'scripts/version-targets.txt'), 'sdk/package.json\t"version": "__V__"\n')
  writeFileSync(join(root, 'quarry/types/version.go'), 'const Version = "0.19.0"\n')
  writeFileSync(join(root, 'sdk/package.json'), manifest)
  return spawnSync('scripts/version-check.sh', args, { cwd: root, encoding: 'utf8' })
}

it.each([[], ['v0.19.0']])('passes a matching version with %j', (...args) => {
  expect(check('{"version": "0.19.0"}\n', ...args.filter((a) => a !== undefined)).status).toBe(0)
})

it('refuses a tag of another version', () => {
  expect(check('{"version": "0.19.0"}\n', 'v0.18.0').status).toBe(1)
})

it("refuses the SDK's own version when only a nested one matches", () => {
  const result = check('{"version":"0.18.0", "config": {"version": "0.19.0"}}\n', 'v0.19.0')
  expect(result.stdout).toContain('sdk/package.json (0.18.0)')
  expect(result.status).toBe(1)
})
