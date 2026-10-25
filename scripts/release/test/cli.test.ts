import { spawnSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { main } from '../src/cli.ts'
import { fakeWorld } from './fake-world.ts'

const CLI = fileURLToPath(new URL('../src/cli.ts', import.meta.url))

describe('main', () => {
  const stderr = () => {
    const spy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true)
    return () => spy.mock.calls.map(([chunk]) => String(chunk)).join('')
  }
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('exits 2 with usage when no command is given', async () => {
    const output = stderr()
    expect(await main([], {}, fakeWorld().ctx)).toBe(2)
    expect(output()).toMatch(/no command given[\s\S]*usage:/)
  })

  it.each(['toString', 'constructor', '__proto__', 'hasOwnProperty'])(
    'exits 2 for the inherited name %j',
    async (name) => {
      const output = stderr()
      expect(await main([name], {}, fakeWorld().ctx)).toBe(2)
      expect(output()).toContain(`::error::unknown command: ${name}`)
    }
  )

  it('exits 2 for an unknown command', async () => {
    const output = stderr()
    expect(await main(['publish-everything'], {}, fakeWorld().ctx)).toBe(2)
    expect(output()).toContain('::error::unknown command: publish-everything')
  })

  it.each([
    [['assert-highest-tag'], /expected <tag>/],
    [['assert-highest-tag', 'v1.2.3', 'extra'], /expected <tag>/],
    [['assert-highest-tag', '1.2.3'], /not a release tag: 1\.2\.3/],
    [['publish-jsr', 'sdk'], /expected <package-dir> <version>/]
  ])('exits 2 for invalid arguments %j', async (argv, message) => {
    const output = stderr()
    expect(await main(argv, {}, fakeWorld().ctx)).toBe(2)
    expect(output()).toMatch(message)
  })

  it.each([
    ['PROBE_ATTEMPTS', '0'],
    ['PROBE_ATTEMPTS', 'three'],
    ['PROBE_RETRY_DELAY', '-1'],
    ['JSR_ATTEMPTS', '1.5'],
    ['PROBE_ATTEMPTS', '9'.repeat(400)],
    ['PROBE_ATTEMPTS', '21'],
    ['JSR_ATTEMPT_TIMEOUT', '0']
  ])('exits 2 for an invalid %s of %j', async (name, value) => {
    const world = fakeWorld({ jsrPublished: true })
    const output = stderr()
    expect(await main(['publish-jsr', world.pkgDir, '1.2.3'], { [name]: value }, world.ctx)).toBe(2)
    expect(output()).toContain(`${name} must be`)
  })

  it('applies the lookup tunables', async () => {
    const world = fakeWorld({ jsrPublished: true, jsrLookupErrors: 1 })
    stderr()
    const env = { PROBE_ATTEMPTS: '2', PROBE_RETRY_DELAY: '7' }
    expect(await main(['publish-jsr', world.pkgDir, '1.2.3'], env, world.ctx)).toBe(0)
    expect(world.sleeps).toEqual([7000])
  })

  it('exits 1 with an error annotation when a release step fails', async () => {
    const world = fakeWorld({ tags: ['v2.0.0'] })
    const output = stderr()
    expect(await main(['assert-highest-tag', 'v1.0.0'], {}, world.ctx)).toBe(1)
    expect(output()).toMatch(/^::error::v1\.0\.0 is lower than the released v2\.0\.0/)
  })

  it('reports an unexpected error with its stack', async () => {
    const world = fakeWorld()
    const ctx = { ...world.ctx, run: () => Promise.reject(new Error('spawn git ENOENT')) }
    const output = stderr()
    expect(await main(['assert-highest-tag', 'v1.0.0'], {}, ctx)).toBe(1)
    expect(output()).toContain('::error::unexpected error: spawn git ENOENT')
    expect(output()).toContain('at ')
  })
})

describe('entry point', () => {
  it('runs on this Node without a build step', () => {
    const result = spawnSync(process.execPath, [CLI], { encoding: 'utf8', timeout: 30_000 })
    expect(result.error).toBeUndefined()
    expect(result.signal).toBeNull()
    expect(result.status).toBe(2)
    expect(result.stderr).toContain('usage: node scripts/release/src/cli.ts')
  })
})
