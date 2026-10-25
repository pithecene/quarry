import { describe, expect, it } from 'vitest'
import { convergePointers } from '../src/commands/converge-pointers.ts'
import type { RetryPolicy } from '../src/lookup.ts'
import { fakeWorld, type WorldState } from './fake-world.ts'

const IMAGE = 'ghcr.io/x/y'
const LOOKUPS: RetryPolicy = { attempts: 3, delayMs: 5000 }

/** Both images of each version, as a release pushes them. */
const imagesOf = (...versions: string[]): Set<string> =>
  new Set(versions.flatMap((v) => [`${IMAGE}:${v}`, `${IMAGE}:${v}-slim`]))

const setup = (overrides: Partial<WorldState>) => {
  const world = fakeWorld(overrides)
  const converge = () => convergePointers(world.ctx, world.pkgDir, IMAGE, LOOKUPS)
  const npmLatest = () =>
    world
      .linesOf('npm')
      .filter((l) => l.startsWith('npm dist-tag add'))
      .map((l) => l.split(' ')[3])
  const alias = (name: 'latest' | 'slim') =>
    world.calls
      .filter(
        (c) => c.command === 'docker' && c.args[2] === 'create' && c.args[4] === `${IMAGE}:${name}`
      )
      .map((c) => c.args[5])
  return { world, converge, npmLatest, alias }
}

describe('converge-pointers', () => {
  it('points every pointer at the highest published release', async () => {
    const t = setup({
      tags: ['v1.2.2', 'v1.2.3', 'junk', 'v1.3.0-rc.1'],
      npmVersions: new Set(['1.2.2', '1.2.3']),
      images: imagesOf('1.2.2', '1.2.3')
    })
    await t.converge()
    expect(t.npmLatest()).toEqual(['@scope/pkg@1.2.3'])
    expect(t.alias('latest')).toEqual([`${IMAGE}:1.2.3`])
    expect(t.alias('slim')).toEqual([`${IMAGE}:1.2.3-slim`])
  })

  it('orders versions numerically', async () => {
    const t = setup({
      tags: ['v0.9.0', 'v0.10.0'],
      npmVersions: new Set(['0.9.0', '0.10.0']),
      images: imagesOf('0.9.0', '0.10.0')
    })
    await t.converge()
    expect(t.npmLatest()).toEqual(['@scope/pkg@0.10.0'])
    expect(t.alias('latest')).toEqual([`${IMAGE}:0.10.0`])
  })

  it('keeps pointers on the highest published release while a newer tag is unpublished', async () => {
    const t = setup({
      tags: ['v1.2.3', 'v1.3.0'],
      npmVersions: new Set(['1.2.3']),
      images: imagesOf('1.2.3')
    })
    await t.converge()
    expect(t.npmLatest()).toEqual(['@scope/pkg@1.2.3'])
    expect(t.alias('latest')).toEqual([`${IMAGE}:1.2.3`])
    expect(t.world.linesOf('npm').filter((l) => l.startsWith('npm view'))).toHaveLength(2)
  })

  it('converges each registry independently, and images only when both are pushed', async () => {
    const t = setup({
      tags: ['v1.2.3', 'v1.3.0'],
      npmVersions: new Set(['1.2.3', '1.3.0']),
      images: new Set([...imagesOf('1.2.3'), `${IMAGE}:1.3.0`])
    })
    await t.converge()
    expect(t.npmLatest()).toEqual(['@scope/pkg@1.3.0'])
    expect(t.alias('latest')).toEqual([`${IMAGE}:1.2.3`])
  })

  it('is idempotent and never points at an older release', async () => {
    const t = setup({
      tags: ['v1.2.3', 'v1.3.0'],
      npmVersions: new Set(['1.2.3', '1.3.0']),
      images: imagesOf('1.2.3', '1.3.0')
    })
    await t.converge()
    await t.converge()
    expect(t.npmLatest()).toEqual(['@scope/pkg@1.3.0', '@scope/pkg@1.3.0'])
  })

  it('leaves pointers unchanged when nothing is published', async () => {
    const t = setup({ tags: ['v1.2.3'] })
    await t.converge()
    expect(t.npmLatest()).toEqual([])
    expect(t.alias('latest')).toEqual([])
  })

  it('fails on an npm pointer failure, after converging the images', async () => {
    const t = setup({
      tags: ['v1.2.3'],
      npmVersions: new Set(['1.2.3']),
      images: imagesOf('1.2.3'),
      npmDistTagFails: true
    })
    await expect(t.converge()).rejects.toThrow('failed to converge: npm latest')
    expect(t.alias('latest')).toEqual([`${IMAGE}:1.2.3`])
  })

  it('fails without writing when the tag list cannot be read', async () => {
    const t = setup({ tagsUnreadable: true })
    await expect(t.converge()).rejects.toThrow(/could not list release tags/)
    expect(t.world.calls.filter((c) => c.command !== 'git')).toHaveLength(0)
  })

  it('fails when origin has no release tags', async () => {
    const t = setup({ tags: ['latest'] })
    await expect(t.converge()).rejects.toThrow('no release tags on origin')
  })

  describe('registry lookup failures', () => {
    const both = {
      tags: ['v1.2.3', 'v1.3.0'],
      npmVersions: new Set(['1.2.3', '1.3.0']),
      images: imagesOf('1.2.3', '1.3.0')
    }

    it('retries a transient npm lookup failure', async () => {
      const t = setup({ ...both, npmLookupErrors: 2 })
      await t.converge()
      expect(t.npmLatest()).toEqual(['@scope/pkg@1.3.0'])
    })

    it('fails on a persistent npm lookup failure without falling back to an older version', async () => {
      const t = setup({ ...both, npmLookupErrors: 3 })
      await expect(t.converge()).rejects.toThrow('failed to converge: npm latest')
      expect(t.npmLatest()).toEqual([])
      expect(t.world.linesOf('npm')).not.toContain('npm view @scope/pkg@1.2.3 version')
      expect(t.alias('latest')).toEqual([`${IMAGE}:1.3.0`])
    })

    it('retries a transient image lookup failure', async () => {
      const t = setup({ ...both, imageLookupErrors: 2 })
      await t.converge()
      expect(t.alias('latest')).toEqual([`${IMAGE}:1.3.0`])
    })

    it('fails on a persistent image lookup failure without falling back to an older version', async () => {
      const t = setup({ ...both, imageLookupErrors: 3 })
      await expect(t.converge()).rejects.toThrow('failed to converge: GHCR :latest/:slim')
      expect(t.alias('latest')).toEqual([])
      expect(t.world.linesOf('docker')).not.toContain(
        `docker buildx imagetools inspect ${IMAGE}:1.2.3`
      )
      expect(t.npmLatest()).toEqual(['@scope/pkg@1.3.0'])
    })
  })
})
