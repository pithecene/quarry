import { spawnSync } from 'node:child_process'
import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { uploadAssets } from '../src/commands/upload-assets.ts'
import { writeChecksums } from '../src/commands/write-checksums.ts'
import { fakeWorld, type WorldState } from './fake-world.ts'

const POLICY = { attempts: 3, delayMs: 5000, attemptTimeoutMs: 1000 }

const distWorld = (overrides: Partial<WorldState> = {}) => {
  const world = fakeWorld({ release: 'draft', ...overrides })
  const dir = join(world.pkgDir, 'dist')
  mkdirSync(dir)
  for (const name of ['a.tar.gz', 'b.tar.gz']) writeFileSync(join(dir, name), name)
  const uploaded = () =>
    world
      .linesOf('gh')
      .filter((l) => l.startsWith('gh release upload'))
      .map((l) => l.split('/').at(-1))
  return { world, dir, uploaded, upload: () => uploadAssets(world.ctx, 'v1.2.3', dir, POLICY) }
}

describe('upload-assets', () => {
  it('uploads each file once', async () => {
    const { world, dir, upload, uploaded } = distWorld()
    await upload()
    expect(uploaded()).toEqual(['a.tar.gz', 'b.tar.gz'])
    expect(world.linesOf('gh')).toContain(
      `gh release upload --clobber -- v1.2.3 ${join(dir, 'a.tar.gz')}`
    )
  })

  it.each(['fail', 'hang', 'unstartable'] as const)(
    'retries only the file whose upload ended in: %s',
    async (outcome) => {
      const { world, upload, uploaded } = distWorld({
        uploads: new Map([['a.tar.gz', [outcome, outcome]]])
      })
      await upload()
      expect(uploaded()).toEqual(['a.tar.gz', 'b.tar.gz', 'a.tar.gz', 'a.tar.gz'])
      expect(world.sleeps).toEqual([5000, 10000, 20000])
      const uploads = world.calls.filter((c) => c.args[1] === 'upload')
      expect(uploads.every((c) => c.options.timeoutMs === POLICY.attemptTimeoutMs)).toBe(true)
    }
  )

  it('does not upload again a file that GitHub stored before the client failed', async () => {
    const { upload, uploaded } = distWorld({ uploads: new Map([['a.tar.gz', ['stored']]]) })
    await upload()
    expect(uploaded()).toEqual(['a.tar.gz', 'b.tar.gz'])
  })

  it('uploads nothing on a re-run of a published release', async () => {
    const { world, upload, uploaded } = distWorld()
    await upload()
    world.state.release = 'published'
    world.calls.length = 0
    await upload()
    expect(uploaded()).toEqual([])
  })

  it('fails with the file after its attempts run out, and still uploads the rest', async () => {
    const { upload, uploaded } = distWorld({
      uploads: new Map([['a.tar.gz', ['fail', 'fail', 'fail']]])
    })
    await expect(upload()).rejects.toThrow('does not hold as built: a.tar.gz')
    expect(uploaded()).toContain('b.tar.gz')
  })

  it('replaces a differing file of a draft, and never one of a published release', async () => {
    const assets = () => new Map([['a.tar.gz', 'sha256:other']])
    const draft = distWorld({ assets: assets() })
    await draft.upload()
    expect(draft.uploaded()).toContain('a.tar.gz')

    const published = distWorld({ release: 'published', assets: assets() })
    await published.upload()
    expect(published.uploaded()).toEqual([])
  })

  it('does not take an unreadable asset list for missing assets', async () => {
    const { upload, uploaded } = distWorld({ assetReadErrors: 1 })
    await upload()
    expect(uploaded()).toEqual(['a.tar.gz', 'b.tar.gz'])
  })

  it('fails on a missing release', async () => {
    await expect(distWorld({ release: 'missing' }).upload()).rejects.toThrow()
  })

  it('fails on an empty directory', async () => {
    const world = fakeWorld({ release: 'draft' })
    const dir = join(world.pkgDir, 'empty')
    mkdirSync(dir)
    await expect(uploadAssets(world.ctx, 'v1.2.3', dir, POLICY)).rejects.toThrow(
      'no files to upload'
    )
  })
})

describe('write-checksums', () => {
  it('writes files that verify beside a download', async () => {
    const { world, dir } = distWorld()
    await writeChecksums(world.ctx, dir)
    const check = spawnSync('shasum', ['-a', '256', '-c', 'a.tar.gz.sha256', 'b.tar.gz.sha256'], {
      cwd: dir,
      encoding: 'utf8'
    })
    expect(check.stdout).toBe('a.tar.gz: OK\nb.tar.gz: OK\n')
  })
})
