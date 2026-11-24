import { describe, expect, it, onTestFinished, vi } from 'vitest'
import { main } from '../src/cli.ts'
import { assertReleaseNotes } from '../src/commands/assert-release-notes.ts'
import { draftRelease } from '../src/commands/draft-release.ts'
import { publishRelease } from '../src/commands/publish-release.ts'
import { fakeWorld } from './fake-world.ts'

const TAG = 'v1.2.3'

describe('assert-release-notes', () => {
  it('passes an annotated tag that holds notes', async () => {
    const world = fakeWorld()
    await expect(assertReleaseNotes(world.ctx, TAG)).resolves.toBeUndefined()
    // actions/checkout flattens the tag, so the command fetches it first.
    expect(world.linesOf('git')[0]).toBe(
      'git fetch --quiet --force origin refs/tags/v1.2.3:refs/tags/v1.2.3'
    )
  })

  it.each([
    ['a lightweight tag', { tagType: 'commit' }, /lightweight tag/],
    ['a tag that moved off the checked-out commit', { tagCommit: 'c1' }, /no longer names/],
    ['an empty message', { tagMessage: '\n' }, /holds no release notes/],
    [
      'a message that is only a signature',
      { tagMessage: '-----BEGIN PGP SIGNATURE-----\nx' },
      /holds no release notes/
    ]
  ] as const)('rejects %s', async (_name, state, message) => {
    const world = fakeWorld(state)
    await expect(assertReleaseNotes(world.ctx, TAG)).rejects.toThrow(message)
  })
})

describe('draft-release', () => {
  it('creates a draft with the notes of the tag, without its signature', async () => {
    const world = fakeWorld({ tagMessage: 'Title\n\nBody\n-----BEGIN SSH SIGNATURE-----\nx' })
    await draftRelease(world.ctx, TAG)
    expect(world.calls.at(-1)?.args).toEqual([
      'release',
      'create',
      TAG,
      '--draft',
      '--verify-tag',
      '--title',
      TAG,
      '--notes',
      'Title\n\nBody'
    ])
  })

  it('leaves a published release alone', async () => {
    const world = fakeWorld({ release: 'published' })
    await draftRelease(world.ctx, TAG)
    expect(world.linesOf('gh')).toHaveLength(1)
  })

  it('gives an existing draft the notes of the tag', async () => {
    const world = fakeWorld({ release: 'draft', tagMessage: 'Notes' })
    await draftRelease(world.ctx, TAG)
    expect(world.linesOf('gh').at(-1)).toBe('gh release edit v1.2.3 --title v1.2.3 --notes Notes')
    expect(world.state.release).toBe('draft')
  })

  it.each(['prerelease', 'draft prerelease'] as const)('rejects a %s', async (release) => {
    const world = fakeWorld({ release })
    await expect(draftRelease(world.ctx, TAG)).rejects.toThrow(/prerelease/)
  })
})

describe('cli', () => {
  it('refuses a tag that reads as an option', async () => {
    const world = fakeWorld({ release: 'draft' })
    const stderr = vi.spyOn(process.stderr, 'write').mockReturnValue(true)
    onTestFinished(() => stderr.mockRestore())
    expect(await main(['upload-assets', '--help', world.pkgDir], {}, world.ctx)).toBe(2)
    expect(String(stderr.mock.calls[0]?.[0])).toContain('not a release tag: --help')
    expect(world.linesOf('gh')).toEqual([])
  })
})

describe('publish-release', () => {
  it('publishes a draft', async () => {
    const world = fakeWorld({ release: 'draft' })
    await publishRelease(world.ctx, TAG)
    expect(world.state.release).toBe('published')
  })

  it('leaves a published release alone', async () => {
    const world = fakeWorld({ release: 'published' })
    await publishRelease(world.ctx, TAG)
    expect(world.linesOf('gh')).toHaveLength(1)
  })

  it('reports a lookup failure as one, not as a missing release', async () => {
    const world = fakeWorld({ releaseLookupFails: true })
    await expect(publishRelease(world.ctx, TAG)).rejects.toThrow(
      /could not read the GitHub release/
    )
  })

  it('fails on a missing release', async () => {
    await expect(publishRelease(fakeWorld().ctx, TAG)).rejects.toThrow(/missing/)
  })
})
