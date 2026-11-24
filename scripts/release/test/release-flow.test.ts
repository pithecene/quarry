import { describe, expect, it } from 'vitest'
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

  it.each(['draft', 'published'] as const)('keeps a %s release on a re-run', async (release) => {
    const world = fakeWorld({ release })
    await draftRelease(world.ctx, TAG)
    expect(world.state.release).toBe(release)
    expect(world.linesOf('gh')).toHaveLength(1)
  })

  it('rejects a prerelease', async () => {
    const world = fakeWorld({ release: 'prerelease' })
    await expect(draftRelease(world.ctx, TAG)).rejects.toThrow(/prerelease/)
  })
})

describe('cli', () => {
  it('refuses a tag that reads as an option', async () => {
    const world = fakeWorld({ release: 'draft' })
    expect(await main(['upload-assets', '--help', world.pkgDir], {}, world.ctx)).toBe(2)
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
