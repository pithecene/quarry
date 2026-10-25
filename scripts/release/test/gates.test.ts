import { describe, expect, it } from 'vitest'
import { assertHighestTag } from '../src/commands/assert-highest-tag.ts'
import { assertReleasePublished } from '../src/commands/assert-release-published.ts'
import { ReleaseError } from '../src/errors.ts'
import { parseReleaseTag, type Version } from '../src/versions.ts'
import { fakeWorld } from './fake-world.ts'

const v = (tag: string): Version => {
  const parsed = parseReleaseTag(tag)
  if (parsed === undefined) throw new Error(`bad test tag ${tag}`)
  return parsed
}

describe('assert-highest-tag', () => {
  const gate = (tags: string[], tag: string) => {
    const world = fakeWorld({ tags })
    return assertHighestTag(world.ctx, v(tag))
  }

  it('passes a tag higher than every released tag', async () => {
    await expect(gate(['v0.13.4', 'v0.13.5', 'v0.13.6'], 'v0.13.7')).resolves.toBeUndefined()
  })

  it('passes a re-run of the highest tag', async () => {
    await expect(gate(['v0.13.4', 'v0.13.5', 'v0.13.6'], 'v0.13.6')).resolves.toBeUndefined()
  })

  it('rejects an older tag', async () => {
    await expect(gate(['v0.13.4', 'v0.13.5', 'v0.13.6'], 'v0.13.5')).rejects.toThrow(
      /lower than the released v0\.13\.6/
    )
  })

  it('compares versions numerically, not lexically', async () => {
    await expect(gate(['v0.9.0', 'v0.10.0'], 'v0.9.1')).rejects.toThrow(ReleaseError)
    await expect(gate(['v0.9.0', 'v0.10.0'], 'v0.11.0')).resolves.toBeUndefined()
  })

  it('ignores tags that are not releases', async () => {
    await expect(gate(['v1.0.0-rc.1', 'latest', 'v0.13.6'], 'v0.13.6')).resolves.toBeUndefined()
  })

  it('fails on a released tag too large to compare exactly', async () => {
    await expect(gate(['v9007199254740993.0.0', 'v0.13.6'], 'v0.13.7')).rejects.toThrow(
      'cannot compare the release tag v9007199254740993.0.0'
    )
  })

  it('passes the first release', async () => {
    await expect(gate([], 'v0.1.0')).resolves.toBeUndefined()
  })

  it('fails instead of passing when the tag list cannot be read', async () => {
    const world = fakeWorld({ tags: ['v0.13.6'], tagsUnreadable: true })
    await expect(assertHighestTag(world.ctx, v('v0.13.7'))).rejects.toThrow(
      /could not list release tags/
    )
  })
})

describe('parseReleaseTag', () => {
  it.each(['0.13.6', 'v1.2', 'v1.2.3-rc.1', 'v01.2.3', '', 'v9007199254740993.0.0'])(
    'rejects %j',
    (tag) => {
      expect(parseReleaseTag(tag)).toBeUndefined()
    }
  )
})

describe('assert-release-published', () => {
  it('passes a published release', async () => {
    const world = fakeWorld({ release: 'published' })
    await expect(assertReleasePublished(world.ctx, 'v1.2.3')).resolves.toBeUndefined()
  })

  it.each([
    ['missing', /is missing/],
    ['draft', /is a draft/],
    ['prerelease', /is a prerelease/]
  ] as const)('rejects a %s release', async (release, message) => {
    const world = fakeWorld({ release })
    await expect(assertReleasePublished(world.ctx, 'v1.2.3')).rejects.toThrow(message)
  })

  it('reports a lookup failure as one, not as a missing release', async () => {
    const world = fakeWorld({ releaseLookupFails: true })
    const error = await assertReleasePublished(world.ctx, 'v1.2.3').catch((e: unknown) => e)
    expect(error).toBeInstanceOf(ReleaseError)
    expect((error as ReleaseError).message).toMatch(/could not read the GitHub release/)
    expect((error as ReleaseError).message).not.toMatch(/missing/)
  })
})
