import type { Context } from './context.ts'
import { ReleaseError } from './errors.ts'
import { lastLine } from './process.ts'

/** A release version, vX.Y.Z. Prereleases are not releases. */
export type Version = {
  readonly major: number
  readonly minor: number
  readonly patch: number
}

const RELEASE_TAG = /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/

/**
 * Parses a release tag such as `v1.2.3`; anything else is `undefined`. A
 * component beyond the safe integer range is not a release tag, because it
 * could not be compared exactly.
 */
export const parseReleaseTag = (tag: string): Version | undefined => {
  const match = RELEASE_TAG.exec(tag)
  if (!match) return undefined
  const major = component(match[1])
  const minor = component(match[2])
  const patch = component(match[3])
  if (major === undefined || minor === undefined || patch === undefined) return undefined
  return { major, minor, patch }
}

const component = (digits: string | undefined): number | undefined => {
  const value = Number(digits)
  return Number.isSafeInteger(value) ? value : undefined
}

/** Numeric version order: negative when `a` is lower than `b`. */
export const compareVersions = (a: Version, b: Version): number =>
  a.major - b.major || a.minor - b.minor || a.patch - b.patch

/** `1.2.3`, the form registries use. */
export const formatVersion = (v: Version): string => `${v.major}.${v.minor}.${v.patch}`

/** `v1.2.3`, the form tags use. */
export const formatTag = (v: Version): string => `v${formatVersion(v)}`

/**
 * The release versions among `tags`, highest first, ignoring other tags. A
 * tag in release form that cannot be compared exactly is an error: ignoring
 * it could let a lower tag pass as the highest.
 */
export const releaseVersionsDescending = (tags: readonly string[]): Version[] =>
  tags
    .filter((tag) => RELEASE_TAG.test(tag))
    .map((tag) => {
      const version = parseReleaseTag(tag)
      if (version === undefined) throw new ReleaseError(`cannot compare the release tag ${tag}`)
      return version
    })
    .sort((a, b) => compareVersions(b, a))

/**
 * The tags on `origin`. A failure to read them is an error, never an empty
 * list: an empty list would let any tag pass as the highest.
 */
export const remoteTags = async (ctx: Context): Promise<string[]> => {
  const result = await ctx.run('git', ['ls-remote', '--tags', '--refs', 'origin', 'v*'])
  if (result.code !== 0) {
    throw new ReleaseError(`could not list release tags on origin: ${lastLine(result.stderr)}`)
  }
  return result.stdout
    .split('\n')
    .map((line) => line.split('\t')[1]?.replace(/^refs\/tags\//, ''))
    .filter((tag): tag is string => tag !== undefined && tag !== '')
}
