import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { onTestFinished } from 'vitest'
import type { Context } from '../src/context.ts'
import type { RunOptions, RunResult } from '../src/process.ts'

/** Registry and repository state the fake commands read and write. */
export type WorldState = {
  /** Tags on origin, as `git ls-remote` would list them. */
  tags: string[]
  tagsUnreadable: boolean
  /** Versions of the npm package on the registry. */
  npmVersions: Set<string>
  /** Image references (`name:tag`) in the image registry. */
  images: Set<string>
  /** Whether the version is on JSR. */
  jsrPublished: boolean
  /** State of the GitHub release, or `missing`. */
  release: 'published' | 'draft' | 'prerelease' | 'missing'
  releaseLookupFails: boolean
  /** How `pnpm publish` behaves: publishes, fails, or loses a race. */
  pnpmMode: 'ok' | 'fail' | 'race'
  /** How `npx jsr publish` behaves; `poll_fail` uploads but exits non-zero. */
  npxMode: 'ok' | 'fail' | 'poll_fail' | 'hang' | 'hang_exit_0'
  npmDistTagFails: boolean
  /** Lookups that fail with a transport error before the registry answers. */
  npmLookupErrors: number
  imageLookupErrors: number
  jsrLookupErrors: number
  /** HTTP status JSR answers with while `jsrLookupErrors` lasts. */
  jsrErrorStatus: number | 'network'
}

export type Call = {
  readonly command: string
  readonly args: readonly string[]
  readonly options: RunOptions
}

const ok = (stdout = ''): RunResult => ({ code: 0, stdout, stderr: '', timedOut: false })
const fail = (stderr: string): RunResult => ({ code: 1, stdout: '', stderr, timedOut: false })

/** Consumes one injected lookup error from `key`, if any are left. */
const takeError = (
  state: WorldState,
  key: 'npmLookupErrors' | 'imageLookupErrors' | 'jsrLookupErrors'
): boolean => {
  if (state[key] <= 0) return false
  state[key] -= 1
  return true
}

/** The package version the fake package directory declares. */
export const PACKAGE_VERSION = '1.2.3'
export const PACKAGE_NAME = '@scope/pkg'

/**
 * A deterministic stand-in for git, npm, pnpm, npx, docker, gh, and JSR,
 * plus a package directory holding `package.json` and `jsr.json`.
 */
export const fakeWorld = (overrides: Partial<WorldState> = {}) => {
  const state: WorldState = {
    tags: [],
    tagsUnreadable: false,
    npmVersions: new Set(),
    images: new Set(),
    jsrPublished: false,
    release: 'missing',
    releaseLookupFails: false,
    pnpmMode: 'ok',
    npxMode: 'ok',
    npmDistTagFails: false,
    npmLookupErrors: 0,
    imageLookupErrors: 0,
    jsrLookupErrors: 0,
    jsrErrorStatus: 'network',
    ...overrides
  }
  const calls: Call[] = []
  const logs: string[] = []
  const sleeps: number[] = []
  const fetches: string[] = []

  const pkgDir = mkdtempSync(join(tmpdir(), 'release-test-'))
  onTestFinished(() => rmSync(pkgDir, { recursive: true, force: true }))
  writeFileSync(
    join(pkgDir, 'package.json'),
    JSON.stringify({ name: PACKAGE_NAME, version: PACKAGE_VERSION })
  )
  writeFileSync(join(pkgDir, 'jsr.json'), JSON.stringify({ name: PACKAGE_NAME }))

  const handle = (command: string, args: readonly string[]): RunResult => {
    const line = args.join(' ')
    switch (command) {
      case 'git':
        if (state.tagsUnreadable) return fail('fatal: could not read from remote repository')
        return ok(state.tags.map((t) => `0000000\trefs/tags/${t}`).join('\n'))
      case 'npm':
        if (args[0] === 'view') {
          if (takeError(state, 'npmLookupErrors')) return fail('npm error code ECONNREFUSED')
          const version = (args[1] ?? '').split('@').at(-1) ?? ''
          if (state.npmVersions.has(version)) return ok(`${version}\n`)
          return fail('npm error code E404')
        }
        if (args[0] === 'dist-tag')
          return state.npmDistTagFails ? fail('npm error code E403') : ok()
        break
      case 'pnpm':
        if (state.pnpmMode === 'fail') return fail('publish failed')
        state.npmVersions.add(PACKAGE_VERSION)
        return state.pnpmMode === 'race' ? fail('npm error code E409') : ok()
      case 'npx':
        switch (state.npxMode) {
          case 'ok':
            state.jsrPublished = true
            return ok()
          case 'poll_fail':
            state.jsrPublished = true
            return fail('error polling publish status')
          case 'fail':
            return fail('publish rejected')
          case 'hang':
            return { code: null, stdout: '', stderr: '', timedOut: true }
          case 'hang_exit_0':
            return { code: 0, stdout: '', stderr: '', timedOut: true }
        }
        break
      case 'docker':
        if (args[2] === 'inspect') {
          if (takeError(state, 'imageLookupErrors'))
            return fail('ERROR: failed to do request: connection refused')
          const ref = args[3] ?? ''
          return state.images.has(ref) ? ok() : fail(`ERROR: ${ref}: not found`)
        }
        if (args[2] === 'create') return ok()
        break
      case 'gh':
        if (state.releaseLookupFails) return fail('HTTP 401: Bad credentials')
        if (state.release === 'missing') return fail('release not found')
        return ok(
          JSON.stringify({
            isDraft: state.release === 'draft',
            isPrerelease: state.release === 'prerelease'
          })
        )
    }
    throw new Error(`fake world has no behavior for: ${command} ${line}`)
  }

  const ctx: Context = {
    run: async (command, args, options = {}) => {
      calls.push({ command, args, options })
      return handle(command, args)
    },
    fetch: async (url) => {
      fetches.push(url)
      if (takeError(state, 'jsrLookupErrors')) {
        if (state.jsrErrorStatus === 'network') throw new TypeError('fetch failed')
        return { status: state.jsrErrorStatus }
      }
      return { status: state.jsrPublished ? 200 : 404 }
    },
    sleep: async (ms) => {
      sleeps.push(ms)
    },
    log: (message) => {
      logs.push(message)
    }
  }

  /** The calls of one command, as `command arg arg ...` lines. */
  const linesOf = (command: string): string[] =>
    calls.filter((c) => c.command === command).map((c) => [c.command, ...c.args].join(' '))

  return { state, ctx, calls, logs, sleeps, fetches, pkgDir, linesOf }
}
