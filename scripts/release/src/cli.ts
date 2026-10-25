#!/usr/bin/env node
/**
 * Release steps for .github/workflows/release.yml and release-dry-run.yml.
 *
 * Usage: node scripts/release/src/cli.ts <command> [arguments]
 *
 * Runs directly on the pinned Node (type stripping, no build step) and
 * imports only Node built-ins, so a workflow job needs no install.
 *
 * Exit codes: 0 success, 1 a release step failed, 2 invalid usage.
 */

import { setTimeout as sleep } from 'node:timers/promises'
import { assertHighestTag } from './commands/assert-highest-tag.ts'
import { assertReleasePublished } from './commands/assert-release-published.ts'
import { convergePointers } from './commands/converge-pointers.ts'
import { publishJsr } from './commands/publish-jsr.ts'
import { publishNpm } from './commands/publish-npm.ts'
import type { Context } from './context.ts'
import { describeError, ReleaseError, UsageError } from './errors.ts'
import type { RetryPolicy } from './lookup.ts'
import { spawnRun } from './process.ts'
import { parseReleaseTag, type Version } from './versions.ts'

type Env = Readonly<Record<string, string | undefined>>

type Command = {
  readonly usage: string
  readonly run: (ctx: Context, args: readonly string[], env: Env) => Promise<void>
}

type Bounds = { readonly fallback: number; readonly min: number; readonly max: number }

/**
 * A whole number from the environment within `[min, max]`, or `fallback`
 * when unset. The bounds keep a typo from turning a retry loop into one
 * that never ends.
 */
const envInt = (env: Env, name: string, { fallback, min, max }: Bounds): number => {
  const raw = env[name]
  if (raw === undefined || raw === '') return fallback
  const value = /^\d+$/.test(raw) ? Number(raw) : Number.NaN
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new UsageError(`${name} must be a whole number from ${min} to ${max}, got "${raw}"`)
  }
  return value
}

const ATTEMPTS: Omit<Bounds, 'fallback'> = { min: 1, max: 20 }
const DELAY_SECONDS: Omit<Bounds, 'fallback'> = { min: 0, max: 600 }

/** Registry lookups: PROBE_ATTEMPTS (3) and PROBE_RETRY_DELAY (5 s, doubled per retry). */
const lookupPolicy = (env: Env): RetryPolicy => ({
  attempts: envInt(env, 'PROBE_ATTEMPTS', { ...ATTEMPTS, fallback: 3 }),
  delayMs: envInt(env, 'PROBE_RETRY_DELAY', { ...DELAY_SECONDS, fallback: 5 }) * 1000
})

const releaseTag = (raw: string): Version => {
  const tag = parseReleaseTag(raw)
  if (tag === undefined) throw new UsageError(`not a release tag: ${raw} (expected vX.Y.Z)`)
  return tag
}

/** Exactly one non-empty positional argument per name, keyed by name. */
const positionals = <const K extends string>(
  args: readonly string[],
  names: readonly K[]
): Record<K, string> => {
  if (args.length !== names.length || args.some((a) => a === '')) {
    throw new UsageError(`expected ${names.map((n) => `<${n}>`).join(' ')}`)
  }
  return Object.fromEntries(names.map((n, i) => [n, args[i]])) as Record<K, string>
}

// A Map, so a name such as `toString` or `__proto__` is not a command.
const COMMANDS: ReadonlyMap<string, Command> = new Map(
  Object.entries({
    'assert-highest-tag': {
      usage: 'assert-highest-tag <tag>',
      run: (ctx, args) => {
        const { tag } = positionals(args, ['tag'])
        return assertHighestTag(ctx, releaseTag(tag))
      }
    },
    'assert-release-published': {
      usage: 'assert-release-published <tag>',
      run: (ctx, args) => {
        const { tag } = positionals(args, ['tag'])
        return assertReleasePublished(ctx, tag)
      }
    },
    'publish-npm': {
      usage: 'publish-npm <package-dir>',
      run: (ctx, args, env) => {
        const { 'package-dir': dir } = positionals(args, ['package-dir'])
        return publishNpm(ctx, dir, lookupPolicy(env))
      }
    },
    'publish-jsr': {
      usage:
        'publish-jsr <package-dir> <version>   (JSR_ATTEMPTS 3, JSR_ATTEMPT_TIMEOUT 480 s, JSR_RETRY_DELAY 10 s)',
      run: (ctx, args, env) => {
        const { 'package-dir': dir, version } = positionals(args, ['package-dir', 'version'])
        const policy = {
          attempts: envInt(env, 'JSR_ATTEMPTS', { ...ATTEMPTS, fallback: 3 }),
          attemptTimeoutMs:
            envInt(env, 'JSR_ATTEMPT_TIMEOUT', { min: 1, max: 3600, fallback: 480 }) * 1000,
          retryDelayMs: envInt(env, 'JSR_RETRY_DELAY', { ...DELAY_SECONDS, fallback: 10 }) * 1000
        }
        return publishJsr(ctx, dir, version, policy, lookupPolicy(env))
      }
    },
    'converge-pointers': {
      usage: 'converge-pointers <npm-package-dir> <image>',
      run: (ctx, args, env) => {
        const { 'npm-package-dir': dir, image } = positionals(args, ['npm-package-dir', 'image'])
        return convergePointers(ctx, dir, image, lookupPolicy(env))
      }
    }
  } satisfies Record<string, Command>)
)

const usage = (): string =>
  [
    'usage: node scripts/release/src/cli.ts <command> [arguments]',
    '',
    'commands:',
    ...[...COMMANDS.values()].map((c) => `  ${c.usage}`),
    '',
    'Registry lookups retry per PROBE_ATTEMPTS (3) and PROBE_RETRY_DELAY (5 s).'
  ].join('\n')

/** Runs one release command and returns the process exit code. */
export const main = async (argv: readonly string[], env: Env, ctx: Context): Promise<number> => {
  const [name, ...args] = argv
  const command = name === undefined ? undefined : COMMANDS.get(name)
  try {
    if (command === undefined) {
      throw new UsageError(name === undefined ? 'no command given' : `unknown command: ${name}`)
    }
    await command.run(ctx, args, env)
    return 0
  } catch (error) {
    if (error instanceof UsageError) {
      process.stderr.write(`::error::${error.message}\n\n${usage()}\n`)
      return 2
    }
    // A release step's own failure carries its message; anything else is a
    // defect in the tool, reported with its stack.
    const message =
      error instanceof ReleaseError ? error.message : `unexpected error: ${describeError(error)}`
    process.stderr.write(`::error::${message}\n`)
    if (!(error instanceof ReleaseError) && error instanceof Error && error.stack) {
      process.stderr.write(`${error.stack}\n`)
    }
    return 1
  }
}

if (import.meta.main) {
  const ctx: Context = {
    run: spawnRun,
    fetch: (url, init) => fetch(url, init),
    sleep: (ms) => sleep(ms),
    log: (message) => process.stderr.write(`${message}\n`)
  }
  process.exitCode = await main(process.argv.slice(2), process.env, ctx)
}
