import { describe, expect, it } from 'vitest'
import { type JsrPublishPolicy, publishJsr } from '../src/commands/publish-jsr.ts'
import { publishNpm } from '../src/commands/publish-npm.ts'
import type { RetryPolicy } from '../src/lookup.ts'
import { fakeWorld, PACKAGE_VERSION } from './fake-world.ts'

const LOOKUPS: RetryPolicy = { attempts: 3, delayMs: 5000 }

describe('publish-npm', () => {
  const publish = (world: ReturnType<typeof fakeWorld>) =>
    publishNpm(world.ctx, world.pkgDir, LOOKUPS)

  it('succeeds without republishing an already published version', async () => {
    const world = fakeWorld({ npmVersions: new Set([PACKAGE_VERSION]) })
    await publish(world)
    expect(world.linesOf('pnpm')).toHaveLength(0)
  })

  it('publishes an unpublished version once, under the release dist-tag', async () => {
    const world = fakeWorld()
    await publish(world)
    expect(world.linesOf('pnpm')).toEqual([
      'pnpm publish --access restricted --no-git-checks --tag release'
    ])
    expect(world.calls.find((c) => c.command === 'pnpm')?.options).toMatchObject({
      cwd: world.pkgDir
    })
  })

  it('never touches latest', async () => {
    const world = fakeWorld()
    await publish(world)
    expect(world.calls.flatMap((c) => c.args)).not.toContain('latest')
  })

  it('counts a concurrent publish as success', async () => {
    const world = fakeWorld({ pnpmMode: 'race' })
    await expect(publish(world)).resolves.toBeUndefined()
  })

  it('fails on a real publish failure', async () => {
    const world = fakeWorld({ pnpmMode: 'fail' })
    await expect(publish(world)).rejects.toThrow('failed to publish @scope/pkg@1.2.3')
  })

  it('retries a transient lookup failure and does not publish a version it then finds', async () => {
    const world = fakeWorld({ npmVersions: new Set([PACKAGE_VERSION]), npmLookupErrors: 2 })
    await publish(world)
    expect(world.linesOf('pnpm')).toHaveLength(0)
    expect(world.sleeps).toEqual([5000, 10000])
  })

  it('still publishes once when the registry cannot be read', async () => {
    const world = fakeWorld({ npmLookupErrors: 3 })
    await publish(world)
    expect(world.linesOf('pnpm')).toHaveLength(1)
  })

  it('does not take an unreadable registry after a failed publish as success', async () => {
    const world = fakeWorld({ pnpmMode: 'fail', npmLookupErrors: 6 })
    await expect(publish(world)).rejects.toThrow(/could not check the registry/)
  })
})

describe('publish-jsr', () => {
  const POLICY: JsrPublishPolicy = { attempts: 3, attemptTimeoutMs: 480_000, retryDelayMs: 10_000 }
  const publish = (world: ReturnType<typeof fakeWorld>, policy: Partial<JsrPublishPolicy> = {}) =>
    publishJsr(world.ctx, world.pkgDir, PACKAGE_VERSION, { ...POLICY, ...policy }, LOOKUPS)

  it('reads the per-version metadata on jsr.io', async () => {
    const world = fakeWorld({ jsrPublished: true })
    await publish(world)
    expect(world.fetches).toEqual(['https://jsr.io/@scope/pkg/1.2.3_meta.json'])
  })

  it('succeeds without republishing an already published version', async () => {
    const world = fakeWorld({ jsrPublished: true })
    await publish(world)
    expect(world.linesOf('npx')).toHaveLength(0)
  })

  it('publishes an unpublished version once, time-limited', async () => {
    const world = fakeWorld()
    await publish(world)
    expect(world.linesOf('npx')).toEqual(['npx --yes jsr publish --set-version 1.2.3'])
    expect(world.calls.find((c) => c.command === 'npx')?.options).toMatchObject({
      cwd: world.pkgDir,
      timeoutMs: 480_000
    })
  })

  it('counts an accepted upload whose status poll failed as success, without retrying', async () => {
    const world = fakeWorld({ npxMode: 'poll_fail' })
    await publish(world)
    expect(world.linesOf('npx')).toHaveLength(1)
  })

  it('fails after the configured attempts, backing off between them', async () => {
    const world = fakeWorld({ npxMode: 'fail' })
    await expect(publish(world)).rejects.toThrow('after 3 attempts')
    expect(world.linesOf('npx')).toHaveLength(3)
    expect(world.sleeps).toEqual([10_000, 20_000])
  })

  it('retries each stalled attempt and then fails', async () => {
    const world = fakeWorld({ npxMode: 'hang' })
    await expect(publish(world, { attempts: 2 })).rejects.toThrow('after 2 attempts')
    expect(world.linesOf('npx')).toHaveLength(2)
  })

  it('does not take a timed-out attempt that exited 0 as published', async () => {
    const world = fakeWorld({ npxMode: 'hang_exit_0' })
    await expect(publish(world, { attempts: 2 })).rejects.toThrow('after 2 attempts')
    expect(world.linesOf('npx')).toHaveLength(2)
  })

  it('retries a transient lookup failure and does not publish a version it then finds', async () => {
    const world = fakeWorld({ jsrPublished: true, jsrLookupErrors: 2 })
    await publish(world)
    expect(world.linesOf('npx')).toHaveLength(0)
  })

  it('treats a non-404 error status as a failed lookup, not as absent', async () => {
    const world = fakeWorld({ jsrPublished: true, jsrLookupErrors: 2, jsrErrorStatus: 503 })
    await publish(world)
    expect(world.linesOf('npx')).toHaveLength(0)
  })

  it('still publishes once when JSR cannot be read', async () => {
    const world = fakeWorld({ jsrLookupErrors: 3 })
    await publish(world)
    expect(world.linesOf('npx')).toHaveLength(1)
  })

  it('attempts again when a failed attempt cannot be confirmed', async () => {
    const world = fakeWorld({ npxMode: 'poll_fail', jsrLookupErrors: 6 })
    await publish(world, { attempts: 2 })
    expect(world.linesOf('npx')).toHaveLength(2)
  })
})
