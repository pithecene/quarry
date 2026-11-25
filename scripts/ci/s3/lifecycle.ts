// The lifecycle of runs on an S3 service: write, notification, SDK read, prune.
// Usage: lifecycle.ts <quarry binary> <endpoint>, with AWS_* credentials.

import assert from 'node:assert/strict'
import { once } from 'node:events'
import { readFile, rm, writeFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { join } from 'node:path'
import {
  listRuns,
  type NotificationLocator,
  type RunLocator,
  readRun
} from '../../../sdk/src/index.ts'
import {
  BUDGET_MS,
  dir,
  flagsAt,
  quarry,
  quarryPath,
  s3,
  signal,
  storageAt,
  storedKeys
} from './service.ts'

const PREFIX = 'pre fix'
const storage = storageAt(PREFIX)
const flags = flagsAt(PREFIX)

const received: string[] = []
const server = createServer(async (req, res) => {
  let body = ''
  for await (const chunk of req) body += chunk
  received.push(body)
  res.writeHead(204).end()
})

const run = (script: string, id: string, policy: string[] = []) =>
  quarry(
    ...['run', '--script', join(dir, script), '--run-id', id, '--source', 'a b'],
    ...['--category', 'c/d', '--no-browser', ...flags, '--adapter', 'webhook', '--adapter-url'],
    `http://127.0.0.1:${(server.address() as AddressInfo).port}/`,
    ...policy
  )

/**
 * The keys that a prune leaves of `before`: the kept run's, the head pointer,
 * a tombstone for each pruned snapshot, and the fence of each removed Volume.
 */
function expectedRemainder(before: string[], keptDay: string): string[] {
  const dataset = 'datasets/quarry/'
  const partition = `${dataset}partitions/source=a%20b/category=c%2Fd/day=${keptDay}/run_id=run%20failed/`
  const runs = `${dataset}runs/a%20b/c%2Fd/${keptDay}/run%20failed/`
  const snapshot = (key: string) => /\/segments\/([^/]+)\//.exec(key)?.[1] ?? ''
  const kept = new Set(before.filter((key) => key.startsWith(partition)).map(snapshot))
  const canonical = before.filter((key) => key.startsWith(`${dataset}segments/`))
  return [
    ...before.filter((key) => key.startsWith(partition) || key.startsWith(runs)),
    ...canonical.filter((key) => kept.has(snapshot(key))),
    ...canonical
      .map(snapshot)
      .filter((id) => !kept.has(id))
      .map((id) => `${dataset}expired/${id}`),
    ...before.filter((key) => /^artifacts\/.*\/volumes\/[^/]+\/latest$/.test(key)),
    `${dataset}latest`
  ].sort()
}

/** Reads a run's artifact back and compares every byte. */
async function assertArtifact(key: RunLocator, id: string, want: Buffer): Promise<void> {
  const out = join(dir, `${key.runId}.bin`)
  const result = await quarry(
    ...['read', 'artifact', ...flags, `--source=${key.source}`, `--category=${key.category}`],
    ...[`--day=${key.day}`, `--run-id=${key.runId}`, `--artifact-id=${id}`, `--out=${out}`]
  )
  assert.equal(result.code, 0, result.err)
  assert.ok((await readFile(out)).equals(want), 'the artifact bytes differ')
}

async function prune(...args: string[]): Promise<Record<string, number>> {
  const result = await quarry('prune', '--format=json', ...flags, ...args)
  assert.equal(result.code, 0, result.err)
  return JSON.parse(result.out) as Record<string, number>
}

const locators: NotificationLocator[] = []

async function main(): Promise<void> {
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  await writeFile(
    join(dir, 'ok.mjs'),
    `export default async function run(ctx) {
      for (let n = 0; n < 5; n++) await ctx.emit.item({ item_type: 'x', data: { n } })
      await ctx.storage.put({ filename: 'a.txt', content_type: 'text/plain', data: Buffer.from('a') })
      await ctx.emit.artifact({ name: 'a.bin', content_type: 'application/octet-stream', data: Buffer.from(Uint32Array.from({ length: 9 << 18 }, (_, n) => n ^ (ctx.run.run_id.length << 24)).buffer) })
      await ctx.emit.runComplete({})
    }`
  )
  await writeFile(
    join(dir, 'bad.mjs'),
    "export default async function run() { throw new Error('deliberate') }"
  )

  const policies = { strict: [], streaming: ['--policy', 'streaming', '--flush-count', '2'] }
  for (const [id, policy] of Object.entries(policies)) {
    const result = await run('ok.mjs', `run ${id}`, policy)
    assert.equal(result.code, 0, result.err)
    const body = received.at(-1) ?? ''
    assert.equal(received.length, locators.length + 1, 'one notification per run')
    assert.ok(!body.includes(process.env.AWS_SECRET_ACCESS_KEY ?? ''), 'no credential is sent')
    const { locator } = JSON.parse(body) as { locator: NotificationLocator }
    assert.deepEqual(locator.storage, { ...storage, dataset: 'quarry' })
    assert.equal(locator.run.runId, `run ${id}`)
    const read = await readRun(locator.run, { storage: locator.storage, quarryPath, signal })
    assert.equal(read.run.run_id, `run ${id}`)
    assert.equal(read.run.state, 'succeeded')
    assert.equal(read.events.length, 7, 'five items, an artifact, and the terminal event')
    // Each word names its run and its offset, across the 8 MiB range boundary.
    const words = Uint32Array.from(
      { length: 9 << 18 },
      (_, n) => n ^ (locator.run.runId.length << 24)
    )
    const artifact = read.events.find((event) => event.artifact_id !== undefined)?.artifact_id
    await assertArtifact(locator.run, String(artifact), Buffer.from(words.buffer))
    locators.push(locator)
  }
  // The day is part of a run's key, so midnight would make the retry another run.
  const untilMidnight = 86_400_000 - (Date.now() % 86_400_000)
  if (untilMidnight > BUDGET_MS && locators[0]?.run.day === new Date().toISOString().slice(0, 10)) {
    const duplicate = await run('ok.mjs', 'run strict')
    assert.equal(duplicate.code, 2, duplicate.err)
    assert.match(duplicate.err, /already (exists|used|recorded)|duplicate/i)
    assert.equal(received.length, locators.length, 'a refused run sends no notification')
  }
  assert.equal((await run('bad.mjs', 'run failed')).code, 1)
  const failed = (JSON.parse(received.at(-1) ?? '') as { locator: NotificationLocator }).locator
  const runs = await listRuns({ storage, quarryPath, signal })
  assert.equal(runs.runs.length, 3)

  const due = ['--succeeded-days=0', '--failed-days=0']
  const before = await storedKeys(PREFIX)
  const planned = await prune('--dry-run', ...due)
  const pruned = await prune(...due)
  const again = await prune(...due)
  // Each success has a sidecar with its companion, and one artifact Volume. The
  // failed run committed last, so it holds the dataset's head.
  const removed = { runs_pruned: 2, runs_kept: 1, sidecar_objects: 4, volumes: 2 }
  for (const [counter, want] of Object.entries(removed)) {
    assert.equal(pruned[counter], want, counter)
    assert.equal(again[counter], counter === 'runs_kept' ? 1 : 0, `second prune: ${counter}`)
  }
  for (const counter of [...Object.keys(removed), 'objects']) {
    assert.equal(planned[counter], pruned[counter], `dry run: ${counter}`)
  }
  assert.equal(again.objects, 0, 'a second prune removes nothing')
  for (const locator of locators) {
    await assert.rejects(readRun(locator.run, { storage, quarryPath, signal }), /run not found/)
  }
  assert.equal(failed.run.runId, 'run failed')
  assert.deepEqual(failed.storage, { ...storage, dataset: 'quarry' })
  const kept = await readRun(failed.run, { storage: failed.storage, quarryPath, signal })
  assert.equal(kept.run.run_id, 'run failed')
  assert.equal(kept.run.state, 'failed', 'the kept run stays readable')
  const remainder = await storedKeys(PREFIX)
  assert.deepEqual(remainder.sort(), expectedRemainder(before, failed.run.day))
  for (const fence of remainder.filter((key) => key.startsWith('artifacts/'))) {
    const path = [PREFIX, ...fence.split('/')].map(encodeURIComponent).join('/')
    assert.equal(await s3(`/${path}`), 'lode:removed', fence)
  }
  signal.throwIfAborted()
  console.log('S3 lifecycle passed')
}

try {
  await main()
} finally {
  server.close()
  await rm(dir, { recursive: true, force: true })
}
