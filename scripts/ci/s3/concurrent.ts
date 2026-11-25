// Runs that commit to one dataset at once, on an S3 service.
// Usage: concurrent.ts <quarry binary> <endpoint>, with AWS_* credentials.

import assert from 'node:assert/strict'
import { rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { listRuns, readRun } from '../../../sdk/src/index.ts'
import { dir, flagsAt, quarry, quarryPath, s3, signal, storageAt, storedKeys } from './service.ts'

const ROUNDS = 5
const RUNS = 4
const ITEMS = 5
/** Contention can exhaust the retries of some runs, never of most of them. */
const LEAST_SUCCESSES = (ROUNDS * RUNS) / 2
const EXIT_STORAGE = 4

/** Runs one round, and returns how many of its runs succeeded. */
async function round(prefix: string): Promise<number> {
  const storage = storageAt(prefix)
  const ids = Array.from({ length: RUNS }, (_, n) => `run-${n}`)
  const results = await Promise.all(
    ids.map(async (id) => ({
      id,
      ...(await quarry(
        ...['run', '--script', join(dir, 'run.mjs'), '--run-id', id, '--source', 's'],
        ...['--category', 'c', '--no-browser', ...flagsAt(prefix)]
      ))
    }))
  )
  const { runs } = await listRuns({ storage, quarryPath, signal })
  assert.deepEqual(runs.map((run) => run.run_id).sort(), ids, 'every run is listed')
  for (const { id, code, err } of results) {
    const run = runs.find((listed) => listed.run_id === id)
    assert.ok(run, id)
    // A lost swap is a conflict, whose retries can run out, and never uncertain.
    assert.doesNotMatch(err, /failed to reconcile latest pointer/, id)
    assert.ok(code === 0 || code === EXIT_STORAGE, `${id} exited ${code}: ${err}`)
    if (code !== 0) assert.match(err, /snapshot conflict/, id)
    assert.equal(run.state, code === 0 ? 'succeeded' : 'failed', id)
    const key = { source: 's', category: 'c', day: run.day, runId: id }
    const { events } = await readRun(key, { storage, quarryPath, signal })
    // What a run stored is a prefix of what it emitted, and a success is whole.
    const items = events.filter((event) => event.type === 'item')
    assert.deepEqual(
      items.map((event) => (event.payload as { data: { n: number } }).data.n),
      items.map((_, n) => n),
      `${id} stored its items in order`
    )
    if (code === 0) {
      assert.equal(items.length, ITEMS, id)
      assert.equal(events.at(-1)?.type, 'run_complete', id)
    }
  }
  await assertOneChain(prefix)
  return results.filter((result) => result.code === 0).length
}

/** Asserts that the committed snapshots form one chain that ends at the head. */
async function assertOneChain(prefix: string): Promise<void> {
  const manifests = (await storedKeys(prefix)).filter((key) =>
    /^datasets\/quarry\/segments\/[^/]+\/manifest\.json$/.test(key)
  )
  const parents = new Map<string, string>()
  for (const key of manifests) {
    const manifest = JSON.parse(await s3(`/${prefix}/${key}`)) as Record<string, string>
    parents.set(String(manifest.snapshot_id), manifest.parent_snapshot_id ?? '')
  }
  assert.equal(parents.size, manifests.length, 'each snapshot has one manifest')
  let id = await s3(`/${prefix}/datasets/quarry/latest`)
  const chain = new Set<string>()
  while (id !== '') {
    assert.ok(parents.has(id) && !chain.has(id), `the chain breaks at ${id}`)
    chain.add(id)
    id = parents.get(id) ?? ''
  }
  assert.equal(chain.size, parents.size, 'a snapshot is outside the chain')
}

try {
  await writeFile(
    join(dir, 'run.mjs'),
    `export default async function run(ctx) {
      for (let n = 0; n < ${ITEMS}; n++) await ctx.emit.item({ item_type: 'x', data: { n } })
      await ctx.emit.runComplete({})
    }`
  )
  let succeeded = 0
  for (let n = 0; n < ROUNDS; n++) succeeded += await round(`concurrent-${n}`)
  const all = ROUNDS * RUNS
  assert.ok(succeeded >= LEAST_SUCCESSES, `${succeeded} of ${all} runs succeeded`)
  signal.throwIfAborted()
  console.log(
    `Concurrent runs passed: ${succeeded} succeeded, ${all - succeeded} ran out of retries`
  )
} finally {
  await rm(dir, { recursive: true, force: true })
}
