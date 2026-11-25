// Runs that commit to one dataset at once, on an S3 service.
// Usage: concurrent.ts <quarry binary> <endpoint>, with AWS_* credentials.

import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { promisify } from 'node:util'
import { listRuns, readRun } from '../../../sdk/src/index.ts'
import { dir, endpoint, flagsAt, quarry, quarryPath, signal, storageAt } from './service.ts'

const ROUNDS = 5
const RUNS = 4
const ITEMS = 5
/** Contention can exhaust the retries of some runs, never of most of them. */
const LEAST_SUCCESSES = (ROUNDS * RUNS) / 2
const EXIT_STORAGE = 4
/** The conflict retries of one write (CONTRACT_LODE.md, Write Retry). */
const RETRIES = 3
/** A run ID is unique across invocations (CONTRACT_RUN.md). */
const INVOCATION = `smoke-${Date.now()}-${process.pid}`
/** The conflict retries of every run, which show that their commits met. */
let contended = 0

/** Runs one round, and returns how many of its runs succeeded. */
async function round(prefix: string): Promise<number> {
  const storage = storageAt(prefix)
  const ids = Array.from({ length: RUNS }, (_, n) => `${INVOCATION}-${prefix}-${n}`)
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
  for (const { id, code, out, err } of results) {
    const run = runs.find((listed) => listed.run_id === id)
    assert.ok(run, id)
    // A lost swap is a conflict, whose retries can run out, and never uncertain.
    assert.doesNotMatch(err, /failed to reconcile latest pointer/, id)
    assert.ok(code === 0 || code === EXIT_STORAGE, `${id} exited ${code}: ${err}`)
    if (code !== 0) assert.match(err, /snapshot conflict/, id)
    assert.equal(run.state, code === 0 ? 'succeeded' : 'failed', id)
    const key = { source: 's', category: 'c', day: run.day, runId: id }
    const { events } = await readRun(key, { storage, quarryPath, signal })
    // A success stored all that its run emitted, and a refusal a prefix of it.
    const stored = events.map(({ seq, type, run_id, payload }) => ({ seq, type, run_id, payload }))
    const emitted = [
      ...Array.from({ length: ITEMS }, (_, n) => ({
        seq: n + 1,
        type: 'item',
        run_id: id,
        payload: { item_type: 'x', data: { value: `${id}/${n}` } }
      })),
      { seq: ITEMS + 1, type: 'run_complete', run_id: id, payload: { summary: { items: ITEMS } } }
    ]
    assert.deepEqual(stored, code === 0 ? emitted : emitted.slice(0, stored.length), id)
    const retries = Number(/lode_write_retry_total:\s+(\d+)/.exec(out + err)?.[1])
    assert.ok(Number.isInteger(retries), `${id} reports its retries`)
    if (code !== 0) assert.ok(retries >= RETRIES, `${id} was refused after ${retries} retries`)
    contended += retries
  }
  await assertOneChain(prefix)
  return results.filter((result) => result.code === 0).length
}

/** Asserts, through Lode, that the committed snapshots form one chain from the head. */
async function assertOneChain(prefix: string): Promise<void> {
  const test = ['test', '-count=1', './lode/', '-run', '^TestS3SnapshotsFormOneChain$']
  const at = [`-s3-endpoint=${endpoint}`, `-s3-path=smoke/${prefix}`]
  await promisify(execFile)('go', [...test, ...at], { cwd: 'quarry', signal })
}

try {
  await writeFile(
    join(dir, 'run.mjs'),
    `export default async function run(ctx) {
      for (let n = 0; n < ${ITEMS}; n++) await ctx.emit.item({ item_type: 'x', data: { value: ctx.run.run_id + '/' + n } })
      await ctx.emit.runComplete({ summary: { items: ${ITEMS} } })
    }`
  )
  let succeeded = 0
  for (let n = 0; n < ROUNDS; n++) succeeded += await round(`concurrent-${n}`)
  const all = ROUNDS * RUNS
  assert.ok(contended > 0, 'no two commits met')
  assert.ok(succeeded >= LEAST_SUCCESSES, `${succeeded} of ${all} runs succeeded`)
  signal.throwIfAborted()
  console.log(
    `Concurrent runs passed: ${succeeded} succeeded, ${all - succeeded} refused, ${contended} retries`
  )
} finally {
  await rm(dir, { recursive: true, force: true })
}
