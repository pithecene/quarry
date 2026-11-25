// The lifecycle of runs on an S3 service: write, notification, SDK read, prune.
// Usage: lifecycle.ts <quarry binary> <endpoint>, with AWS_* credentials.

import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { listRuns, type NotificationLocator, readRun } from '../../../sdk/src/index.ts'

const [binary = '', endpoint = ''] = process.argv.slice(2)
// The run writes and prunes, so it accepts a local service only.
assert.match(endpoint, /^http:\/\/127\.0\.0\.1:\d+$/, 'expected a loopback endpoint')
const BUDGET_MS = 300_000
const KILL_GRACE_MS = 30_000
const signal = AbortSignal.timeout(BUDGET_MS)
const quarryPath = resolve(binary)
const storage = {
  backend: 's3',
  path: 'smoke/pre fix',
  region: 'us-east-1',
  endpoint,
  s3PathStyle: true
} as const
const flags = [
  '--storage-backend=s3',
  `--storage-path=${storage.path}`,
  '--storage-region=us-east-1',
  `--storage-endpoint=${endpoint}`,
  '--storage-s3-path-style'
]

let dir = ''
const received: string[] = []
const server = createServer(async (req, res) => {
  let body = ''
  for await (const chunk of req) body += chunk
  received.push(body)
  res.writeHead(204).end()
})

async function quarry(...args: string[]): Promise<{ code: number; out: string; err: string }> {
  const child = spawn(quarryPath, args, { cwd: dir, signal })
  // An abort reports an error first. The close follows it, and ends the wait.
  // An aborted CLI gets SIGTERM and time to stop its executor, then SIGKILL.
  let failure: Error | undefined
  let kill: NodeJS.Timeout | undefined
  child.on('error', (error) => {
    failure = error
    kill = setTimeout(() => child.kill('SIGKILL'), KILL_GRACE_MS)
  })
  let out = ''
  let err = ''
  child.stdout.on('data', (data) => {
    out += data
  })
  child.stderr.on('data', (data) => {
    err += data
  })
  const code = await new Promise<number>((done) => child.on('close', done))
  clearTimeout(kill)
  if (failure) throw failure
  return { code, out, err }
}

const run = (script: string, id: string, policy: string[] = []) =>
  quarry(
    ...['run', '--script', join(dir, script), '--run-id', id, '--source', 'a b'],
    ...['--category', 'c/d', '--no-browser', ...flags, '--adapter', 'webhook', '--adapter-url'],
    `http://127.0.0.1:${(server.address() as AddressInfo).port}/`,
    ...policy
  )

async function prune(...args: string[]): Promise<Record<string, number>> {
  const result = await quarry('prune', '--format=json', ...flags, ...args)
  assert.equal(result.code, 0, result.err)
  return JSON.parse(result.out) as Record<string, number>
}

const locators: NotificationLocator[] = []

async function main(): Promise<void> {
  dir = await mkdtemp(join(tmpdir(), 'quarry-s3-'))
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  await writeFile(
    join(dir, 'ok.mjs'),
    `export default async function run(ctx) {
      for (let n = 0; n < 5; n++) await ctx.emit.item({ item_type: 'x', data: { n } })
      await ctx.storage.put({ filename: 'a.txt', content_type: 'text/plain', data: Buffer.from('a') })
      await ctx.emit.artifact({ name: 'a.bin', content_type: 'application/octet-stream', data: Buffer.alloc(9 << 20, 7) })
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
  console.log('S3 lifecycle passed')
}

try {
  await main()
} finally {
  server.close()
  if (dir !== '') await rm(dir, { recursive: true, force: true })
}
