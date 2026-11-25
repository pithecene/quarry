// The lifecycle of runs on an S3 service: write, notification, SDK read, prune.
// Usage: s3-lifecycle.mts <quarry binary> <endpoint>, with AWS_* credentials.

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

const dir = await mkdtemp(join(tmpdir(), 'quarry-s3-'))
const received: string[] = []
const server = createServer(async (req, res) => {
  let body = ''
  for await (const chunk of req) body += chunk
  received.push(body)
  res.writeHead(204).end()
})

async function quarry(...args: string[]): Promise<{ code: number; out: string; err: string }> {
  const child = spawn(quarryPath, args, { cwd: dir, timeout: 120_000 })
  let out = ''
  let err = ''
  child.stdout.on('data', (data) => {
    out += data
  })
  child.stderr.on('data', (data) => {
    err += data
  })
  const [code] = await once(child, 'close')
  return { code, out, err }
}

const run = (script: string, id: string, policy: string[] = []) =>
  quarry(
    ...['run', '--script', join(dir, script), '--run-id', id, '--source', 'a b'],
    ...['--category', 'c/d', '--no-browser', ...flags, '--adapter', 'webhook', '--adapter-url'],
    `http://127.0.0.1:${(server.address() as AddressInfo).port}/`,
    ...policy
  )

async function prune(...args: string[]) {
  const result = await quarry('prune', '--format=json', ...flags, ...args)
  assert.equal(result.code, 0, result.err)
  return JSON.parse(result.out) as { runs_pruned: number; runs_kept: number; objects: number }
}

const locators: NotificationLocator[] = []
try {
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
    const read = await readRun(locator.run, { storage: locator.storage, quarryPath })
    assert.equal(read.run.state, 'succeeded')
    assert.equal(read.events.length, 7, 'five items, an artifact, and the terminal event')
    locators.push(locator)
  }
  assert.notEqual((await run('ok.mjs', 'run strict')).code, 0, 'a duplicate run ID is refused')
  assert.equal(received.length, locators.length, 'a refused run sends no notification')
  assert.equal((await run('bad.mjs', 'run failed')).code, 1)
  const failed = (JSON.parse(received.at(-1) ?? '') as { locator: NotificationLocator }).locator
  const runs = await listRuns({ storage, quarryPath })
  assert.equal(runs.runs.length, 3)

  const due = ['--succeeded-days=0', '--failed-days=0']
  const planned = await prune('--dry-run', ...due)
  const pruned = await prune(...due)
  assert.equal(planned.runs_pruned, pruned.runs_pruned)
  assert.equal(planned.objects, pruned.objects)
  assert.ok(pruned.runs_pruned >= 2, 'both successes are pruned')
  assert.equal((await prune(...due)).objects, 0, 'a second prune removes nothing')
  for (const locator of locators) {
    await assert.rejects(readRun(locator.run, { storage, quarryPath }))
  }
  if (pruned.runs_kept > 0) {
    const kept = await readRun(failed.run, { storage, quarryPath })
    assert.equal(kept.run.state, 'failed', 'a kept run stays readable')
  }
  console.log('S3 lifecycle passed', pruned)
} finally {
  server.close()
  await rm(dir, { recursive: true, force: true })
}
