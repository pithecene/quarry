/**
 * Unit tests for the run reader. A fake `quarry` executable records its
 * arguments and replies with canned output, so these tests pin the CLI
 * invocation (CONTRACT_CLI.md §list runs, §list events) and the error
 * handling without a Go build.
 */
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { listRuns, QuarryReadError, readRun } from '../../src/reader'

type Fake = { path: string; argsFile: string }

const dirs: string[] = []

afterEach(async () => {
  await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })))
})

async function fakeQuarry(stdout: string, exitCode = 0, stderr = ''): Promise<Fake> {
  const dir = await mkdtemp(join(tmpdir(), 'quarry-reader-'))
  dirs.push(dir)
  const argsFile = join(dir, 'args.json')
  const path = join(dir, 'quarry')
  const script = [
    '#!/usr/bin/env node',
    `require('node:fs').writeFileSync(${JSON.stringify(argsFile)}, JSON.stringify(process.argv.slice(2)))`,
    `require('node:fs').writeSync(1, ${JSON.stringify(stdout)})`,
    `require('node:fs').writeSync(2, ${JSON.stringify(stderr)})`,
    `process.exitCode = ${exitCode}`
  ].join('\n')
  await writeFile(path, script)
  await chmod(path, 0o755)
  return { path, argsFile }
}

async function argsOf(fake: Fake): Promise<string[]> {
  return JSON.parse(await readFile(fake.argsFile, 'utf8')) as string[]
}

const storage = { backend: 'fs', path: '/data/quarry' } as const

const run = {
  run_id: 'r1',
  attempt: 1,
  dataset: 'quarry',
  source: 'demo',
  category: 'default',
  day: '2026-10-22',
  partition: 'source=demo/category=default/day=2026-10-22/run_id=r1/',
  started_at: null,
  state: 'unclassified',
  backfilled: true,
  ended_at: '2026-10-22T04:18:53.858Z',
  outcome: null,
  terminal: 'run_complete',
  event_count: null
}

describe('listRuns', () => {
  it('passes storage, selectors, and paging as CLI flags and parses the page', async () => {
    const fake = await fakeQuarry(JSON.stringify({ runs: [run], next: 'cursor-1' }))
    const page = await listRuns({
      quarryPath: fake.path,
      storage: { ...storage, dataset: 'other', endpoint: 'https://r2.example', s3PathStyle: true },
      source: 'demo',
      category: 'default',
      state: 'unclassified',
      limit: 10,
      after: 'cursor-0'
    })
    expect(page.next).toBe('cursor-1')
    expect(page.runs[0]?.started_at).toBeNull()
    expect(await argsOf(fake)).toEqual([
      'list',
      'runs',
      '--storage-backend=fs',
      '--storage-path=/data/quarry',
      '--storage-dataset=other',
      '--storage-endpoint=https://r2.example',
      '--storage-s3-path-style',
      '--source=demo',
      '--category=default',
      '--state=unclassified',
      '--limit=10',
      '--after=cursor-0',
      '--format=json'
    ])
  })

  it('omits unset options', async () => {
    const fake = await fakeQuarry(JSON.stringify({ runs: [] }))
    const page = await listRuns({ quarryPath: fake.path, storage })
    expect(page).toEqual({ runs: [] })
    expect(await argsOf(fake)).toEqual([
      'list',
      'runs',
      '--storage-backend=fs',
      '--storage-path=/data/quarry',
      '--format=json'
    ])
  })

  it('throws QuarryReadError with the CLI stderr on a non-zero exit', async () => {
    const fake = await fakeQuarry('', 1, 'list runs: invalid cursor\n')
    const read = listRuns({ quarryPath: fake.path, storage, after: 'bogus' })
    await expect(read).rejects.toBeInstanceOf(QuarryReadError)
    await expect(read).rejects.toMatchObject({
      exitCode: 1,
      message: expect.stringContaining('invalid cursor')
    })
  })

  it('throws QuarryReadError on output that is not JSON', async () => {
    const fake = await fakeQuarry('not json')
    await expect(listRuns({ quarryPath: fake.path, storage })).rejects.toThrow(/invalid JSON/)
  })

  it('throws QuarryReadError when the executable is missing', async () => {
    const read = listRuns({ quarryPath: join(tmpdir(), 'no-such-quarry-binary'), storage })
    await expect(read).rejects.toMatchObject({ name: 'QuarryReadError', exitCode: null })
  })
})

describe('readRun', () => {
  it('locates the run with list events and returns the run and its events', async () => {
    const events = [{ record_kind: 'event', seq: 1, ts: '2026-10-22T04:18:53Z', type: 'item' }]
    const fake = await fakeQuarry(JSON.stringify({ run, events }))
    const got = await readRun(
      { source: 'demo', category: 'default', day: '2026-10-22', runId: '-r1' },
      { quarryPath: fake.path, storage }
    )
    expect(got.events).toEqual(events)
    expect(got.run.run_id).toBe('r1')
    expect(await argsOf(fake)).toEqual([
      'list',
      'events',
      '--storage-backend=fs',
      '--storage-path=/data/quarry',
      '--source=demo',
      '--category=default',
      '--day=2026-10-22',
      '--run-id=-r1',
      '--format=json'
    ])
  })
})
