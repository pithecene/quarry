/**
 * Polling pattern: find runs that succeeded since the last poll and process them.
 *
 * Usage: npx tsx polling-runs.ts <storage-path> [checkpoint-file]
 *
 * Setup: copy this file into a Node project that installs
 * `@pithecene/quarry-sdk`. It does not run in place in this repository,
 * whose examples resolve the SDK for types only. The SDK reader (`listRuns`,
 * `readRun`) runs the `quarry` CLI, so `quarry` must be on PATH.
 *
 * This example reads filesystem storage. To poll S3, change the storage it
 * passes to an `s3` ReaderStorage, with `path` as `bucket/prefix`.
 *
 * Runs that Quarry wrote before 0.14 have no run records, so `listRuns` cannot
 * find them until `quarry index rebuild` backfills their records.
 *
 * Not handled here, by design:
 * - Concurrent pollers. Run one poller per checkpoint file.
 * - A crash before a poll saves its checkpoint. The next poll processes the
 *   runs that poll handled again, so processing must be idempotent.
 * - Large stores. Each poll lists every succeeded run, because `listRuns`
 *   orders runs by source, category, day, and run ID rather than by time, so
 *   a page cursor cannot mark "everything after the last poll". Narrow the
 *   scan with the `source`, `category`, and `day` selectors.
 */
import { randomUUID } from 'node:crypto'
import { readFile, rename, rm, writeFile } from 'node:fs/promises'
import { dirname } from 'node:path'

import { listRuns, type ReaderStorage, type RunSummary, readRun } from '@pithecene/quarry-sdk'

async function loadProcessed(path: string): Promise<Set<string>> {
  let text: string
  try {
    text = await readFile(path, 'utf8')
  } catch (error) {
    // A missing checkpoint means a first poll. Any other error stops the poller.
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return new Set()
    }
    throw error
  }
  const ids: unknown = JSON.parse(text)
  if (!Array.isArray(ids) || !ids.every((id) => typeof id === 'string')) {
    throw new Error(`checkpoint ${path} is not a JSON array of run IDs`)
  }
  return new Set(ids)
}

// saveProcessed replaces the checkpoint only after the new one is written
// whole, so a stopped poller leaves the last good checkpoint in place.
async function saveProcessed(path: string, processed: Set<string>): Promise<void> {
  // A fixed-length random name beside the checkpoint, created exclusively with
  // mode 0o600: saving never truncates another file, and the replacement gives
  // group and other users no permission bits.
  // dirname keeps the parent as written, so the filesystem resolves it as it
  // resolves the checkpoint path; join would collapse ".." before symlinks.
  const tmp = `${dirname(path)}/.${randomUUID()}.tmp`
  await writeFile(tmp, JSON.stringify([...processed]), { flag: 'wx', mode: 0o600 })
  try {
    await rename(tmp, path)
  } catch (err) {
    try {
      await rm(tmp, { force: true })
    } catch (cleanup) {
      throw new AggregateError([err, cleanup], `replacing ${path} failed, and ${tmp} remains`)
    }
    throw err
  }
}

async function processRun(run: RunSummary, storage: ReaderStorage): Promise<void> {
  const { events } = await readRun(
    { source: run.source, category: run.category, day: run.day, runId: run.run_id },
    { storage }
  )
  const items = events.filter((event) => event.type === 'item')
  // Replace with your downstream processing.
  console.log(`[poller] ${run.run_id}: ${items.length} items`)
}

async function poll(storage: ReaderStorage, checkpointPath: string): Promise<void> {
  const processed = await loadProcessed(checkpointPath)
  const before = processed.size
  let after: string | undefined
  do {
    const page = await listRuns({ storage, state: 'succeeded', limit: 100, after })
    for (const run of page.runs) {
      if (processed.has(run.run_id)) {
        continue
      }
      await processRun(run, storage)
      processed.add(run.run_id)
    }
    after = page.next
  } while (after !== undefined)
  if (processed.size > before) {
    await saveProcessed(checkpointPath, processed)
  }
}

const [storagePath, checkpointPath = './.quarry-poll-checkpoint.json'] = process.argv.slice(2)
if (storagePath === undefined) {
  console.error('usage: polling-runs.ts <storage-path> [checkpoint-file]')
  process.exit(2)
}
poll({ backend: 'fs', path: storagePath }, checkpointPath).catch((error: unknown) => {
  console.error('[poller] poll failed:', error)
  process.exitCode = 1
})
