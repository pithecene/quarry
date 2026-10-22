/**
 * Run reader: lists runs and reads their events by invoking the `quarry`
 * CLI and parsing its JSON output (CONTRACT_CLI.md §list runs, §list events).
 * The CLI is the one implementation of the storage layout, so this module
 * never reads storage itself.
 */
import { spawn } from 'node:child_process'

/**
 * A run's state as storage proves it. `unterminated` runs have no end record
 * (live, killed, or unrecorded); `unclassified` runs ended but their outcome
 * is unknown (backfilled records).
 */
export type RunState = 'succeeded' | 'failed' | 'unterminated' | 'unclassified'

/** One run, as `quarry list runs --format json` reports it. */
export type RunSummary = {
  run_id: string
  job_id?: string
  parent_run_id?: string
  attempt: number
  dataset: string
  source: string
  category: string
  day: string
  partition: string
  /** Null for a run whose records were backfilled. */
  started_at: string | null
  state: RunState
  backfilled: boolean
  ended_at: string | null
  outcome: string | null
  terminal: 'run_complete' | 'run_error' | null
  message?: string
  event_count: number | null
}

/** One page of runs. `next` is absent on the last page. */
export type RunPage = {
  runs: RunSummary[]
  next?: string
}

/** A stored event or artifact commit record, in sequence order. */
export type StoredEvent = Record<string, unknown> & {
  record_kind: 'event' | 'artifact_event'
  seq: number
  ts: string
}

/** A run and its events. */
export type RunEvents = {
  run: RunSummary
  events: StoredEvent[]
}

/** Storage to read, with the same meaning as the CLI storage flags. */
export type ReaderStorage = {
  backend: 'fs' | 's3'
  /** fs: directory; s3: bucket/prefix. */
  path: string
  /** Lode dataset ID; the CLI default is "quarry". */
  dataset?: string
  region?: string
  endpoint?: string
  s3PathStyle?: boolean
}

/** Options shared by every read. */
export type ReaderOptions = {
  storage: ReaderStorage
  /** The `quarry` executable; defaults to `quarry` on PATH. */
  quarryPath?: string
  /** Aborts the read and terminates the CLI process. */
  signal?: AbortSignal
}

/** Selectors and paging for {@link listRuns}. */
export type ListRunsOptions = ReaderOptions & {
  /** Leading selectors: `category` requires `source`, `day` requires `category`. */
  source?: string
  category?: string
  day?: string
  state?: RunState
  /** Maximum runs per page; omitted or 0 means no limit. */
  limit?: number
  /** The `next` value of a previous page. */
  after?: string
}

/** Locates one run's records. */
export type RunLocator = {
  source: string
  category: string
  day: string
  runId: string
}

/** Thrown when the CLI cannot be run or exits with an error. */
export class QuarryReadError extends Error {
  readonly exitCode: number | null
  readonly stderr: string

  constructor(message: string, exitCode: number | null, stderr: string) {
    super(message)
    this.name = 'QuarryReadError'
    this.exitCode = exitCode
    this.stderr = stderr
  }
}

/**
 * Lists one page of runs from their run records, ordered by source,
 * category, day, and run ID.
 */
export async function listRuns(options: ListRunsOptions): Promise<RunPage> {
  const args = [
    'list',
    'runs',
    ...storageArgs(options.storage),
    ...optionalArgs({
      source: options.source,
      category: options.category,
      day: options.day,
      state: options.state,
      limit: options.limit,
      after: options.after
    })
  ]
  return runQuarry(args, options, isRunPage)
}

/** Reads one run and its events in sequence order. */
export async function readRun(run: RunLocator, options: ReaderOptions): Promise<RunEvents> {
  const args = [
    'list',
    'events',
    ...storageArgs(options.storage),
    ...optionalArgs({
      source: run.source,
      category: run.category,
      day: run.day,
      'run-id': run.runId
    })
  ]
  return runQuarry(args, options, isRunEvents)
}

function storageArgs(storage: ReaderStorage): string[] {
  return [
    ...optionalArgs({
      'storage-backend': storage.backend,
      'storage-path': storage.path,
      'storage-dataset': storage.dataset,
      'storage-region': storage.region,
      'storage-endpoint': storage.endpoint
    }),
    ...(storage.s3PathStyle ? ['--storage-s3-path-style'] : [])
  ]
}

// `--flag=value` keeps a value that starts with "-" from being read as a flag.
// An empty value is rejected: the CLI would read it as the flag being absent
// (a default dataset, an unselected source) and read something else.
function optionalArgs(flags: Record<string, string | number | undefined>): string[] {
  return Object.entries(flags).flatMap(([name, value]) => {
    if (value === '') {
      throw new QuarryReadError(`--${name} must not be empty`, null, '')
    }
    return value === undefined ? [] : [`--${name}=${value}`]
  })
}

async function runQuarry<T>(
  args: string[],
  options: ReaderOptions,
  isShape: (v: unknown) => v is T
): Promise<T> {
  const command = options.quarryPath ?? 'quarry'
  const label = `quarry ${args.slice(0, 2).join(' ')}`
  const { code, stdout, stderr } = await capture(
    command,
    [...args, '--format=json'],
    options.signal
  )
  if (code !== 0) {
    const detail = stderr.trim() || `exit code ${code}`
    throw new QuarryReadError(`${label} failed: ${detail}`, code, stderr)
  }
  const parsed = parseJSON(stdout)
  if (!isShape(parsed)) {
    throw new QuarryReadError(
      `${label} returned output that is not its JSON response`,
      code,
      stderr
    )
  }
  return parsed
}

function parseJSON(text: string): unknown {
  try {
    return JSON.parse(text)
  } catch {
    return undefined
  }
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

type FieldCheck = (v: unknown) => boolean

const isString: FieldCheck = (v) => typeof v === 'string'
const isNumber: FieldCheck = (v) => typeof v === 'number'
const oneOf =
  (...values: readonly unknown[]): FieldCheck =>
  (v) =>
    values.includes(v)
const orNull =
  (check: FieldCheck): FieldCheck =>
  (v) =>
    v === null || check(v)
const optional =
  (check: FieldCheck): FieldCheck =>
  (v) =>
    v === undefined || check(v)

const RUN_STATES: readonly RunState[] = ['succeeded', 'failed', 'unterminated', 'unclassified']

// Field rules for RunSummary (CONTRACT_CLI.md §list runs).
const runSummaryFields: Record<string, FieldCheck> = {
  run_id: isString,
  job_id: optional(isString),
  parent_run_id: optional(isString),
  attempt: isNumber,
  dataset: isString,
  source: isString,
  category: isString,
  day: isString,
  partition: isString,
  started_at: orNull(isString),
  state: oneOf(...RUN_STATES),
  backfilled: (v) => typeof v === 'boolean',
  ended_at: orNull(isString),
  outcome: orNull(isString),
  terminal: oneOf('run_complete', 'run_error', null),
  message: optional(isString),
  event_count: orNull(isNumber)
}

// Field rules every stored event record satisfies.
const storedEventFields: Record<string, FieldCheck> = {
  record_kind: oneOf('event', 'artifact_event'),
  seq: isNumber,
  ts: isString
}

function hasFields(v: unknown, fields: Record<string, FieldCheck>): v is Record<string, unknown> {
  return isObject(v) && Object.entries(fields).every(([name, check]) => check(v[name]))
}

function isRunSummary(v: unknown): v is RunSummary {
  return hasFields(v, runSummaryFields)
}

function isStoredEvent(v: unknown): v is StoredEvent {
  return hasFields(v, storedEventFields)
}

function isRunPage(v: unknown): v is RunPage {
  return (
    isObject(v) &&
    Array.isArray(v.runs) &&
    v.runs.every(isRunSummary) &&
    (v.next === undefined || typeof v.next === 'string')
  )
}

function isRunEvents(v: unknown): v is RunEvents {
  return (
    isObject(v) && isRunSummary(v.run) && Array.isArray(v.events) && v.events.every(isStoredEvent)
  )
}

type Captured = { code: number | null; stdout: string; stderr: string }

function capture(
  command: string,
  args: string[],
  signal: AbortSignal | undefined
): Promise<Captured> {
  return new Promise((resolve, reject) => {
    const fail = (e: Error, stderr: string): void =>
      reject(new QuarryReadError(`could not run ${command}: ${e.message}`, null, stderr))
    let child: ReturnType<typeof spawn>
    try {
      // spawn throws synchronously for an invalid command string.
      child = spawn(command, args, { signal, stdio: ['ignore', 'pipe', 'pipe'] })
    } catch (e) {
      fail(e instanceof Error ? e : new Error(String(e)), '')
      return
    }
    const out: Buffer[] = []
    const err: Buffer[] = []
    child.stdout?.on('data', (chunk: Buffer) => out.push(chunk))
    child.stderr?.on('data', (chunk: Buffer) => err.push(chunk))
    child.on('error', (e) => fail(e, Buffer.concat(err).toString()))
    child.on('close', (code) =>
      resolve({
        code,
        stdout: Buffer.concat(out).toString(),
        stderr: Buffer.concat(err).toString()
      })
    )
  })
}
