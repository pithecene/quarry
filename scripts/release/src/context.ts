import type { Run } from './process.ts'

/** Progress messages, written to stderr so stdout stays free for data. */
export type Log = (message: string) => void

/** Waits for the given number of milliseconds. */
export type Sleep = (ms: number) => Promise<void>

/** The subset of `fetch` the release steps use. */
export type Fetch = (url: string, init: { signal: AbortSignal }) => Promise<{ status: number }>

/**
 * Everything a release step does to the outside world. The CLI wires the
 * real implementations; tests substitute fakes.
 */
export type Context = {
  readonly run: Run
  readonly fetch: Fetch
  readonly sleep: Sleep
  readonly log: Log
}
