import type { Browser, BrowserContext, Page } from 'puppeteer'
import type { EmitSink, StoragePartitionMeta } from './emit'
import { createAPIs } from './emit-impl'
import type { MemoryThresholds } from './memory'
import { createMemoryAPI } from './memory'
import type { QuarryContext, RunMeta } from './types/context'

/**
 * Options for creating a QuarryContext.
 * Used by executor-node to construct the context.
 *
 * @internal
 */
export type CreateContextOptions<Job = unknown> = {
  job: Job
  run: RunMeta
  /** Omitted in a run with `noBrowser`. */
  page?: Page
  /** Omitted in a run with `noBrowser`. */
  browser?: Browser
  /** Omitted in a run with `noBrowser`. */
  browserContext?: BrowserContext
  /**
   * The run declares no browser (CONTRACT_RUN.md §Runs Without a Browser).
   * The context's page, browser and browser context then throw
   * NoBrowserError on use.
   */
  noBrowser?: boolean
  sink: EmitSink
  /** Storage partition metadata for SDK-side key computation. @internal */
  storagePartition?: StoragePartitionMeta
  /** Custom memory pressure thresholds. @internal */
  memoryThresholds?: MemoryThresholds
}

/**
 * Thrown when a script uses `ctx.page`, `ctx.browser` or
 * `ctx.browserContext` in a run that declares no browser
 * (`quarry run --no-browser`).
 */
export class NoBrowserError extends Error {
  /** The context member the script used. */
  readonly member: string

  constructor(member: string) {
    super(`ctx.${member} is unavailable: this run declares no browser (quarry run --no-browser)`)
    this.name = 'NoBrowserError'
    this.member = member
  }
}

/**
 * A stand-in for a browser handle in a run without a browser. Reading any
 * named member throws NoBrowserError. Symbol members read as undefined, so
 * inspection and promise checks of the handle itself do not throw.
 */
function noBrowserHandle<T>(member: string): T {
  const fail = (): never => {
    throw new NoBrowserError(member)
  }
  return new Proxy(Object.create(null) as object, {
    get: (_target, key) => (typeof key === 'symbol' || key === 'then' ? undefined : fail()),
    has: fail,
    set: fail,
    ownKeys: fail,
    apply: fail
  }) as T
}

function requireHandle<T>(value: T | undefined, member: string): T {
  if (value === undefined) {
    throw new Error(`createContext needs ${member} unless noBrowser is set`)
  }
  return value
}

/**
 * Create a QuarryContext instance.
 * This is called by the executor-node, not by user scripts.
 *
 * @internal
 */
export function createContext<Job = unknown>(
  options: CreateContextOptions<Job>
): QuarryContext<Job> {
  const { emit, storage } = createAPIs(options.run, options.sink, options.storagePartition)
  const noBrowser = options.noBrowser === true
  const page = noBrowser ? noBrowserHandle<Page>('page') : requireHandle(options.page, 'page')
  const browser = noBrowser
    ? noBrowserHandle<Browser>('browser')
    : requireHandle(options.browser, 'browser')
  const browserContext = noBrowser
    ? noBrowserHandle<BrowserContext>('browserContext')
    : requireHandle(options.browserContext, 'browserContext')
  const memory = createMemoryAPI({
    page: noBrowser ? null : page,
    thresholds: options.memoryThresholds
  })

  const ctx: QuarryContext<Job> = {
    job: options.job,
    run: Object.freeze(options.run),
    page,
    browser,
    browserContext,
    emit,
    storage,
    memory
  }

  return Object.freeze(ctx)
}
