import { readFileSync, renameSync, writeFileSync } from 'node:fs'

/** Connects to a browser by endpoint; `puppeteer.connect` in production. */
export type ConnectBrowser = (options: {
  browserWSEndpoint: string
}) => Promise<{ close(): Promise<void> }>

/**
 * Whether the browser an endpoint names still runs: `gone` only when its port
 * refuses connections or answers as anything but that browser, `unknown`
 * when it cannot be told.
 */
export type ProbeBrowser = (endpoint: string) => Promise<'running' | 'gone' | 'unknown'>

/** How long one step of closing a recorded browser may take. */
const STEP_TIMEOUT_MS = 5_000

/** How often to probe a closing browser. */
const SETTLE_POLL_MS = 100

/**
 * Close the browsers that `recordPath` lists, and return those that may
 * still run.
 *
 * A server records its browser's endpoint as it launches the browser. One
 * that ends without closing its browser, as when killed with SIGKILL, leaves
 * the browser running and listed, so the next server closes it. A server
 * starts only while the runtime holds the discovery lock and no server
 * listens, so a listed browser belongs to no server that a run can reach.
 * The endpoint carries the browser's own ID, so it never names another
 * browser. A browser stays listed until a probe shows it gone, so that a
 * later server tries again.
 */
export async function closeRecordedBrowsers(
  recordPath: string,
  connect: ConnectBrowser,
  probe: ProbeBrowser,
  log: (message: string) => void,
  settleMs: number = STEP_TIMEOUT_MS
): Promise<string[]> {
  const outcomes = await Promise.all(
    readRecord(recordPath, log).map(async (endpoint) => {
      const attempted = await attemptClose(endpoint, connect)
      // Only the probe shows the browser gone: Puppeteer's close() resolves
      // even when the browser refused to close.
      const gone = await becomesGone(endpoint, probe, settleMs)
      if (!gone) {
        log(`Could not close a browser an earlier server left (${endpoint}); keeping it recorded`)
      } else if (attempted) {
        log(`Closed a browser an earlier server left running (${endpoint})`)
      }
      return { endpoint, keep: !gone }
    })
  )
  return outcomes.filter((o) => o.keep).map((o) => o.endpoint)
}

/** Replace the record with `endpoints`, atomically. */
export function recordBrowsers(recordPath: string, endpoints: readonly string[]): void {
  const tmp = `${recordPath}.tmp`
  writeFileSync(tmp, `${JSON.stringify({ browsers: endpoints })}\n`, { mode: 0o600 })
  renameSync(tmp, recordPath)
}

/**
 * Probe the browser at `endpoint` through its `/json/version`, which names
 * the endpoint of the browser on that port.
 */
export const probeBrowser: ProbeBrowser = async (endpoint) => {
  const url = new URL(endpoint)
  const scheme = url.protocol === 'wss:' ? 'https:' : 'http:'
  let res: Response
  try {
    res = await fetch(`${scheme}//${url.host}/json/version`, {
      signal: AbortSignal.timeout(STEP_TIMEOUT_MS)
    })
  } catch (err) {
    return refused(err) ? 'gone' : 'unknown'
  }
  // Whatever answers, only this browser names this endpoint.
  const body = (await res.json().catch(() => null)) as { webSocketDebuggerUrl?: unknown } | null
  return body?.webSocketDebuggerUrl === endpoint ? 'running' : 'gone'
}

/** Ask the browser to close; true when the request was delivered. */
async function attemptClose(endpoint: string, connect: ConnectBrowser): Promise<boolean> {
  try {
    const browser = await withTimeout(connect({ browserWSEndpoint: endpoint }), STEP_TIMEOUT_MS)
    await withTimeout(browser.close(), STEP_TIMEOUT_MS)
    return true
  } catch {
    // The probe decides whether it is gone
    return false
  }
}

/** Probe until the browser is gone, for up to `settleMs`, as it may take a moment to exit. */
async function becomesGone(
  endpoint: string,
  probe: ProbeBrowser,
  settleMs: number
): Promise<boolean> {
  const deadline = Date.now() + settleMs
  for (;;) {
    if ((await probe(endpoint)) === 'gone') return true
    if (Date.now() >= deadline) return false
    await new Promise((resolve) => setTimeout(resolve, SETTLE_POLL_MS))
  }
}

/**
 * Read the recorded endpoints. A missing record lists none. A record that
 * cannot be read throws, so the server does not start and overwrite it: the
 * browsers it lists stay findable. A record that reads but holds no usable
 * endpoint can name no browser to close, so it is dropped and logged.
 */
function readRecord(recordPath: string, log: (message: string) => void): string[] {
  let text: string
  try {
    text = readFileSync(recordPath, 'utf8')
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return []
    throw new Error(`cannot read ${recordPath}: ${String(err)}`)
  }
  const entries = parseEntries(text)
  if (entries === undefined) {
    log(`Ignoring malformed ${recordPath}`)
    return []
  }
  const endpoints = entries.filter(isEndpoint)
  if (endpoints.length < entries.length) log(`Ignoring malformed entries in ${recordPath}`)
  return endpoints
}

function parseEntries(text: string): unknown[] | undefined {
  try {
    const browsers = (JSON.parse(text) as { browsers?: unknown } | null)?.browsers
    return Array.isArray(browsers) ? browsers : undefined
  } catch {
    return undefined
  }
}

function isEndpoint(entry: unknown): entry is string {
  if (typeof entry !== 'string') return false
  try {
    const { protocol } = new URL(entry)
    return protocol === 'ws:' || protocol === 'wss:'
  } catch {
    return false
  }
}

/** Whether a fetch failed because nothing listens on the port. */
function refused(err: unknown): boolean {
  const cause = (err as { cause?: { code?: unknown } } | null)?.cause
  return cause?.code === 'ECONNREFUSED'
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}
