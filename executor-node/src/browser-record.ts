import { readFileSync, renameSync, writeFileSync } from 'node:fs'

/** Connects to a browser by endpoint; `puppeteer.connect` in production. */
export type ConnectBrowser = (options: {
  browserWSEndpoint: string
}) => Promise<{ close(): Promise<void> }>

/** How long closing a recorded browser may take before the server moves on. */
const CLOSE_TIMEOUT_MS = 5_000

/**
 * Close the browser that `recordPath` names, if it still runs.
 *
 * A server records its browser's endpoint as it starts. One that ends without
 * closing its browser, as when killed with SIGKILL, leaves the browser
 * running with its endpoint recorded, so the next server closes it. A server
 * starts only while the runtime holds the discovery lock and no server
 * listens, so the recorded browser belongs to no server that a run can
 * reach. The endpoint carries the browser's own ID, so it never names
 * another browser. A record that cannot be read is logged and left alone.
 */
export async function closeRecordedBrowser(
  recordPath: string,
  connect: ConnectBrowser,
  log: (message: string) => void
): Promise<void> {
  const endpoint = readRecordedEndpoint(recordPath, log)
  if (endpoint === undefined) return
  try {
    const browser = await withTimeout(connect({ browserWSEndpoint: endpoint }), CLOSE_TIMEOUT_MS)
    await withTimeout(browser.close(), CLOSE_TIMEOUT_MS)
    log(`Closed the browser an earlier server left running (${endpoint})`)
  } catch {
    // Closed with its server, as is usual
  }
}

/** Record `endpoint` as this server's browser, replacing the file atomically. */
export function recordBrowser(recordPath: string, endpoint: string): void {
  const tmp = `${recordPath}.tmp`
  writeFileSync(tmp, `${JSON.stringify({ ws_endpoint: endpoint })}\n`, { mode: 0o600 })
  renameSync(tmp, recordPath)
}

function readRecordedEndpoint(
  recordPath: string,
  log: (message: string) => void
): string | undefined {
  let text: string
  try {
    text = readFileSync(recordPath, 'utf8')
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
      log(`Cannot read ${recordPath}, leaving any browser it names: ${String(err)}`)
    }
    return undefined
  }
  try {
    const endpoint = (JSON.parse(text) as { ws_endpoint?: unknown } | null)?.ws_endpoint
    if (typeof endpoint === 'string' && endpoint !== '') return endpoint
  } catch {
    // Reported below
  }
  log(`Ignoring malformed ${recordPath}`)
  return undefined
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}
