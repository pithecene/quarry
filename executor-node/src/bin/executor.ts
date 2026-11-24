#!/usr/bin/env node
/**
 * CLI entrypoint for the Quarry executor.
 *
 * Usage:
 *   quarry-executor <script-path>
 *
 * Run metadata is read from stdin as a JSON object with fields:
 * - run_id (string, required)
 * - attempt (number, required)
 * - job_id (string, optional)
 * - parent_run_id (string, optional)
 * - job (any, required) - the job payload
 *
 * Events are written to stdout as length-prefixed msgpack frames.
 * Stderr is used for executor diagnostics (not protocol).
 *
 * Exit codes:
 * - 0: Script completed (run_complete emitted)
 * - 1: Script error (run_error emitted)
 * - 2: Executor crash (no terminal event emitted)
 * - 3: Invalid arguments or input
 *
 * @module
 */
import { join } from 'node:path'
import type { ProxyEndpoint } from '@pithecene/quarry-sdk'
import { browserProxyArgs, chromiumArgs } from '../browser-args.js'
import { isBrowserKey, serveLeases } from '../browser-lease-server.js'
import { closeRecordedBrowsers, probeBrowser, recordBrowsers } from '../browser-record.js'
import { errorMessage, execute, launchIdentity, parseRunMeta } from '../executor.js'
import { AckReader } from '../ipc/ack-reader.js'
import { drainStdout } from '../ipc/sink.js'
import { installStdoutGuard } from '../ipc/stdout-guard.js'
import { type LoadedScript, loadScript, ScriptLoadError } from '../loader.js'
import { parseStoragePartition } from '../parse-storage-partition.js'

/**
 * Write an error message to stderr and exit with code 3 (invalid input).
 */
function fatalError(message: string): never {
  process.stderr.write(`Error: ${message}\n`)
  process.exit(3)
}

/** Known lifecycle hook names to report. */
const HOOK_NAMES = [
  'prepare',
  'beforeRun',
  'afterRun',
  'onError',
  'beforeTerminal',
  'cleanup'
] as const

/**
 * The identity of the browser that this process would launch for scriptPath,
 * with the plugins and arguments that the environment selects.
 */
function currentLaunchIdentity(scriptPath: string): ReturnType<typeof launchIdentity> {
  return launchIdentity(
    scriptPath,
    {
      stealth: process.env.QUARRY_STEALTH !== '0',
      adblocker: process.env.QUARRY_ADBLOCKER === '1'
    },
    chromiumArgs(browserProxyArgs())
  )
}

/**
 * Validate a script module without executing it.
 *
 * Loads the script, validates its shape, and prints a JSON result to stdout.
 *
 * Exit codes:
 * - 0: Script is valid
 * - 1: Script load or validation failed
 */
async function validateScript(scriptPath: string): Promise<never> {
  let loaded: LoadedScript
  try {
    loaded = await loadScript(scriptPath)
  } catch (err) {
    const message = err instanceof ScriptLoadError ? err.message : errorMessage(err)
    const result = { valid: false, error: message }
    process.stdout.write(`${JSON.stringify(result)}\n`)
    process.exit(1)
  }

  // Report which hooks are present
  const hooks: string[] = []
  for (const name of HOOK_NAMES) {
    if (loaded.hooks[name] !== undefined) {
      hooks.push(name)
    }
  }

  const result = {
    valid: true,
    exports: {
      default: true,
      hooks
    }
  }
  process.stdout.write(`${JSON.stringify(result)}\n`)
  process.exit(0)
}

/**
 * Parse optional proxy endpoint from input.
 * Returns undefined if no proxy is configured.
 */
function parseProxy(input: Record<string, unknown>): ProxyEndpoint | undefined {
  if (!('proxy' in input) || input.proxy === null || input.proxy === undefined) {
    return undefined
  }

  const proxy = input.proxy as Record<string, unknown>

  // Validate required fields
  if (typeof proxy.protocol !== 'string') {
    throw new Error('proxy.protocol must be a string')
  }
  if (typeof proxy.host !== 'string' || proxy.host === '') {
    throw new Error('proxy.host must be a non-empty string')
  }
  if (
    typeof proxy.port !== 'number' ||
    !Number.isInteger(proxy.port) ||
    proxy.port < 1 ||
    proxy.port > 65535
  ) {
    throw new Error('proxy.port must be an integer between 1 and 65535')
  }

  // Validate protocol
  const validProtocols = ['http', 'https', 'socks5']
  if (!validProtocols.includes(proxy.protocol)) {
    throw new Error(`proxy.protocol must be one of: ${validProtocols.join(', ')}`)
  }

  // Validate auth pair
  const hasUsername = typeof proxy.username === 'string' && proxy.username !== ''
  const hasPassword = typeof proxy.password === 'string' && proxy.password !== ''
  if (hasUsername !== hasPassword) {
    throw new Error('proxy.username and proxy.password must be provided together')
  }

  return {
    protocol: proxy.protocol as 'http' | 'https' | 'socks5',
    host: proxy.host,
    port: proxy.port,
    ...(hasUsername && { username: proxy.username as string }),
    ...(hasPassword && { password: proxy.password as string })
  }
}

/**
 * Read JSON metadata from stdin (phase 1 of two-phase stdin).
 *
 * Go's json.NewEncoder appends a newline after the JSON object.
 * We read bytes until the first newline, parse JSON, and leave stdin
 * open for phase 2 (ack frames via AckReader).
 *
 * @returns Parsed JSON metadata object
 */
async function readStdinMetadata(): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const chunks: Buffer[] = []

    function onReadable(): void {
      for (
        let chunk = process.stdin.read() as Buffer | null;
        chunk !== null;
        chunk = process.stdin.read() as Buffer | null
      ) {
        // Scan for newline in this chunk
        const nlIndex = chunk.indexOf(0x0a) // '\n'
        if (nlIndex !== -1) {
          // Take everything up to (not including) the newline
          chunks.push(chunk.subarray(0, nlIndex))

          // Unshift remaining bytes back to stdin for phase 2 (ack reader)
          const remaining = chunk.subarray(nlIndex + 1)
          if (remaining.length > 0) {
            process.stdin.unshift(remaining)
          }

          cleanup()
          resolve(Buffer.concat(chunks).toString('utf-8'))
          return
        }
        chunks.push(chunk)
      }
    }

    function onEnd(): void {
      // stdin closed before newline — use whatever we have
      cleanup()
      resolve(Buffer.concat(chunks).toString('utf-8'))
    }

    function onError(err: Error): void {
      cleanup()
      reject(err)
    }

    function cleanup(): void {
      process.stdin.removeListener('readable', onReadable)
      process.stdin.removeListener('end', onEnd)
      process.stdin.removeListener('error', onError)
    }

    process.stdin.on('readable', onReadable)
    process.stdin.on('end', onEnd)
    process.stdin.on('error', onError)
  })
}

/**
 * Run a long-lived browser server that runs share through leases.
 *
 * Unlike --launch-browser (which waits for stdin EOF), this mode manages its
 * own lifetime. The Go runtime launches it as a detached process that
 * outlives the run that started it, and every run that uses it holds a
 * connection to its lease socket (see browser-lease-server.ts).
 *
 * Lifecycle:
 * 1. Close the browsers earlier servers left running, if any (see
 *    browser-record.ts), then launch the browser with plugins and record it
 * 2. Listen on `server.sock` in QUARRY_BROWSER_REUSE_DIR, then print `ready`
 * 3. Shut down once no run has held a lease for the idle timeout, when the
 *    browser closes or crashes, or on SIGTERM or SIGINT
 */
async function browserServer(scriptPath: string): Promise<never> {
  // The runtime always supplies the key of its launch settings; a server
  // without one could match no run correctly, so it does not start.
  const browserKey = process.env.QUARRY_BROWSER_KEY ?? ''
  if (!isBrowserKey(browserKey)) {
    process.stderr.write('QUARRY_BROWSER_KEY must be the sha256: key of the launch settings\n')
    process.exit(3)
  }
  // The runtime keyed this server on the identity it saw before it took the
  // lock. Launching only if this process sees the same one keeps a change in
  // between, such as an edited .puppeteerrc, from serving the old key.
  const expected = process.env.QUARRY_BROWSER_IDENTITY ?? ''
  const identity = await currentLaunchIdentity(scriptPath)
  if (expected === '' || JSON.stringify(JSON.parse(expected)) !== JSON.stringify(identity)) {
    process.stderr.write(
      'the launch identity changed after the runtime keyed this server; not starting\n'
    )
    process.exit(3)
  }
  const { getPuppeteer: getBrowserPuppeteer, getVanillaPuppeteer } = await import('../executor.js')
  const reuseDir = process.env.QUARRY_BROWSER_REUSE_DIR ?? '.'
  const recordPath = join(reuseDir, 'browser.json')
  const log = (message: string): void => void process.stderr.write(`${message}\n`)

  const vanilla = await getVanillaPuppeteer(scriptPath)
  const stillListed = await closeRecordedBrowsers(
    recordPath,
    (options) => vanilla.connect(options),
    probeBrowser,
    log
  )

  const puppeteer = await getBrowserPuppeteer(scriptPath, {
    stealth: process.env.QUARRY_STEALTH !== '0',
    adblocker: process.env.QUARRY_ADBLOCKER === '1'
  })

  // The proxy applies at the browser level, to every run that connects.
  // The launch takes the executable it verified, so a configuration edited since
  // cannot select another.
  const browser = await puppeteer.launch({
    headless: true,
    executablePath: identity.executablePath,
    args: chromiumArgs(browserProxyArgs())
  })
  // In the same tick as the launch, so that a server killed from here on
  // leaves its browser listed for the next server to close.
  try {
    recordBrowsers(recordPath, [...stillListed, browser.wsEndpoint()])
  } catch (err) {
    await browser.close()
    throw err
  }

  // Subscribe before anything awaits, so no stop reason is missed.
  const stopped = Promise.race([
    new Promise<Stop>((resolve) =>
      browser.once('disconnected', () => resolve({ reason: 'browser disconnected', code: 1 }))
    ),
    ...(['SIGTERM', 'SIGINT'] as const).map(
      (signal) =>
        new Promise<Stop>((resolve) =>
          process.once(signal, () => resolve({ reason: signal, code: 0 }))
        )
    )
  ])

  const leases = await serveLeases({
    socketPath: join(reuseDir, 'server.sock'),
    wsEndpoint: browser.wsEndpoint(),
    browserKey,
    idleTimeoutMs: Number.parseInt(process.env.QUARRY_BROWSER_IDLE_TIMEOUT ?? '60', 10) * 1000
  }).catch(async (err: unknown) => {
    await browser.close()
    throw err
  })

  // The runtime closes its end of stdout once it reads `ready`
  process.on('SIGPIPE', () => undefined)
  // A browser lost while the socket started is never offered to a run.
  if (browser.connected) process.stdout.write('ready\n')

  const stop = await Promise.race([
    stopped,
    leases.idle.then((): Stop => ({ reason: 'no lease for the idle timeout', code: 0 }))
  ])
  process.stderr.write(`Browser server shutting down: ${stop.reason}\n`)
  leases.close()
  await browser.close().catch(() => undefined)
  process.exit(stop.code)
}

/** Why the browser server stops, and its exit code. */
type Stop = { reason: string; code: number }

/**
 * Launch a shared browser and print its WebSocket endpoint to stdout.
 * Stays alive until stdin closes, then shuts down the browser.
 *
 * Used by the Go runtime to provide a managed browser for fan-out runs.
 * The script path is used to resolve puppeteer from the user's project.
 */
async function launchBrowserServer(scriptPath: string): Promise<never> {
  const { getPuppeteer: getBrowserPuppeteer } = await import('../executor.js')

  const puppeteer = await getBrowserPuppeteer(scriptPath, {
    stealth: process.env.QUARRY_STEALTH !== '0',
    adblocker: process.env.QUARRY_ADBLOCKER === '1'
  })

  const browser = await puppeteer.launch({
    headless: true,
    args: chromiumArgs(browserProxyArgs())
  })

  // Print WS endpoint to stdout (Go runtime reads this line)
  const wsEndpoint = browser.wsEndpoint()
  process.stdout.write(`${wsEndpoint}\n`)

  // Stay alive until stdin closes (Go runtime closes stdin to signal shutdown)
  await new Promise<void>((resolve) => {
    process.stdin.resume()
    process.stdin.on('end', resolve)
    process.stdin.on('close', resolve)
    process.on('SIGTERM', resolve)
    process.on('SIGINT', resolve)
  })

  await browser.close()
  process.exit(0)
}

/**
 * Print what a browser launched for this run would be, so the runtime keys
 * browser reuse on what Puppeteer actually resolves.
 */
async function reportLaunchIdentity(scriptPath: string): Promise<never> {
  const report = `${JSON.stringify(await currentLaunchIdentity(scriptPath))}\n`
  // Exit only once the report has left: a pipe can take a write in parts.
  await new Promise<void>((done) => process.stdout.write(report, () => done()))
  process.exit(0)
}

/** The modes that replace a run, by flag. */
const MODES = new Map<string, (scriptPath: string) => Promise<never>>([
  // Reusable browser server: self-managing lifetime with idle timeout
  ['--browser-server', browserServer],
  // Report the browser that a launch for this run would start
  ['--launch-identity', reportLaunchIdentity],
  // Load the script and check its shape without executing
  ['--validate', validateScript],
  // Fan-out browser server: stdin-managed lifetime
  ['--launch-browser', launchBrowserServer]
])

/**
 * Main entry point.
 */
async function main(): Promise<never> {
  const args = process.argv.slice(2)

  // Register the ESM resolve hook for --resolve-from before any mode loads a
  // module, so a shared browser loads what its runs load. NODE_PATH only works
  // for CJS require(); ESM needs module.register(). The hook retries with a
  // fallback parentURL, which preserves ESM import conditions.
  const resolveFrom = process.env.QUARRY_RESOLVE_FROM
  if (resolveFrom) {
    const { registerResolveFromHook } = await import('../resolve-from.js')
    await registerResolveFromHook(resolveFrom)
  }

  const mode = MODES.get(args[0] ?? '')
  if (mode) {
    const modeScript = args[1]
    if (!modeScript) {
      process.stderr.write(`Usage: quarry-executor ${args[0]} <script-path>\n`)
      process.exit(3)
    }
    return mode(modeScript)
  }

  if (args.length < 1) {
    process.stderr.write('Usage: quarry-executor <script-path>\n')
    process.stderr.write('Run metadata is read from stdin as JSON.\n')
    process.exit(3)
  }

  // Protect IPC channel from stray stdout writes by third-party code.
  // Must be installed after browser-mode early returns (which legitimately
  // write text to stdout) but before any IPC framing begins.
  const { ipcOutput, ipcWrite } = installStdoutGuard()

  // A cancelled run ends here. Puppeteer's own SIGTERM listener stops Node from
  // ending, and its exit listener kills a browser it launched or is launching.
  process.once('SIGTERM', () => process.exit(143))

  const scriptPath = args[0]

  // Read metadata from stdin (phase 1 of two-phase stdin).
  // stdin remains open for ack frames (phase 2).
  let input: unknown
  try {
    const stdinData = await readStdinMetadata()
    if (stdinData.trim() === '') {
      fatalError('stdin is empty, expected JSON input')
    }
    input = JSON.parse(stdinData)
  } catch (err) {
    fatalError(`parsing stdin JSON: ${errorMessage(err)}`)
  }

  if (input === null || typeof input !== 'object') {
    fatalError('stdin must be a JSON object')
  }

  const inputObj = input as Record<string, unknown>

  // Parse run metadata
  let run: ReturnType<typeof parseRunMeta>
  try {
    run = parseRunMeta(inputObj)
  } catch (err) {
    fatalError(`parsing run metadata: ${errorMessage(err)}`)
  }

  // Extract job payload
  if (!('job' in inputObj)) {
    fatalError('missing "job" field in input')
  }
  const job = inputObj.job

  // Parse optional proxy
  let proxy: ProxyEndpoint | undefined
  try {
    proxy = parseProxy(inputObj)
  } catch (err) {
    fatalError(`parsing proxy: ${errorMessage(err)}`)
  }

  // Parse optional storage partition metadata for SDK-side key computation
  const storagePartition = parseStoragePartition(inputObj, (msg) => {
    process.stderr.write(`Warning: ${msg}\n`)
  })

  // Parse optional browser WebSocket endpoint
  const browserWSEndpoint =
    typeof inputObj.browser_ws_endpoint === 'string' && inputObj.browser_ws_endpoint !== ''
      ? inputObj.browser_ws_endpoint
      : undefined

  // The run declares no browser (CONTRACT_RUN.md §Runs Without a Browser)
  const noBrowser = inputObj.no_browser === true

  // Start AckReader on stdin (phase 2 of two-phase stdin).
  // stdin remains open after metadata read for runtime→executor ack frames.
  const ackReader = new AckReader(process.stdin)
  ackReader.start()

  // Execute
  const result = await execute({
    scriptPath,
    job,
    run,
    proxy,
    storagePartition,
    browserWSEndpoint,
    noBrowser,
    ackReader,
    output: ipcOutput,
    outputWrite: ipcWrite,
    puppeteerOptions: {
      headless: true,
      args: chromiumArgs()
    },
    // Stealth on by default; disable with QUARRY_STEALTH=0
    stealth: process.env.QUARRY_STEALTH !== '0',
    // Adblocker off by default; enable with QUARRY_ADBLOCKER=1
    adblocker: process.env.QUARRY_ADBLOCKER === '1'
  })

  // Stop AckReader after execution completes
  ackReader.stop()

  // Flush stdout so the runtime sees the terminal event before EOF
  await drainStdout()

  // Map outcome to exit code
  switch (result.outcome.status) {
    case 'completed':
      process.exit(0)
      break
    case 'error':
      process.exit(1)
      break
    case 'crash':
      process.stderr.write(`Executor crash: ${result.outcome.message}\n`)
      process.exit(2)
      break
    default: {
      // Exhaustiveness check
      const _exhaustive: never = result.outcome
      process.exit(2)
    }
  }
}

main().catch((err) => {
  process.stderr.write(`Unexpected error: ${errorMessage(err)}\n`)
  process.exit(2)
})
