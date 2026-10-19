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
import { unlinkSync } from 'node:fs'
import type { ProxyEndpoint } from '@pithecene/quarry-sdk'
import { chromiumArgs } from '../browser-args.js'
import { evaluateIdlePoll, type IdlePollState } from '../browser-idle.js'
import { errorMessage, execute, parseRunMeta } from '../executor.js'
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
 * Validate a script module without executing it.
 *
 * Registers the --resolve-from ESM hook if configured, loads the script,
 * validates its shape, and prints a JSON result to stdout.
 *
 * Exit codes:
 * - 0: Script is valid
 * - 1: Script load or validation failed
 */
async function validateScript(scriptPath: string): Promise<never> {
  // Register ESM resolve hook if configured (same as normal execution path)
  const resolveFrom = process.env.QUARRY_RESOLVE_FROM
  if (resolveFrom) {
    const { registerResolveFromHook } = await import('../resolve-from.js')
    await registerResolveFromHook(resolveFrom)
  }

  let loaded: LoadedScript
  try {
    loaded = await loadScript(scriptPath)
  } catch (err) {
    const message = err instanceof ScriptLoadError ? err.message : errorMessage(err)
    const result = { valid: false, error: message }
    process.stdout.write(JSON.stringify(result) + '\n')
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
  process.stdout.write(JSON.stringify(result) + '\n')
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
      let chunk: Buffer | null
      while ((chunk = process.stdin.read() as Buffer | null) !== null) {
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
 * Run a long-lived browser server that self-terminates after idle timeout.
 *
 * Unlike --launch-browser (which waits for stdin EOF), this mode manages its
 * own lifetime via idle monitoring. The Go runtime launches this as a detached
 * process that outlives the parent quarry run.
 *
 * Lifecycle:
 * 1. Launch browser with plugins
 * 2. Print WS endpoint to stdout
 * 3. Monitor /json/list for active pages every 5s
 * 4. After idle timeout with no active pages, shut down
 * 5. Remove discovery file on exit
 */
async function browserServer(scriptPath: string): Promise<never> {
  const { getPuppeteer: getBrowserPuppeteer } = await import('../executor.js')

  const puppeteer = await getBrowserPuppeteer(scriptPath, {
    stealth: process.env.QUARRY_STEALTH !== '0',
    adblocker: process.env.QUARRY_ADBLOCKER === '1'
  })

  // Build Chromium launch args (proxy applied at the browser level)
  const proxyUrl = process.env.QUARRY_BROWSER_PROXY
  const proxyArgs = proxyUrl ? [`--proxy-server=${proxyUrl}`] : []

  const browser = await puppeteer.launch({
    headless: true,
    args: chromiumArgs(proxyArgs)
  })

  const wsEndpoint = browser.wsEndpoint()
  process.stdout.write(`${wsEndpoint}\n`)

  // Handle SIGPIPE gracefully (parent may close stdout after reading endpoint)
  process.on('SIGPIPE', () => {
    // Ignore — parent read the endpoint and closed its pipe
  })

  const idleTimeoutMs = Number.parseInt(process.env.QUARRY_BROWSER_IDLE_TIMEOUT ?? '60', 10) * 1000
  const discoveryFile = process.env.QUARRY_BROWSER_DISCOVERY_FILE ?? ''

  // Extract port from WS endpoint for HTTP health queries
  const wsUrl = new URL(wsEndpoint)
  const baseUrl = `http://127.0.0.1:${wsUrl.port}`

  const pollIntervalMs = 5_000

  /**
   * Count active page targets (excluding the default about:blank).
   * Uses Chromium's /json/list endpoint — no CDP protocol needed.
   */
  async function countActivePages(): Promise<number> {
    const res = await fetch(`${baseUrl}/json/list`)
    if (!res.ok) throw new Error(`/json/list returned ${res.status}`)
    const targets = (await res.json()) as Array<{ type: string; url: string }>
    return targets.filter((t) => t.type === 'page' && t.url !== 'about:blank').length
  }

  /** Remove the discovery file on shutdown (best effort). */
  function removeDiscoveryFile(): void {
    if (!discoveryFile) return
    try {
      unlinkSync(discoveryFile)
    } catch {
      // Already removed or inaccessible — fine
    }
  }

  // Idle monitoring state — declared before shutdown() which reads it
  const pollConfig = { idleTimeoutMs, maxConsecutiveFailures: 3 }
  let pollState: IdlePollState = { idleStartedAt: null, consecutiveFailures: 0 }

  async function shutdown(): Promise<never> {
    const idleSec = pollState.idleStartedAt
      ? Math.round((Date.now() - pollState.idleStartedAt) / 1000)
      : 0
    process.stderr.write(`Browser server idle for ${idleSec}s, shutting down\n`)
    removeDiscoveryFile()
    await browser.close()
    process.exit(0)
  }

  // Graceful shutdown on signals
  process.on('SIGTERM', () => void shutdown())
  process.on('SIGINT', () => void shutdown())

  const timer = setInterval(async () => {
    let fetchResult: { ok: true; activePages: number } | { ok: false }
    try {
      fetchResult = { ok: true, activePages: await countActivePages() }
    } catch {
      fetchResult = { ok: false }
    }

    const action = evaluateIdlePoll(fetchResult, pollState, pollConfig)

    switch (action.type) {
      case 'continue':
        pollState = action.nextState
        return
      case 'shutdown':
        clearInterval(timer)
        await shutdown()
        return
      case 'crash-exit':
        process.stderr.write(
          `Browser health check failed ${action.failures} times consecutively, exiting\n`
        )
        clearInterval(timer)
        removeDiscoveryFile()
        process.exit(1)
    }
  }, pollIntervalMs)

  // Block forever — process exits via shutdown() or signal handlers
  // eslint-disable-next-line no-constant-condition
  while (true) {
    await new Promise((resolve) => setTimeout(resolve, 2_147_483_647))
  }
}

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
    args: chromiumArgs()
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
 * Main entry point.
 */
async function main(): Promise<never> {
  const args = process.argv.slice(2)

  // Reusable browser server mode: self-managing lifetime with idle timeout
  if (args[0] === '--browser-server') {
    const scriptPath = args[1]
    if (!scriptPath) {
      process.stderr.write('Usage: quarry-executor --browser-server <script-path>\n')
      process.exit(3)
    }
    return browserServer(scriptPath)
  }

  // Validate mode: load script and check shape without executing
  if (args[0] === '--validate') {
    const scriptPath = args[1]
    if (!scriptPath) {
      process.stderr.write('Usage: quarry-executor --validate <script-path>\n')
      process.exit(3)
    }
    return validateScript(scriptPath)
  }

  // Legacy browser server mode: stdin-managed lifetime for fan-out
  if (args[0] === '--launch-browser') {
    const scriptPath = args[1]
    if (!scriptPath) {
      process.stderr.write('Usage: quarry-executor --launch-browser <script-path>\n')
      process.exit(3)
    }
    return launchBrowserServer(scriptPath)
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

  // Register ESM resolve hook for --resolve-from support.
  // NODE_PATH only works for CJS require(); ESM needs module.register().
  // The hook retries bare-specifier resolution with a fallback parentURL
  // so ESM import conditions (not CJS require conditions) are preserved.
  const resolveFrom = process.env.QUARRY_RESOLVE_FROM
  if (resolveFrom) {
    const { registerResolveFromHook } = await import('../resolve-from.js')
    await registerResolveFromHook(resolveFrom)
  }

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
