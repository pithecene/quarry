/**
 * Executor: orchestrates script execution with proper lifecycle management.
 *
 * Responsibilities:
 * - Initialize Puppeteer browser/page
 * - Create context with EmitSink
 * - Execute script with lifecycle hooks
 * - Auto-emit terminal events (run_complete/run_error) if not already emitted
 * - Determine outcome based on actually written terminal events
 * - Clean up resources
 *
 * Outcome determination (in precedence order):
 * 1. Sink failure at any point → crash
 * 2. Terminal event successfully written → match that event
 * 3. Script threw without terminal → error (auto-emit run_error)
 * 4. Script completed without terminal → completed (auto-emit run_complete)
 *
 * @module
 */
import { accessSync, constants, readFileSync, realpathSync, writeSync } from 'node:fs'
import { createRequire } from 'node:module'
import { basename, delimiter, dirname, isAbsolute, join, resolve, sep } from 'node:path'
import type { Writable } from 'node:stream'
import { fileURLToPath, pathToFileURL } from 'node:url'
import {
  createAPIs,
  createContext,
  type JobId,
  type PrepareResult,
  type ProxyEndpoint,
  type RunId,
  type RunMeta,
  type StoragePartitionMeta,
  TerminalEventError,
  type TerminalSignal
} from '@pithecene/quarry-sdk'
import type { Browser, BrowserContext, LaunchOptions, Page } from 'puppeteer'
import type { AckReader } from './ipc/ack-reader.js'
import type { ProxyEndpointRedactedFrame, RunResultOutcome } from './ipc/frame.js'
import { ObservingSink, type SinkState } from './ipc/observing-sink.js'
import { StdioSink } from './ipc/sink.js'
import { type LoadedScript, loadScript, ScriptLoadError } from './loader.js'

/**
 * Plugin configuration for browser hardening.
 */
export type PluginConfig = {
  readonly stealth: boolean
  readonly adblocker: boolean
}

/** A loaded module and the file it loaded from. */
type LoadedModule = { readonly module: Record<string, unknown>; readonly location: string }

/**
 * Resolve a package from the node_modules of the script's directory and its
 * ancestors, then NODE_PATH. Node's global folders ($HOME/.node_modules and the
 * like) are refused: nothing in the run's configuration names them.
 */
export function resolvePeer(name: string, scriptPath: string): string {
  const require = createRequire(resolve(scriptPath))
  const nodePath = (process.env.NODE_PATH ?? '').split(delimiter)
  const found = require.resolve(name)
  const permitted = (require.resolve.paths(name) ?? [])
    .filter((dir) => basename(dir) === 'node_modules' || nodePath.includes(dir))
    .some((dir) => holds(join(dir, name), found))
  if (!permitted) {
    throw new Error(`Cannot find package '${name}' from ${resolve(scriptPath)}: refused ${found}`)
  }
  return found
}

// holds reports whether file is inside the package at dir, which may be a link.
function holds(dir: string, file: string): boolean {
  try {
    return file.startsWith(dir + sep) || file.startsWith(realpathSync(dir) + sep)
  } catch {
    return false
  }
}

/**
 * Load a module from the script's directory, falling back to standard import,
 * and report the file that the successful import loaded.
 *
 * The embedded bundle runs from memory, so dependencies must be resolved from
 * the user's script location or the configured module root.
 */
async function loadModule(name: string, scriptPath: string): Promise<LoadedModule> {
  // Strategy 1: resolve from script's directory. The URL
  // form loads from a bundle that has no directory of its own.
  let scriptError: unknown
  try {
    const location = resolvePeer(name, scriptPath)
    return { module: await import(pathToFileURL(location).href), location }
  } catch (err) {
    scriptError = err
  }

  // Strategy 2: Standard import, through --resolve-from for the embedded bundle
  try {
    const module = (await import(name)) as Record<string, unknown>
    return { module, location: fileURLToPath(import.meta.resolve(name)) }
  } catch (err) {
    throw new Error(`${errorMessage(scriptError)}; fallback import: ${errorMessage(err)}`, {
      cause: err
    })
  }
}

/** Resolve a module from the script's directory, falling back to standard import. */
async function resolveModule(name: string, scriptPath: string): Promise<Record<string, unknown>> {
  return (await loadModule(name, scriptPath)).module
}

/** Where a module loaded from and which version it is. */
export type ModuleIdentity = { readonly location: string; readonly version: string }

/**
 * What a browser that this process launches would be: the modules that would
 * load, the Chrome binary Puppeteer would start, the plugins, and the
 * effective launch arguments. A reusable browser is shared only with runs
 * whose identity is the same, so the identity comes from the same loading as
 * the launch, and anything it cannot establish fails the report.
 */
export type LaunchIdentity = {
  readonly modules: Readonly<Record<string, ModuleIdentity>>
  readonly executablePath: string
  readonly plugins: PluginConfig
  readonly args: readonly string[]
}

/**
 * The version of the package that owns `location`: the nearest package.json
 * above it with a version, which also names an aliased install correctly.
 */
function owningVersion(name: string, location: string): string {
  for (let dir = dirname(location); ; dir = dirname(dir)) {
    try {
      const pkg = JSON.parse(readFileSync(join(dir, 'package.json'), 'utf8')) as {
        version?: unknown
      }
      if (typeof pkg.version === 'string' && pkg.version !== '') return pkg.version
    } catch {
      // No package.json here; keep walking up.
    }
    if (dirname(dir) === dir)
      throw new Error(`cannot identify the version of ${name} loaded from ${location}`)
  }
}

/**
 * The file that a launch of `path` runs, as the launch looks it up: an
 * absolute path as given, a relative path against the working directory, and
 * a bare name through PATH. Symlinks are resolved, so two names for one
 * binary agree and one name for two binaries does not.
 */
function executableFile(path: string): string {
  if (isAbsolute(path) || path.includes(sep)) return realpathSync(resolve(path))
  for (const dir of (process.env.PATH ?? '').split(delimiter)) {
    if (dir === '') continue
    const candidate = join(dir, path)
    try {
      accessSync(candidate, constants.X_OK)
      return realpathSync(candidate)
    } catch {
      // Not here; keep looking.
    }
  }
  throw new Error(`cannot find the Chrome executable ${path} on PATH`)
}

/** The Puppeteer surface that the identity reads. */
type PuppeteerIdentitySurface = {
  executablePath?: () => string | Promise<string>
  defaultArgs?: (options: {
    headless: boolean
    args: readonly string[]
  }) => string[] | Promise<string[]>
}

/** Compute the identity of the browser that this process would launch. */
export async function launchIdentity(
  scriptPath: string,
  plugins: PluginConfig,
  args: readonly string[]
): Promise<LaunchIdentity> {
  const names = ['puppeteer']
  if (plugins.stealth || plugins.adblocker) names.push('puppeteer-extra')
  if (plugins.stealth) names.push('puppeteer-extra-plugin-stealth')
  if (plugins.adblocker) names.push('puppeteer-extra-plugin-adblocker')
  const modules: Record<string, ModuleIdentity> = {}
  let puppeteer: PuppeteerIdentitySurface | undefined
  for (const name of names) {
    const loaded = await loadModule(name, scriptPath)
    modules[name] = { location: loaded.location, version: owningVersion(name, loaded.location) }
    if (name === 'puppeteer') puppeteer = loaded.module.default as PuppeteerIdentitySurface
  }
  if (
    typeof puppeteer?.executablePath !== 'function' ||
    typeof puppeteer.defaultArgs !== 'function'
  ) {
    throw new Error(
      'puppeteer exposes no executablePath() and defaultArgs() to identify its launch'
    )
  }
  const configured = await puppeteer.executablePath()
  if (typeof configured !== 'string' || configured === '') {
    throw new Error('puppeteer reported no Chrome executable')
  }
  return {
    modules,
    executablePath: executableFile(configured),
    plugins,
    // Puppeteer 25 returns both as promises.
    args: await puppeteer.defaultArgs({ headless: true, args })
  }
}

/**
 * Report a per-run browser to the runtime on the descriptor that
 * QUARRY_BROWSER_PID_FD names. Puppeteer starts Chromium in a process group of
 * its own, so a signal to this executor's group does not reach it: the runtime
 * stops every reported browser that was not reported closed once this process
 * ends. A browser is reported closed only after its process has exited.
 * @internal
 */
export function reportBrowser(
  event: 'launched' | 'closed',
  browser: Pick<Browser, 'process'>
): void {
  const fd = Number.parseInt(process.env.QUARRY_BROWSER_PID_FD ?? '', 10)
  if (!Number.isInteger(fd)) return
  const child = browser.process()
  if (child?.pid === undefined) return
  if (event === 'closed' && child.exitCode === null && child.signalCode === null) return
  try {
    writeSync(fd, `${event} ${child.pid}\n`)
  } catch {
    // The runtime has closed its end: it no longer supervises this run.
  }
}

/**
 * Extract a human-readable message from an unknown error value.
 */
export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

/**
 * Resolve a module or throw a descriptive error with install hints.
 */
async function resolveModuleOrThrow(
  name: string,
  scriptPath: string,
  hint: string
): Promise<Record<string, unknown>> {
  try {
    return await resolveModule(name, scriptPath)
  } catch (err) {
    const scriptDir = dirname(resolve(scriptPath))
    throw new Error(
      `Failed to load ${name}: ${errorMessage(err)}\n${hint}\n` +
        `Install it in your project (${scriptDir}): npm install ${name}`
    )
  }
}

/**
 * Lazily import puppeteer with puppeteer-extra plugin support.
 *
 * Resolution strategy:
 * 1. Resolve vanilla puppeteer from the script's directory
 * 2. With no plugin enabled, return it: puppeteer-extra has nothing to add
 * 3. Resolve puppeteer-extra and wrap with addExtra()
 * 4. Apply configured plugins (stealth, adblocker)
 *
 * Uses addExtra() rather than puppeteer-extra's default export to avoid
 * a top-level require('puppeteer') that would fail in the bundled executor
 * model (the Go binary runs executor.mjs from memory).
 */
let puppeteerModule: { launch: (options?: LaunchOptions) => Promise<Browser> } | null = null
let cachedPluginConfig: PluginConfig | null = null

export { getPuppeteer }

async function getPuppeteer(
  scriptPath: string,
  plugins: PluginConfig
): Promise<{ launch: (options?: LaunchOptions) => Promise<Browser> }> {
  if (puppeteerModule && cachedPluginConfig) {
    const configChanged =
      cachedPluginConfig.stealth !== plugins.stealth ||
      cachedPluginConfig.adblocker !== plugins.adblocker
    if (!configChanged) return puppeteerModule
    puppeteerModule = null
    cachedPluginConfig = null
  }

  // 1. Resolve vanilla puppeteer
  const puppeteerMod = await resolveModuleOrThrow(
    'puppeteer',
    scriptPath,
    'Puppeteer is a peer dependency of quarry-executor.'
  )
  const vanillaPuppeteer = puppeteerMod.default as {
    launch: (options?: LaunchOptions) => Promise<Browser>
  }

  // 2. With no plugin enabled, a project need not install puppeteer-extra
  if (!plugins.stealth && !plugins.adblocker) {
    cachedPluginConfig = plugins
    puppeteerModule = vanillaPuppeteer
    return vanillaPuppeteer
  }

  // 3. Resolve puppeteer-extra and wrap vanilla puppeteer
  const extraMod = await resolveModuleOrThrow(
    'puppeteer-extra',
    scriptPath,
    'puppeteer-extra is required while a plugin is enabled, and stealth is enabled by default.\nOr disable stealth with QUARRY_STEALTH=0'
  )
  const { addExtra } = extraMod as {
    addExtra: (puppeteer: unknown) => {
      launch: (options?: LaunchOptions) => Promise<Browser>
      use: (plugin: unknown) => void
    }
  }
  const pptr = addExtra(vanillaPuppeteer)

  // 4. Apply stealth plugin (on by default)
  if (plugins.stealth) {
    const stealthMod = await resolveModuleOrThrow(
      'puppeteer-extra-plugin-stealth',
      scriptPath,
      'Stealth is enabled by default.\nOr disable stealth with QUARRY_STEALTH=0'
    )
    const StealthPlugin = stealthMod.default as () => unknown
    pptr.use(StealthPlugin())
  }

  // 5. Apply adblocker plugin (off by default)
  if (plugins.adblocker) {
    const adblockerMod = await resolveModuleOrThrow(
      'puppeteer-extra-plugin-adblocker',
      scriptPath,
      'Adblocker was enabled but the plugin is not installed.'
    )
    const AdblockerPlugin = adblockerMod.default as (opts?: Record<string, unknown>) => unknown
    pptr.use(AdblockerPlugin({ blockTrackers: true }))
  }

  cachedPluginConfig = plugins
  puppeteerModule = pptr
  return pptr
}

/**
 * Reset cached puppeteer module. For testing only.
 * @internal
 */
export function _resetPuppeteerForTesting(): void {
  puppeteerModule = null
  cachedPluginConfig = null
}

/**
 * Resolve vanilla puppeteer (without puppeteer-extra wrapping) for connect mode.
 * Used when browser-ws-endpoint is set — stealth/adblocker plugins don't apply
 * since we're connecting to an already-running browser.
 */
export async function getVanillaPuppeteer(
  scriptPath: string
): Promise<{ connect: (options: { browserWSEndpoint: string }) => Promise<Browser> }> {
  const puppeteerMod = await resolveModuleOrThrow(
    'puppeteer',
    scriptPath,
    'Puppeteer is a peer dependency of quarry-executor.'
  )
  return puppeteerMod.default as {
    connect: (options: { browserWSEndpoint: string }) => Promise<Browser>
  }
}

/**
 * Executor configuration passed from the runtime.
 */
export type ExecutorConfig<Job = unknown> = {
  /** Path to the script file */
  readonly scriptPath: string
  /** Job payload for the script */
  readonly job: Job
  /** Run metadata */
  readonly run: RunMeta
  /** Output stream for IPC frames (defaults to process.stdout) */
  readonly output?: Writable
  /** Write function for IPC frame data. When provided, bypasses `output.write()`
   *  so the stdout guard patch is not hit, while `output` is still used for
   *  backpressure events and stream state. */
  readonly outputWrite?: (data: Buffer) => boolean
  /** Puppeteer launch options */
  readonly puppeteerOptions?: LaunchOptions
  /** Optional resolved proxy endpoint per CONTRACT_PROXY.md */
  readonly proxy?: ProxyEndpoint
  /** Enable puppeteer-extra stealth plugin (default: true) */
  readonly stealth?: boolean
  /** Enable puppeteer-extra adblocker plugin (default: false) */
  readonly adblocker?: boolean
  /** Optional WebSocket URL of an externally managed browser. When set, executor connects instead of launching. */
  readonly browserWSEndpoint?: string
  /**
   * The run declares no browser: the executor neither launches nor connects
   * to one and never loads Puppeteer (CONTRACT_RUN.md §Runs Without a Browser).
   */
  readonly noBrowser?: boolean
  /** Optional storage partition metadata for SDK-side key computation. */
  readonly storagePartition?: StoragePartitionMeta
  /** Optional AckReader for file_write_ack correlation on stdin. */
  readonly ackReader?: AckReader
}

/**
 * Execution outcome for reporting.
 */
export type ExecutionOutcome =
  | { readonly status: 'completed'; readonly summary?: Record<string, unknown> }
  | {
      readonly status: 'error'
      readonly errorType: string
      readonly message: string
      readonly stack?: string
    }
  | { readonly status: 'crash'; readonly message: string }

/**
 * Result of executor run.
 */
export type ExecutorResult = {
  readonly outcome: ExecutionOutcome
  /** True if a terminal event was successfully written to the sink (by script or executor) */
  readonly terminalEmitted: boolean
}

/**
 * Parse and validate run metadata from raw input.
 *
 * Lineage rules per CONTRACT_RUN.md (strictly enforced):
 * - attempt must be >= 1
 * - If attempt === 1, parent_run_id must be absent (initial run)
 * - If attempt > 1, parent_run_id must be present (retry run)
 *
 * @param input - Raw metadata object
 * @returns Validated RunMeta
 * @throws Error if required fields are missing, invalid, or lineage rules are violated
 */
export function parseRunMeta(input: unknown): RunMeta {
  if (input === null || typeof input !== 'object') {
    throw new Error('run metadata must be an object')
  }

  const obj = input as Record<string, unknown>

  if (typeof obj.run_id !== 'string' || obj.run_id === '') {
    throw new Error('run_id must be a non-empty string')
  }

  if (typeof obj.attempt !== 'number' || !Number.isInteger(obj.attempt) || obj.attempt < 1) {
    throw new Error('attempt must be a positive integer')
  }

  const hasParentRunId = typeof obj.parent_run_id === 'string' && obj.parent_run_id !== ''

  // Strict lineage validation per CONTRACT_RUN.md
  if (obj.attempt === 1 && hasParentRunId) {
    throw new Error('initial run (attempt=1) must not have parent_run_id')
  }
  if (obj.attempt > 1 && !hasParentRunId) {
    throw new Error(`retry run (attempt=${obj.attempt}) must have parent_run_id`)
  }

  // Build RunMeta
  const run: RunMeta = {
    run_id: obj.run_id as RunId,
    attempt: obj.attempt,
    ...(typeof obj.job_id === 'string' && obj.job_id !== '' && { job_id: obj.job_id as JobId }),
    ...(hasParentRunId && { parent_run_id: obj.parent_run_id as RunId })
  }

  return run
}

/**
 * Build Puppeteer launch options with proxy configuration.
 * Per CONTRACT_PROXY.md: Apply proxy host/port/protocol at browser launch.
 *
 * @param baseOptions - Base Puppeteer launch options
 * @param proxy - Optional proxy endpoint
 * @returns Merged launch options with proxy args
 */
function buildPuppeteerLaunchOptions(
  baseOptions: LaunchOptions | undefined,
  proxy: ProxyEndpoint | undefined
): LaunchOptions {
  if (!proxy) {
    return baseOptions ?? {}
  }

  // Build proxy URL (without credentials - those are applied via page.authenticate)
  const proxyUrl = `${proxy.protocol}://${proxy.host}:${proxy.port}`

  // Merge args, preserving existing args from baseOptions
  const existingArgs = baseOptions?.args ?? []
  const proxyArgs = [`--proxy-server=${proxyUrl}`]

  return {
    ...baseOptions,
    args: [...existingArgs, ...proxyArgs]
  }
}

/**
 * Emit run_result control frame. Best-effort — never throws.
 */
async function emitRunResult(
  stdioSink: StdioSink,
  outcome: ExecutionOutcome,
  proxy: ProxyEndpoint | undefined
): Promise<void> {
  try {
    const runResultOutcome = toRunResultOutcome(outcome)
    const proxyUsed = proxy ? redactProxy(proxy) : undefined
    await stdioSink.writeRunResult(runResultOutcome, proxyUsed)
  } catch {
    // Best effort — don't fail the run if run_result emission fails
  }
}

/**
 * Safely close a resource, ignoring errors.
 */
async function safeClose(resource: { close(): Promise<void> } | null): Promise<void> {
  if (!resource) return
  try {
    await resource.close()
  } catch {
    // Ignore close errors
  }
}

/**
 * Determine if an error is a sink failure (vs expected TerminalEventError).
 */
function isSinkFailure(err: unknown): boolean {
  return !(err instanceof TerminalEventError)
}

/**
 * Convert ExecutionOutcome to RunResultOutcome for IPC frame.
 */
function toRunResultOutcome(outcome: ExecutionOutcome): RunResultOutcome {
  switch (outcome.status) {
    case 'completed':
      return {
        status: 'completed',
        message: outcome.summary ? 'run completed with summary' : 'run completed'
      }
    case 'error':
      return {
        status: 'error',
        message: outcome.message,
        error_type: outcome.errorType,
        stack: outcome.stack
      }
    case 'crash':
      return {
        status: 'crash',
        message: outcome.message
      }
  }
}

/**
 * Convert ProxyEndpoint to redacted form for run result.
 * Per CONTRACT_PROXY.md: proxy_used must exclude password fields.
 */
function redactProxy(proxy: ProxyEndpoint): ProxyEndpointRedactedFrame {
  return {
    protocol: proxy.protocol,
    host: proxy.host,
    port: proxy.port,
    ...(proxy.username && { username: proxy.username })
  }
}

/**
 * Execute a script with full lifecycle management.
 *
 * This function:
 * 1. Loads the script
 * 2. Launches Puppeteer
 * 3. Creates the context with ObservingSink
 * 4. Runs lifecycle hooks + script
 * 5. Determines outcome based on sink state and script behavior
 * 6. Emits terminal event if script didn't and sink is healthy
 * 7. Cleans up resources
 *
 * Outcome determination (in precedence order):
 * 1. Sink failure at any point → crash
 * 2. Terminal event successfully written → match that event
 * 3. Script threw without terminal → error (auto-emit run_error)
 * 4. Script completed without terminal → completed (auto-emit run_complete)
 *
 * Lifecycle ordering:
 * - prepare → acquire browser → beforeRun → script → afterRun (success path)
 * - prepare → acquire browser → beforeRun → script → onError (failure path)
 * - beforeTerminal fires after afterRun/onError, before auto-emit terminal
 * - cleanup runs after terminal emission attempt (not called on prepare-skip)
 *
 * @remarks
 * **Cleanup hook contract**: The cleanup hook receives the same context but
 * MUST NOT emit protocol events. Terminal events (run_complete/run_error)
 * are emitted before cleanup runs. Any emit calls in cleanup will throw
 * TerminalEventError.
 *
 * @param config - Executor configuration
 * @returns Execution result
 */
export async function execute<Job = unknown>(config: ExecutorConfig<Job>): Promise<ExecutorResult> {
  const output = config.output ?? process.stdout
  const stdioSink = new StdioSink(output, config.outputWrite, config.ackReader)
  const sink = new ObservingSink(stdioSink)

  let browser: Browser | null = null
  let browserContext: BrowserContext | null = null
  let page: Page | null = null
  let script: LoadedScript<Job> | null = null
  let ctx: ReturnType<typeof createContext<Job>> | null = null
  let scriptThrew = false
  let scriptError: { message: string; stack?: string } | null = null
  let isConnected = false

  /**
   * Determine final outcome based on sink state and script behavior.
   * Precedence: sink failure > terminal state > script error > completed
   */
  function determineOutcome(sinkState: SinkState): ExecutorResult {
    // 1. Sink failure at any point → crash
    if (sinkState.isSinkFailed()) {
      const failure = sinkState.getSinkFailure()
      const message = errorMessage(failure)
      return {
        outcome: { status: 'crash', message },
        terminalEmitted: sinkState.getTerminalState() !== null
      }
    }

    // 2. Terminal event successfully written → match that event
    const terminalState = sinkState.getTerminalState()
    if (terminalState !== null) {
      if (terminalState.type === 'run_error') {
        return {
          outcome: {
            status: 'error',
            errorType: terminalState.errorType ?? 'unknown',
            message: terminalState.message ?? 'Unknown error'
          },
          terminalEmitted: true
        }
      }
      // run_complete
      return {
        outcome: { status: 'completed', summary: terminalState.summary },
        terminalEmitted: true
      }
    }

    // 3. Script threw without terminal → error
    if (scriptThrew && scriptError) {
      return {
        outcome: {
          status: 'error',
          errorType: 'script_error',
          message: scriptError.message,
          stack: scriptError.stack
        },
        terminalEmitted: false
      }
    }

    // 4. Script completed without terminal → completed
    return {
      outcome: { status: 'completed' },
      terminalEmitted: false
    }
  }

  try {
    // 1. Load script
    try {
      script = await loadScript<Job>(config.scriptPath)
    } catch (err: unknown) {
      if (err instanceof ScriptLoadError) {
        return {
          outcome: { status: 'crash', message: err.message },
          terminalEmitted: false
        }
      }
      throw err
    }

    // 2. Run prepare hook (before browser launch)
    let effectiveJob: Job = config.job
    if (script.hooks.prepare) {
      let prepareResult: PrepareResult<Job>
      try {
        prepareResult = await script.hooks.prepare(config.job, config.run)
      } catch (err) {
        const message = errorMessage(err)
        const crashOutcome: ExecutionOutcome = { status: 'crash', message }
        await emitRunResult(stdioSink, crashOutcome, config.proxy)
        return { outcome: crashOutcome, terminalEmitted: false }
      }

      // Validate shape before accessing .action — a null/undefined/non-object
      // return would otherwise throw a generic TypeError with poor diagnostics.
      if (
        prepareResult === null ||
        prepareResult === undefined ||
        typeof prepareResult !== 'object' ||
        !('action' in prepareResult)
      ) {
        const crashOutcome: ExecutionOutcome = {
          status: 'crash',
          message: `prepare hook must return { action: 'continue' | 'skip' }, got: ${String(prepareResult)}`
        }
        await emitRunResult(stdioSink, crashOutcome, config.proxy)
        return { outcome: crashOutcome, terminalEmitted: false }
      }

      if (prepareResult.action === 'skip') {
        // Emit run_complete with skipped summary, no browser launched
        const { emit } = createAPIs(config.run, sink, config.storagePartition)
        const summary: Record<string, unknown> = { skipped: true }
        if (prepareResult.reason !== undefined) {
          summary.reason = prepareResult.reason
        }
        try {
          await emit.runComplete({ summary })
        } catch {
          // Sink failure — determineOutcome will handle
        }
        const result = determineOutcome(sink)
        await emitRunResult(stdioSink, result.outcome, config.proxy)
        return result
      }

      if (prepareResult.action === 'continue') {
        if (prepareResult.job !== undefined) {
          effectiveJob = prepareResult.job
        }
      } else {
        // Unrecognized action — runtime guard per AGENTS.md "explicit behavior"
        const crashOutcome: ExecutionOutcome = {
          status: 'crash',
          message: `prepare hook returned unrecognized action: ${(prepareResult as Record<string, unknown>).action}`
        }
        await emitRunResult(stdioSink, crashOutcome, config.proxy)
        return { outcome: crashOutcome, terminalEmitted: false }
      }
    }

    // 3. Acquire browser (connect to existing or launch new), unless the run
    // declares none
    if (config.noBrowser === true) {
      // No browser: the context's browser members throw NoBrowserError
    } else if (config.browserWSEndpoint) {
      // Connect mode: use vanilla puppeteer (no stealth/adblocker plugins)
      const puppeteer = await getVanillaPuppeteer(config.scriptPath)
      browser = await puppeteer.connect({ browserWSEndpoint: config.browserWSEndpoint })
      isConnected = true
    } else {
      // Launch mode: use puppeteer-extra with stealth/adblocker plugins
      const plugins: PluginConfig = {
        stealth: config.stealth !== false,
        adblocker: config.adblocker === true
      }
      const puppeteer = await getPuppeteer(config.scriptPath, plugins)
      const launchOptions = buildPuppeteerLaunchOptions(config.puppeteerOptions, config.proxy)
      browser = await puppeteer.launch(launchOptions)
      reportBrowser('launched', browser)
    }
    if (browser) {
      browserContext = await browser.createBrowserContext()
      page = await browserContext.newPage()
    }

    // Apply proxy authentication if needed (per CONTRACT_PROXY.md)
    if (page && config.proxy?.username && config.proxy?.password) {
      await page.authenticate({
        username: config.proxy.username,
        password: config.proxy.password
      })
    }

    // 4. Create context (single instance, reused throughout lifecycle)
    ctx = createContext<Job>({
      job: effectiveJob,
      run: config.run,
      page: page ?? undefined,
      browser: browser ?? undefined,
      browserContext: browserContext ?? undefined,
      noBrowser: config.noBrowser === true,
      sink,
      storagePartition: config.storagePartition
    })

    // 5. Execute with lifecycle hooks
    let rawScriptError: unknown = null
    try {
      // beforeRun hook
      if (script.hooks.beforeRun) {
        await script.hooks.beforeRun(ctx)
      }

      // Main script
      await script.script(ctx)

      // afterRun hook (only on success path)
      if (script.hooks.afterRun) {
        await script.hooks.afterRun(ctx)
      }
    } catch (err) {
      // Script threw - capture for outcome determination
      scriptThrew = true
      rawScriptError = err
      scriptError = {
        message: errorMessage(err),
        stack: err instanceof Error ? err.stack : undefined
      }

      // onError hook - only call if no terminal event was emitted.
      // If a terminal event exists, the outcome is already determined by that event,
      // and calling onError could cause confusing behavior (e.g., trying to emit
      // another terminal event, or performing recovery that's no longer meaningful).
      if (script.hooks.onError && sink.getTerminalState() === null) {
        try {
          await script.hooks.onError(err, ctx)
        } catch {
          // Swallow hook errors to avoid masking original
        }
      }
    }

    // 6. beforeTerminal hook (if executor owns terminal & sink healthy)
    if (script.hooks.beforeTerminal && !sink.isSinkFailed() && sink.getTerminalState() === null) {
      const signal: TerminalSignal =
        rawScriptError !== null
          ? { outcome: 'error', error: rawScriptError }
          : { outcome: 'completed' }
      try {
        await script.hooks.beforeTerminal(signal, ctx)
      } catch {
        // Swallow — consistent with onError/cleanup pattern
      }
    }

    // 7. Auto-emit terminal if needed and sink is healthy
    if (!sink.isSinkFailed() && sink.getTerminalState() === null) {
      try {
        if (scriptThrew && scriptError) {
          await ctx.emit.runError({
            error_type: 'script_error',
            message: scriptError.message,
            stack: scriptError.stack
          })
        } else {
          await ctx.emit.runComplete()
        }
      } catch (err) {
        // If this is a TerminalEventError, script already emitted terminal
        // If this is a sink failure, determineOutcome will handle it
        if (isSinkFailure(err)) {
          // Sink failed during auto-emit - will be handled by determineOutcome
        }
        // TerminalEventError means script already emitted - that's fine
      }
    }

    // 8. Cleanup hook (runs after terminal emission attempt; skipped on prepare-skip)
    // NOTE: cleanup MUST NOT emit events; they will throw TerminalEventError
    if (script.hooks.cleanup && ctx) {
      try {
        await script.hooks.cleanup(ctx)
      } catch {
        // Swallow cleanup errors
      }
    }

    // 9. Determine final outcome
    const result = determineOutcome(sink)

    // 10. Emit run_result control frame per CONTRACT_IPC.md
    // This is emitted exactly once, after terminal event emission attempt
    await emitRunResult(stdioSink, result.outcome, config.proxy)

    return result
  } catch (err) {
    // Executor-level crash (Puppeteer launch failure, etc.)
    const message = errorMessage(err)
    const crashOutcome: ExecutionOutcome = { status: 'crash', message }

    // Emit run_result even for executor-level crashes if possible
    await emitRunResult(stdioSink, crashOutcome, config.proxy)

    return {
      outcome: crashOutcome,
      terminalEmitted: false
    }
  } finally {
    await safeClose(page)
    await safeClose(browserContext)
    if (isConnected && browser) {
      // In connect mode, disconnect without closing the shared browser
      try {
        await browser.disconnect()
      } catch {
        // Ignore disconnect errors
      }
    } else if (browser) {
      await safeClose(browser)
      reportBrowser('closed', browser)
    }
  }
}
