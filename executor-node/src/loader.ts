/**
 * Script loader for dynamically importing user scripts.
 *
 * Validates that the loaded module conforms to QuarryScriptModule interface.
 *
 * @module
 */

import { isAbsolute, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import type { QuarryScript, QuarryScriptModule } from '@pithecene/quarry-sdk'

/**
 * Error thrown when a script module is invalid.
 */
export class ScriptLoadError extends Error {
  constructor(
    public readonly scriptPath: string,
    public readonly reason: string
  ) {
    super(`Failed to load script "${scriptPath}": ${reason}`)
    this.name = 'ScriptLoadError'
  }
}

/**
 * Validated script module with guaranteed default export.
 */
export type LoadedScript<Job = unknown> = {
  /** The main script function */
  readonly script: QuarryScript<Job>
  /** Optional lifecycle hooks */
  readonly hooks: {
    readonly prepare?: QuarryScriptModule<Job>['prepare']
    readonly beforeRun?: QuarryScriptModule<Job>['beforeRun']
    readonly afterRun?: QuarryScriptModule<Job>['afterRun']
    readonly onError?: QuarryScriptModule<Job>['onError']
    readonly beforeTerminal?: QuarryScriptModule<Job>['beforeTerminal']
    readonly cleanup?: QuarryScriptModule<Job>['cleanup']
  }
  /** Original module for debugging */
  readonly module: QuarryScriptModule<Job>
}

/**
 * Type guard to check if a value is a function.
 */
function isFunction(value: unknown): value is (...args: unknown[]) => unknown {
  return typeof value === 'function'
}

/**
 * Type guard to check if a value is an optional function (undefined or function).
 */
function isOptionalFunction(
  value: unknown
): value is ((...args: unknown[]) => unknown) | undefined {
  return value === undefined || isFunction(value)
}

/**
 * Annotate known import errors with actionable hints.
 *
 * Node ESM requires `with { type: 'json' }` on JSON imports, but many
 * runtimes (Bun, bundlers, tsx) accept bare imports. When this surfaces
 * as a transitive dependency crash, the raw error is opaque.
 *
 * @internal Exported for testing only.
 */
export function annotateImportError(message: string): string {
  if (/needs an import attribute of.*type:\s*["']?json/i.test(message)) {
    return (
      message +
      '\n\nHint: Node ESM requires an import attribute for JSON modules. ' +
      'Change the import to:\n' +
      "  import data from './file.json' with { type: 'json' }\n" +
      'This applies to your script and all of its transitive dependencies.'
    )
  }
  return message
}

/**
 * Load and validate a script module.
 *
 * @param scriptPath - Path to the script (absolute or relative to cwd)
 * @returns Validated script module with hooks
 * @throws ScriptLoadError if the module is invalid or cannot be loaded
 */
export async function loadScript<Job = unknown>(scriptPath: string): Promise<LoadedScript<Job>> {
  // Resolve to absolute path
  const absolutePath = isAbsolute(scriptPath) ? scriptPath : resolve(process.cwd(), scriptPath)

  // Convert to file URL for dynamic import
  const fileUrl = pathToFileURL(absolutePath).href

  let module: unknown
  try {
    module = await import(fileUrl)
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    throw new ScriptLoadError(scriptPath, `import failed: ${annotateImportError(message)}`)
  }

  // Validate module shape
  if (module === null || typeof module !== 'object') {
    throw new ScriptLoadError(scriptPath, 'module is not an object')
  }

  const mod = module as Record<string, unknown>

  // Check for default export
  if (!('default' in mod)) {
    throw new ScriptLoadError(scriptPath, 'missing default export')
  }

  if (!isFunction(mod.default)) {
    throw new ScriptLoadError(scriptPath, 'default export is not a function')
  }

  // Validate optional hooks
  const HOOK_NAMES = [
    'prepare',
    'beforeRun',
    'afterRun',
    'onError',
    'beforeTerminal',
    'cleanup'
  ] as const
  for (const name of HOOK_NAMES) {
    if (!isOptionalFunction(mod[name])) {
      throw new ScriptLoadError(scriptPath, `${name} hook is not a function`)
    }
  }

  // Cast through unknown since we've validated the shape above
  const validatedModule = mod as unknown as QuarryScriptModule<Job>

  return {
    script: validatedModule.default,
    hooks: {
      prepare: validatedModule.prepare,
      beforeRun: validatedModule.beforeRun,
      afterRun: validatedModule.afterRun,
      onError: validatedModule.onError,
      beforeTerminal: validatedModule.beforeTerminal,
      cleanup: validatedModule.cleanup
    },
    module: validatedModule
  }
}
