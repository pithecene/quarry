/**
 * A release step that must stop the workflow. The CLI reports its message
 * as a GitHub Actions error annotation and exits 1.
 */
export class ReleaseError extends Error {
  override readonly name = 'ReleaseError'
}

/**
 * The command was invoked incorrectly. The CLI prints usage and exits 2.
 */
export class UsageError extends Error {
  override readonly name = 'UsageError'
}

/** The message of a caught value, which need not be an `Error`. */
export const describeError = (error: unknown): string =>
  error instanceof Error ? error.message : String(error)
