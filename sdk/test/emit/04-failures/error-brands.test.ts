/**
 * The executor bundles its own copy of the SDK, so the errors a script may
 * catch must match `instanceof` across copies of the module.
 */
import { describe, expect, it, vi } from 'vitest'
import { ArtifactRangeRefusedError, ArtifactWriterError } from '../../../src/emit'

/** Load a second, separate copy of the emit module, as the executor bundle holds. */
async function otherCopy(): Promise<typeof import('../../../src/emit')> {
  vi.resetModules()
  return import('../../../src/emit')
}

describe('catchable artifact errors across SDK copies', () => {
  it('matches ArtifactRangeRefusedError constructed by another copy', async () => {
    const other = await otherCopy()
    expect(other.ArtifactRangeRefusedError).not.toBe(ArtifactRangeRefusedError)

    const err = new other.ArtifactRangeRefusedError('refused', 8)
    expect(err).toBeInstanceOf(ArtifactRangeRefusedError)
    expect(err).not.toBeInstanceOf(ArtifactWriterError)
  })

  it('matches ArtifactWriterError constructed by another copy', async () => {
    const other = await otherCopy()
    expect(other.ArtifactWriterError).not.toBe(ArtifactWriterError)

    const err = new other.ArtifactWriterError('refused')
    expect(err).toBeInstanceOf(ArtifactWriterError)
    expect(err).not.toBeInstanceOf(ArtifactRangeRefusedError)
  })

  it('matches no other error', () => {
    expect(new Error('x')).not.toBeInstanceOf(ArtifactRangeRefusedError)
    expect(new Error('x')).not.toBeInstanceOf(ArtifactWriterError)
    expect(null).not.toBeInstanceOf(ArtifactWriterError)
  })
})
