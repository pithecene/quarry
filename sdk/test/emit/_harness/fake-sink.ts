/**
 * Fake EmitSink for testing.
 *
 * Capabilities:
 * - Record writeEvent calls (with full envelope)
 * - Record openArtifact and writeArtifactRange calls
 * - Inject failures on Nth event write or artifact range write
 * - Optional artificial delays
 * - Expose call order + timestamps
 *
 * No assertions. No test logic.
 */
import type { EmitSink } from '../../../src/emit'
import type { ArtifactId, EventEnvelope } from '../../../src/types/events'

export interface WriteEventCall {
  readonly kind: 'writeEvent'
  readonly envelope: EventEnvelope
  readonly timestamp: number
  readonly callIndex: number
}

export interface OpenArtifactCall {
  readonly kind: 'openArtifact'
  readonly artifactId: ArtifactId
  readonly sizeBytes: number
  readonly expectedSha256: string | undefined
  readonly timestamp: number
  readonly callIndex: number
}

export interface WriteArtifactRangeCall {
  readonly kind: 'writeArtifactRange'
  readonly artifactId: ArtifactId
  readonly offset: number
  readonly data: Buffer | Uint8Array
  readonly timestamp: number
  readonly callIndex: number
}

export interface WriteFileCall {
  readonly kind: 'writeFile'
  readonly filename: string
  readonly contentType: string
  readonly data: Buffer | Uint8Array
  readonly timestamp: number
  readonly callIndex: number
}

export type SinkCall = WriteEventCall | OpenArtifactCall | WriteArtifactRangeCall | WriteFileCall

export interface FakeSinkOptions {
  /**
   * Fail writeEvent on the Nth call (1-indexed).
   * Set to 0 or undefined to never fail.
   */
  failOnEventWrite?: number

  /**
   * Fail writeArtifactRange on the Nth call (1-indexed).
   * Set to 0 or undefined to never fail.
   */
  failOnArtifactWrite?: number

  /**
   * Error to throw on failure.
   */
  failureError?: Error

  /**
   * Artificial delay in ms for writeEvent calls.
   */
  eventWriteDelayMs?: number

  /**
   * Artificial delay in ms for writeArtifactRange calls.
   */
  artifactWriteDelayMs?: number
}

export class FakeSink implements EmitSink {
  private readonly calls: SinkCall[] = []
  private eventWriteCount = 0
  private artifactWriteCount = 0
  private callIndex = 0

  constructor(private readonly options: FakeSinkOptions = {}) {}

  /**
   * All recorded calls in order.
   */
  get allCalls(): readonly SinkCall[] {
    return this.calls
  }

  /**
   * All recorded writeEvent calls in order.
   */
  get eventCalls(): readonly WriteEventCall[] {
    return this.calls.filter((c): c is WriteEventCall => c.kind === 'writeEvent')
  }

  /**
   * All recorded openArtifact calls in order.
   */
  get artifactOpenCalls(): readonly OpenArtifactCall[] {
    return this.calls.filter((c): c is OpenArtifactCall => c.kind === 'openArtifact')
  }

  /**
   * All recorded writeArtifactRange calls in order.
   */
  get artifactRangeCalls(): readonly WriteArtifactRangeCall[] {
    return this.calls.filter((c): c is WriteArtifactRangeCall => c.kind === 'writeArtifactRange')
  }

  /**
   * All recorded envelopes in order.
   */
  get envelopes(): readonly EventEnvelope[] {
    return this.eventCalls.map((c) => c.envelope)
  }

  /**
   * Reset all recorded calls and counters.
   */
  reset(): void {
    this.calls.length = 0
    this.eventWriteCount = 0
    this.artifactWriteCount = 0
    this.callIndex = 0
  }

  async writeEvent(envelope: EventEnvelope): Promise<void> {
    this.eventWriteCount++
    const index = this.callIndex++

    if (this.options.eventWriteDelayMs) {
      await new Promise((resolve) => setTimeout(resolve, this.options.eventWriteDelayMs))
    }

    if (this.options.failOnEventWrite && this.eventWriteCount === this.options.failOnEventWrite) {
      throw this.options.failureError ?? new Error('Injected writeEvent failure')
    }

    this.calls.push({
      kind: 'writeEvent',
      envelope,
      timestamp: Date.now(),
      callIndex: index
    })
  }

  async openArtifact(
    artifactId: ArtifactId,
    sizeBytes: number,
    expectedSha256?: string
  ): Promise<void> {
    this.calls.push({
      kind: 'openArtifact',
      artifactId,
      sizeBytes,
      expectedSha256,
      timestamp: Date.now(),
      callIndex: this.callIndex++
    })
  }

  async writeArtifactRange(
    artifactId: ArtifactId,
    offset: number,
    data: Buffer | Uint8Array
  ): Promise<void> {
    this.artifactWriteCount++
    const index = this.callIndex++

    if (this.options.artifactWriteDelayMs) {
      await new Promise((resolve) => setTimeout(resolve, this.options.artifactWriteDelayMs))
    }

    if (
      this.options.failOnArtifactWrite &&
      this.artifactWriteCount === this.options.failOnArtifactWrite
    ) {
      throw this.options.failureError ?? new Error('Injected writeArtifactRange failure')
    }

    this.calls.push({
      kind: 'writeArtifactRange',
      artifactId,
      offset,
      data,
      timestamp: Date.now(),
      callIndex: index
    })
  }

  async writeFile(filename: string, contentType: string, data: Buffer | Uint8Array): Promise<void> {
    const index = this.callIndex++

    this.calls.push({
      kind: 'writeFile',
      filename,
      contentType,
      data,
      timestamp: Date.now(),
      callIndex: index
    })
  }
}
