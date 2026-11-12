/**
 * Retain a large HTTP object as one artifact, fetched in Range chunks
 * (CONTRACT_EMIT.md §retainHttpObject).
 *
 * The IPC contract carries an artifact's bytes in range frames of at most
 * ARTIFACT_RANGE_MAX_BYTES, so a script that keeps a larger object cuts it
 * into Range requests. This helper performs that transport and its checks.
 * The script keeps its own record of what the transport means for its
 * source.
 */

import { createHash } from 'node:crypto'
import { ARTIFACT_MAX_BYTES, ARTIFACT_RANGE_MAX_BYTES, type EmitAPI } from './emit'
import type { ArtifactId } from './types/events'

const DEFAULT_CONTENT_TYPE = 'application/octet-stream'

/** Options for retainHttpObject. */
export type RetainHttpObjectOptions = {
  /** Human-readable name for the artifact. */
  name: string
  /** MIME type of the artifact. The HEAD answer's Content-Type by default. */
  content_type?: string
  /** Bytes per Range request, at most ARTIFACT_RANGE_MAX_BYTES (the default). */
  chunk_bytes?: number
  /** Headers sent with every request, such as authorization or a user agent. */
  headers?: Record<string, string>
  /** Aborts the transfer. */
  signal?: AbortSignal
  /** The fetch to use. The global fetch by default. */
  fetch?: typeof fetch
}

/** One Range request and its answer. */
export type RetainedChunk = {
  /** The Range header sent, `bytes=start-end`. */
  range: string
  /** The answer's status. */
  status: number
  /** The answer's headers, with lowercase names. */
  headers: Record<string, string>
  /** Hex SHA-256 of the chunk's bytes. */
  sha256: string
}

/** The retained object. */
export type RetainedHttpObject = {
  /** The artifact holding the object's bytes. */
  artifact_id: ArtifactId
  /** The object's length in bytes. */
  size_bytes: number
  /** Hex SHA-256 of the whole object. */
  sha256: string
  /** The HEAD answer's status and headers, with lowercase names. */
  head: { status: number; headers: Record<string, string> }
  /** Each Range request and its answer, in order. */
  chunks: RetainedChunk[]
}

/**
 * Thrown when retainHttpObject refuses an object. The helper then emits no
 * artifact event, so the runtime discards any range already sent.
 */
export class RetainHttpObjectError extends Error {
  /** Each failed check, in chunk order. */
  readonly reasons: readonly string[]

  constructor(url: string, reasons: readonly string[]) {
    super(`retainHttpObject refused ${url}: ${reasons.join('; ')}`)
    this.name = 'RetainHttpObjectError'
    this.reasons = reasons
  }
}

function headersOf(response: Response): Record<string, string> {
  const out: Record<string, string> = {}
  response.headers.forEach((value, key) => {
    out[key.toLowerCase()] = value
  })
  return out
}

/** Parses `bytes start-end/total`. The total is null when it reads `*`. */
function parseContentRange(
  value: string | undefined
): { start: number; end: number; total: number | null } | null {
  const match = value?.match(/^bytes (\d+)-(\d+)\/(\d+|\*)$/)
  if (!match) return null
  return {
    start: Number(match[1]),
    end: Number(match[2]),
    total: match[3] === '*' ? null : Number(match[3])
  }
}

/** The checks one chunk fails, empty when it answers its range whole. */
function chunkFailures(
  index: number,
  start: number,
  end: number,
  size: number,
  etag: string | undefined,
  response: Response,
  headers: Record<string, string>,
  received: number
): string[] {
  const failures: string[] = []
  const asked = `bytes=${start}-${end}`
  if (response.status !== 206) {
    failures.push(`chunk ${index}: status ${response.status}, not 206, for ${asked}`)
  }
  const answered = parseContentRange(headers['content-range'])
  if (answered === null || answered.start !== start || answered.end !== end) {
    failures.push(
      `chunk ${index}: Content-Range ${headers['content-range'] ?? 'absent'} does not answer ${asked}`
    )
  }
  if (answered !== null && answered.total !== size) {
    failures.push(
      `chunk ${index}: Content-Range total ${answered.total ?? '*'}, the HEAD length is ${size}`
    )
  }
  if (received !== end - start + 1) {
    failures.push(`chunk ${index}: ${received} bytes received of ${end - start + 1}`)
  }
  if (etag !== undefined && headers.etag !== etag) {
    failures.push(`chunk ${index}: ETag ${headers.etag ?? 'absent'}, the HEAD ETag is ${etag}`)
  }
  return failures
}

/**
 * Retain the HTTP object at `url` as one artifact.
 *
 * A HEAD request states the object's length. Contiguous Range requests of at
 * most one range frame then cover the object in order. Every answer must be
 * 206, and its Content-Range must be exactly the range requested, with the
 * HEAD length as its total. Its body must hold that many bytes, and its ETag
 * must be the HEAD answer's when the HEAD answer carries one. The helper
 * writes each chunk at its offset, so no more than one chunk sits in memory,
 * and a SHA-256 runs over the whole object.
 *
 * @throws RetainHttpObjectError when the HEAD answer or a chunk fails a
 *   check. The error names each failed check of the first failing chunk.
 */
export async function retainHttpObject(
  ctx: { readonly emit: Pick<EmitAPI, 'artifactWriter'> },
  url: string,
  options: RetainHttpObjectOptions
): Promise<RetainedHttpObject> {
  const doFetch = options.fetch ?? fetch
  const chunkBytes = options.chunk_bytes ?? ARTIFACT_RANGE_MAX_BYTES
  if (
    !Number.isSafeInteger(chunkBytes) ||
    chunkBytes < 1 ||
    chunkBytes > ARTIFACT_RANGE_MAX_BYTES
  ) {
    throw new RangeError(`chunk_bytes must be a whole number from 1 to ${ARTIFACT_RANGE_MAX_BYTES}`)
  }
  const request = { headers: options.headers, signal: options.signal }

  const headResponse = await doFetch(url, { ...request, method: 'HEAD' })
  const head = { status: headResponse.status, headers: headersOf(headResponse) }
  const size = Number(head.headers['content-length'])
  if (!headResponse.ok) {
    throw new RetainHttpObjectError(url, [`HEAD answered ${headResponse.status}`])
  }
  if (!Number.isSafeInteger(size) || size < 0) {
    throw new RetainHttpObjectError(url, [
      `HEAD states no length: Content-Length ${head.headers['content-length'] ?? 'absent'}`
    ])
  }
  if (size > ARTIFACT_MAX_BYTES) {
    throw new RetainHttpObjectError(url, [
      `the object is ${size} bytes, above the artifact maximum of ${ARTIFACT_MAX_BYTES}`
    ])
  }
  const etag = head.headers.etag

  const writer = await ctx.emit.artifactWriter({
    name: options.name,
    content_type: options.content_type ?? head.headers['content-type'] ?? DEFAULT_CONTENT_TYPE,
    size_bytes: size
  })
  const digest = createHash('sha256')
  const chunks: RetainedChunk[] = []
  for (let start = 0, index = 0; start < size; start += chunkBytes, index++) {
    const end = Math.min(start + chunkBytes, size) - 1
    const range = `bytes=${start}-${end}`
    const response = await doFetch(url, {
      ...request,
      method: 'GET',
      headers: { ...options.headers, Range: range }
    })
    const headers = headersOf(response)
    const body = new Uint8Array(await response.arrayBuffer())
    const failures = chunkFailures(
      index,
      start,
      end,
      size,
      etag,
      response,
      headers,
      body.byteLength
    )
    if (failures.length > 0) {
      throw new RetainHttpObjectError(url, failures)
    }
    digest.update(body)
    await writer.writeAt(start, body)
    chunks.push({
      range,
      status: response.status,
      headers,
      sha256: createHash('sha256').update(body).digest('hex')
    })
  }

  const artifact_id = await writer.commit()
  return { artifact_id, size_bytes: size, sha256: digest.digest('hex'), head, chunks }
}
