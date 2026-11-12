import { createHash, randomBytes } from 'node:crypto'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import { afterEach, describe, expect, it } from 'vitest'
import type { ArtifactId, ArtifactWriter, EmitArtifactWriterOptions } from '../src/index'
import { RetainHttpObjectError, retainHttpObject } from '../src/index'

type Answer = (req: IncomingMessage, res: ServerResponse, body: Buffer) => void

/** Answers HEAD with the length and a Range request with exactly its range. */
const ranged: Answer = (req, res, body) => {
  if (!res.hasHeader('ETag')) res.setHeader('ETag', '"v1"')
  res.setHeader('Content-Type', 'application/zip')
  if (req.method === 'HEAD') {
    res.setHeader('Content-Length', body.length)
    res.end()
    return
  }
  const match = req.headers.range?.match(/^bytes=(\d+)-(\d+)$/)
  if (!match) {
    res.end(body)
    return
  }
  const start = Number(match[1])
  const end = Number(match[2])
  res.statusCode = 206
  res.setHeader('Content-Range', `bytes ${start}-${end}/${body.length}`)
  res.end(body.subarray(start, end + 1))
}

let server: Server | null = null

async function serve(body: Buffer, answer: Answer): Promise<string> {
  server = createServer((req, res) => answer(req, res, body))
  await new Promise<void>((resolve) => server?.listen(0, '127.0.0.1', resolve))
  const { port } = server.address() as AddressInfo
  return `http://127.0.0.1:${port}/object.zip`
}

afterEach(async () => {
  await new Promise<void>((resolve) => (server ? server.close(() => resolve()) : resolve()))
  server = null
})

/** An emit whose artifact writer assembles the ranges it receives. */
function recordingEmit() {
  const state: {
    options: EmitArtifactWriterOptions | null
    bytes: Buffer | null
    writes: number
    committed: boolean
  } = { options: null, bytes: null, writes: 0, committed: false }
  const emit = {
    artifactWriter: async (options: EmitArtifactWriterOptions): Promise<ArtifactWriter> => {
      state.options = options
      state.bytes = Buffer.alloc(options.size_bytes)
      return {
        artifact_id: 'artifact-1' as ArtifactId,
        writeAt: async (offset: number, data: Buffer | Uint8Array) => {
          state.writes++
          state.bytes?.set(data, offset)
        },
        commit: async () => {
          state.committed = true
          return 'artifact-1' as ArtifactId
        }
      }
    }
  }
  return { ctx: { emit }, state }
}

describe('retainHttpObject', () => {
  it('retains an object across several ranges and reads back byte for byte', async () => {
    const body = randomBytes(10_000)
    const url = await serve(body, ranged)
    const { ctx, state } = recordingEmit()

    const retained = await retainHttpObject(ctx, url, { name: 'object.zip', chunk_bytes: 3000 })

    expect(state.committed).toBe(true)
    expect(state.writes).toBe(4)
    expect(state.bytes?.equals(body)).toBe(true)
    expect(state.options?.content_type).toBe('application/zip')
    expect(retained.size_bytes).toBe(10_000)
    expect(retained.sha256).toBe(createHash('sha256').update(body).digest('hex'))
    expect(retained.chunks.map((c) => c.range)).toEqual([
      'bytes=0-2999',
      'bytes=3000-5999',
      'bytes=6000-8999',
      'bytes=9000-9999'
    ])
  })

  it('refuses an object whose server ignores Range, and commits nothing', async () => {
    const url = await serve(randomBytes(5000), (req, res, body) => {
      if (req.method === 'HEAD') {
        res.setHeader('Content-Length', body.length)
        res.end()
        return
      }
      res.end(body)
    })
    const { ctx, state } = recordingEmit()

    const refusal = retainHttpObject(ctx, url, { name: 'object.zip', chunk_bytes: 2000 })

    await expect(refusal).rejects.toBeInstanceOf(RetainHttpObjectError)
    const error = (await refusal.catch((e: unknown) => e)) as RetainHttpObjectError
    expect(error.reasons[0]).toContain('chunk 0: status 200, not 206')
    expect(state.committed).toBe(false)
  })

  it('refuses a range other than the one requested', async () => {
    const url = await serve(randomBytes(5000), (req, res, body) => {
      if (req.method === 'HEAD') {
        res.setHeader('Content-Length', body.length)
        res.end()
        return
      }
      res.statusCode = 206
      res.setHeader('Content-Range', `bytes 0-999/${body.length}`)
      res.end(body.subarray(0, 1000))
    })
    const { ctx, state } = recordingEmit()

    const error = (await retainHttpObject(ctx, url, { name: 'o', chunk_bytes: 2000 }).catch(
      (e: unknown) => e
    )) as RetainHttpObjectError

    expect(error).toBeInstanceOf(RetainHttpObjectError)
    expect(error.reasons).toEqual([
      'chunk 0: Content-Range bytes 0-999/5000 does not answer bytes=0-1999',
      'chunk 0: 1000 bytes received of 2000'
    ])
    expect(state.committed).toBe(false)
  })

  it('refuses a total that changes between chunks', async () => {
    let gets = 0
    const url = await serve(randomBytes(4000), (req, res, body) => {
      if (req.method === 'HEAD') {
        res.setHeader('Content-Length', body.length)
        res.end()
        return
      }
      const [, s, e] = req.headers.range?.match(/^bytes=(\d+)-(\d+)$/) ?? []
      const start = Number(s)
      const end = Number(e)
      const total = gets++ === 0 ? body.length : body.length + 1
      res.statusCode = 206
      res.setHeader('Content-Range', `bytes ${start}-${end}/${total}`)
      res.end(body.subarray(start, end + 1))
    })
    const { ctx, state } = recordingEmit()

    const error = (await retainHttpObject(ctx, url, { name: 'o', chunk_bytes: 2000 }).catch(
      (e: unknown) => e
    )) as RetainHttpObjectError

    expect(error.reasons).toEqual(['chunk 1: Content-Range total 4001, the HEAD length is 4000'])
    expect(state.committed).toBe(false)
  })

  it('refuses a chunk whose ETag differs from the HEAD answer', async () => {
    const url = await serve(randomBytes(3000), (req, res, body) => {
      if (req.method !== 'HEAD') res.setHeader('ETag', '"v2"')
      ranged(req, res, body)
    })
    const { ctx } = recordingEmit()

    const error = (await retainHttpObject(ctx, url, { name: 'o', chunk_bytes: 2000 }).catch(
      (e: unknown) => e
    )) as RetainHttpObjectError

    expect(error.reasons).toContain('chunk 0: ETag "v2", the HEAD ETag is "v1"')
  })

  it('rejects a chunk size above one range frame', async () => {
    const { ctx } = recordingEmit()
    await expect(
      retainHttpObject(ctx, 'http://127.0.0.1:1/x', { name: 'o', chunk_bytes: 8 * 1024 * 1024 + 1 })
    ).rejects.toBeInstanceOf(RangeError)
  })
})
