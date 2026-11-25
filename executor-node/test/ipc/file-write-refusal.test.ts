import { PassThrough, Writable } from 'node:stream'
import { encode as msgpackEncode } from '@msgpack/msgpack'
import { StorageWriteRefusedError } from '@pithecene/quarry-sdk'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { AckReader } from '../../src/ipc/ack-reader.js'
import { encodeFrame } from '../../src/ipc/frame.js'
import { StdioSink } from '../../src/ipc/sink.js'

describe('refused file writes (CONTRACT_IPC.md §File Write Acknowledgement)', () => {
  let acks: PassThrough
  let reader: AckReader
  const send = (fields: object) =>
    acks.write(encodeFrame(msgpackEncode({ type: 'file_write_ack', write_id: 1, ...fields })))

  beforeEach(() => {
    acks = new PassThrough()
    reader = new AckReader(acks)
    reader.start()
  })
  afterEach(() => reader.stop())

  it('rejects with StorageWriteRefusedError', async () => {
    const pending = reader.waitForAck(1).catch((err: unknown) => err)
    send({ ok: false, error: 'S3 PutObject failed' })
    const rejection = await pending
    expect(rejection).toBeInstanceOf(StorageWriteRefusedError)
    expect(rejection).toMatchObject({ message: 'S3 PutObject failed' })
  })

  it('takes no ack without a boolean ok for a refusal', async () => {
    const pending = reader.waitForAck(1).catch((err: unknown) => err)
    send({ error: 'disk full' })
    const rejection = await pending
    expect(rejection).toBeInstanceOf(Error)
    expect(rejection).not.toBeInstanceOf(StorageWriteRefusedError)
  })

  it('leaves no ack behind when the frame cannot be encoded', async () => {
    const sink = new StdioSink(new PassThrough(), undefined, reader)
    const tooLarge = Buffer.alloc(8 * 1024 * 1024 + 1)
    await expect(sink.writeFile('a.txt', 'text/plain', tooLarge)).rejects.toThrow()
    expect(reader.idle).toBe(true)
  })

  it('handles a refusal that arrives before the write settles', async () => {
    const output = new Writable({
      write(_chunk, _encoding, done) {
        send({ ok: false, error: 'refused' })
        setTimeout(done, 20)
      },
      highWaterMark: 1
    })
    const unhandled: unknown[] = []
    const record = (err: unknown) => unhandled.push(err)
    process.on('unhandledRejection', record)
    try {
      const write = new StdioSink(output, undefined, reader).writeFile(
        'a.txt',
        'text/plain',
        Buffer.from('x')
      )
      await expect(write).rejects.toThrow('refused')
      await new Promise((done) => setTimeout(done, 40))
      expect(unhandled).toEqual([])
    } finally {
      process.off('unhandledRejection', record)
    }
  })
})
