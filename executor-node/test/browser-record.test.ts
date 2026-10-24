import { mkdtempSync, readdirSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { closeRecordedBrowser, recordBrowser } from '../src/browser-record.js'

const WS = 'ws://127.0.0.1:9222/devtools/browser/abc'

function recordPath(): string {
  return join(mkdtempSync(join(tmpdir(), 'quarry-record-')), 'browser.json')
}

function fakeConnect(close = vi.fn(async () => undefined)) {
  return { connect: vi.fn(async () => ({ close })), close }
}

describe('closeRecordedBrowser', () => {
  it('closes the browser an earlier server recorded', async () => {
    const path = recordPath()
    recordBrowser(path, WS)
    const { connect, close } = fakeConnect()
    const log = vi.fn()

    await closeRecordedBrowser(path, connect, log)

    expect(connect).toHaveBeenCalledWith({ browserWSEndpoint: WS })
    expect(close).toHaveBeenCalledOnce()
    expect(log).toHaveBeenCalledWith(expect.stringContaining(WS))
  })

  it('does nothing when no browser is recorded', async () => {
    const { connect } = fakeConnect()
    const log = vi.fn()

    await closeRecordedBrowser(recordPath(), connect, log)

    expect(connect).not.toHaveBeenCalled()
    expect(log).not.toHaveBeenCalled()
  })

  it('leaves a malformed record alone and says so', async () => {
    const path = recordPath()
    writeFileSync(path, 'not json')
    const { connect } = fakeConnect()
    const log = vi.fn()

    await closeRecordedBrowser(path, connect, log)

    expect(connect).not.toHaveBeenCalled()
    expect(log).toHaveBeenCalledWith(expect.stringContaining('malformed'))
  })

  it('moves on when the recorded browser is already gone', async () => {
    const path = recordPath()
    recordBrowser(path, WS)
    const connect = vi.fn(async () => {
      throw new Error('ECONNREFUSED')
    })

    await expect(closeRecordedBrowser(path, connect, vi.fn())).resolves.toBeUndefined()
  })
})

describe('recordBrowser', () => {
  it('replaces the record without leaving a temporary file', () => {
    const path = recordPath()
    recordBrowser(path, 'ws://127.0.0.1:1/devtools/browser/old')
    recordBrowser(path, WS)

    expect(readdirSync(join(path, '..'))).toEqual(['browser.json'])
  })
})
