import { mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { createServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  type ConnectBrowser,
  closeRecordedBrowsers,
  type ProbeBrowser,
  probeBrowser,
  recordBrowsers
} from '../src/browser-record.js'

const WS = 'ws://127.0.0.1:9222/devtools/browser/abc'
const OTHER = 'ws://127.0.0.1:9333/devtools/browser/def'

function recordPath(): string {
  return join(mkdtempSync(join(tmpdir(), 'quarry-record-')), 'browser.json')
}

const closes: ConnectBrowser = async () => ({ close: async () => undefined })
const failsToConnect: ConnectBrowser = async () => {
  throw new Error('connect failed')
}
const probeSays =
  (state: Awaited<ReturnType<ProbeBrowser>>): ProbeBrowser =>
  async () =>
    state

describe('closeRecordedBrowsers', () => {
  it('closes each recorded browser and keeps none', async () => {
    const path = recordPath()
    recordBrowsers(path, [WS, OTHER])
    const connect = vi.fn(closes)
    const log = vi.fn()

    const kept = await closeRecordedBrowsers(path, connect, probeSays('running'), log)

    expect(kept).toEqual([])
    expect(connect).toHaveBeenCalledWith({ browserWSEndpoint: WS })
    expect(connect).toHaveBeenCalledWith({ browserWSEndpoint: OTHER })
    expect(log).toHaveBeenCalledWith(expect.stringContaining(WS))
  })

  it('drops a browser that failed to close only when the probe shows it gone', async () => {
    const path = recordPath()
    recordBrowsers(path, [WS])

    expect(await closeRecordedBrowsers(path, failsToConnect, probeSays('gone'), vi.fn())).toEqual(
      []
    )
    expect(
      await closeRecordedBrowsers(path, failsToConnect, probeSays('running'), vi.fn())
    ).toEqual([WS])
    expect(
      await closeRecordedBrowsers(path, failsToConnect, probeSays('unknown'), vi.fn())
    ).toEqual([WS])
  })

  it('does nothing when no browser is recorded', async () => {
    const connect = vi.fn(closes)
    const log = vi.fn()

    expect(await closeRecordedBrowsers(recordPath(), connect, probeSays('running'), log)).toEqual(
      []
    )
    expect(connect).not.toHaveBeenCalled()
    expect(log).not.toHaveBeenCalled()
  })

  it('leaves a malformed record alone and says so', async () => {
    const path = recordPath()
    writeFileSync(path, 'not json')
    const connect = vi.fn(closes)
    const log = vi.fn()

    expect(await closeRecordedBrowsers(path, connect, probeSays('running'), log)).toEqual([])
    expect(connect).not.toHaveBeenCalled()
    expect(log).toHaveBeenCalledWith(expect.stringContaining('malformed'))
  })
})

describe('recordBrowsers', () => {
  it('replaces the record without leaving a temporary file', () => {
    const path = recordPath()
    recordBrowsers(path, [OTHER])
    recordBrowsers(path, [WS])

    expect(JSON.parse(readFileSync(path, 'utf8'))).toEqual({ browsers: [WS] })
    expect(readdirSync(join(path, '..'))).toEqual(['browser.json'])
  })
})

describe('probeBrowser', () => {
  const servers: Server[] = []
  afterEach(() => {
    for (const server of servers.splice(0)) server.close()
  })

  /** Serve `/json/version` with `body(port)` and return the port. */
  async function serveVersion(body: (port: number) => string): Promise<number> {
    const server = createServer((_, res) => res.end(body((server.address() as AddressInfo).port)))
    servers.push(server)
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    return (server.address() as AddressInfo).port
  }
  const endpointOn = (port: number): string => `ws://127.0.0.1:${port}/devtools/browser/abc`

  it('finds the browser running when its port names its endpoint', async () => {
    const port = await serveVersion((p) => JSON.stringify({ webSocketDebuggerUrl: endpointOn(p) }))

    expect(await probeBrowser(endpointOn(port))).toBe('running')
  })

  it('finds it gone when its port answers as another browser or anything else', async () => {
    const other = await serveVersion(() => JSON.stringify({ webSocketDebuggerUrl: OTHER }))
    const html = await serveVersion(() => '<html></html>')

    expect(await probeBrowser(endpointOn(other))).toBe('gone')
    expect(await probeBrowser(endpointOn(html))).toBe('gone')
  })

  it('finds it gone when nothing listens on its port', async () => {
    const port = await serveVersion(() => '')
    await new Promise((resolve) => servers.pop()?.close(resolve))

    expect(await probeBrowser(endpointOn(port))).toBe('gone')
  })
})
