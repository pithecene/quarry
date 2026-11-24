import { spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync } from 'node:fs'
import { connect, type Socket } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterEach, describe, expect, it } from 'vitest'
import {
  isBrowserKey,
  type LeaseReply,
  type LeaseServer,
  serveLeases
} from '../src/browser-lease-server.js'

const WS = 'ws://127.0.0.1:9222/devtools/browser/abc'
const servers: LeaseServer[] = []
const clients: Socket[] = []

afterEach(() => {
  for (const client of clients.splice(0)) client.destroy()
  for (const server of servers.splice(0)) server.close()
})

async function start(idleTimeoutMs: number): Promise<{ server: LeaseServer; socketPath: string }> {
  const socketPath = join(mkdtempSync(join(tmpdir(), 'quarry-lease-')), 'server.sock')
  const server = await serveLeases({ socketPath, wsEndpoint: WS, browserKey: 'h1', idleTimeoutMs })
  servers.push(server)
  return { server, socketPath }
}

/** Connect, send `request`, and resolve with the reply; the connection stays open. */
function lease(
  socketPath: string,
  request: string
): Promise<{ client: Socket; reply: LeaseReply }> {
  return new Promise((resolve, reject) => {
    const client = connect(socketPath, () => client.write(`${request}\n`))
    clients.push(client)
    client.once('error', reject)
    client.once('data', (data) =>
      resolve({ client, reply: JSON.parse(String(data)) as LeaseReply })
    )
  })
}

/** Resolve with whether `promise` settles within `ms`. */
async function settlesWithin(promise: Promise<void>, ms: number): Promise<boolean> {
  const timeout = new Promise<false>((resolve) => setTimeout(() => resolve(false), ms))
  return Promise.race([promise.then(() => true as const), timeout])
}

// Each test runs over a real Unix socket; a deadlock must fail, not hang.
describe('serveLeases', { timeout: 5_000 }, () => {
  it('grants the endpoint to a run with the same launch settings', async () => {
    const { socketPath } = await start(10_000)

    const { reply } = await lease(socketPath, JSON.stringify({ browser_key: 'h1' }))

    expect(reply).toEqual({ ws_endpoint: WS, pid: process.pid })
  })

  it('refuses other launch settings, a malformed request, and an empty key', async () => {
    const { socketPath } = await start(10_000)

    const mismatch = await lease(socketPath, JSON.stringify({ browser_key: 'h2' }))
    const malformed = await lease(socketPath, 'not json')
    const empty = await lease(socketPath, JSON.stringify({ browser_key: '' }))

    expect(mismatch.reply).toEqual({ refused: 'launch mismatch' })
    expect(malformed.reply).toEqual({ refused: 'malformed request' })
    expect(empty.reply).toEqual({ refused: 'malformed request' })
  })

  it('stops listening once idle for the timeout, removing the socket', async () => {
    const { server, socketPath } = await start(50)

    expect(await settlesWithin(server.idle, 2_000)).toBe(true)
    expect(existsSync(socketPath)).toBe(false)
  })

  it('stays up while a run holds its connection, and goes idle after it closes', async () => {
    const { server, socketPath } = await start(100)
    const { client } = await lease(socketPath, JSON.stringify({ browser_key: 'h1' }))

    expect(await settlesWithin(server.idle, 400)).toBe(false)

    client.destroy()
    expect(await settlesWithin(server.idle, 2_000)).toBe(true)
  })

  it('counts a connection that has not yet sent its request', async () => {
    const { server, socketPath } = await start(100)
    const client = connect(socketPath)
    clients.push(client)
    await new Promise((resolve) => client.once('connect', resolve))

    expect(await settlesWithin(server.idle, 400)).toBe(false)
  })
})

// The browser server starts only with the key of its launch settings: an
// absent, empty, or malformed key could match no run correctly.
describe('isBrowserKey', () => {
  it.each([
    { key: `sha256:${'a'.repeat(64)}`, valid: true },
    { key: '', valid: false },
    { key: 'h1', valid: false },
    { key: `sha256:${'a'.repeat(63)}`, valid: false },
    { key: `sha256:${'A'.repeat(64)}`, valid: false }
  ])('accepts $key: $valid', ({ key, valid }) => {
    expect(isBrowserKey(key)).toBe(valid)
  })
})

// The built entrypoint refuses to start a browser server without a valid key,
// before it loads Puppeteer or opens a socket.
describe('--browser-server', () => {
  const entrypoint = fileURLToPath(new URL('../dist/bin/executor.js', import.meta.url))
  it.each(['', 'h1', `sha256:${'a'.repeat(63)}`])('exits 3 for the key %j', (key) => {
    const reuseDir = mkdtempSync(join(tmpdir(), 'quarry-key-'))
    const result = spawnSync(process.execPath, [entrypoint, '--browser-server', 'script.ts'], {
      env: { ...process.env, QUARRY_BROWSER_KEY: key, QUARRY_BROWSER_REUSE_DIR: reuseDir },
      encoding: 'utf8',
      timeout: 10_000
    })
    expect(result.status).toBe(3)
    expect(result.stderr).toContain('QUARRY_BROWSER_KEY')
    expect(existsSync(join(reuseDir, 'server.sock'))).toBe(false)
  })
})
