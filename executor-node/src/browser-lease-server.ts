import { once } from 'node:events'
import { createServer, type Socket } from 'node:net'

/** What a run sends when it connects: the hash of its proxy configuration. */
export type LeaseRequest = { proxy_hash: string }

/** The server's one reply: the browser's endpoint, or why the run cannot share it. */
export type LeaseReply = { ws_endpoint: string; pid: number } | { refused: string }

export type LeaseServerOptions = {
  socketPath: string
  wsEndpoint: string
  proxyHash: string
  idleTimeoutMs: number
}

/** A listening lease server. */
export type LeaseServer = {
  /** Resolves once the server has had no connection for the idle timeout and has stopped listening. */
  readonly idle: Promise<void>
  /** Stop listening now, removing the socket file. Open connections close on their own. */
  close(): void
}

/** A request longer than this is malformed: a run sends one short line. */
const MAX_REQUEST_BYTES = 4096

/**
 * Serve leases on the browser at `wsEndpoint` through a Unix socket.
 *
 * Each open connection is a reference: a run connects, sends a
 * `LeaseRequest`, receives a `LeaseReply`, and keeps the connection open
 * until it ends. The operating system closes the connection when the run's
 * process ends, however it ends, so the count of open connections is exact.
 *
 * The idle timer runs only while no connection is open. Connections and the
 * timer share one event loop, so the server stops listening only at a moment
 * when no connection is open, and a run that connects later finds no server.
 */
export async function serveLeases(options: LeaseServerOptions): Promise<LeaseServer> {
  const connections = new Set<Socket>()
  let timer: NodeJS.Timeout | undefined
  let becameIdle: () => void = () => undefined
  const idle = new Promise<void>((resolve) => {
    becameIdle = resolve
  })

  const server = createServer((socket) => {
    connections.add(socket)
    clearTimeout(timer)
    socket.on('error', () => {
      // The run ended mid-exchange; 'close' follows and releases its reference
    })
    socket.on('close', () => {
      connections.delete(socket)
      if (connections.size === 0) arm()
    })
    void answer(socket, options)
  })

  // Node removes the socket file before it closes the descriptor, so this
  // server never removes the file of a server started after it.
  const close = (): void => {
    clearTimeout(timer)
    if (server.listening) server.close()
  }

  function arm(): void {
    timer = setTimeout(() => {
      close()
      becameIdle()
    }, options.idleTimeoutMs)
  }

  server.listen(options.socketPath)
  await once(server, 'listening')
  arm()
  return { idle, close }
}

/** Read the run's request and send the one reply. A granted connection stays open. */
async function answer(socket: Socket, options: LeaseServerOptions): Promise<void> {
  const reply = replyTo(await firstLine(socket), options)
  socket.write(`${JSON.stringify(reply)}\n`)
  if ('refused' in reply) socket.end()
}

function replyTo(line: string | undefined, options: LeaseServerOptions): LeaseReply {
  const request = parseRequest(line)
  if (request === undefined) return { refused: 'malformed request' }
  if (request.proxy_hash !== options.proxyHash) return { refused: 'proxy mismatch' }
  return { ws_endpoint: options.wsEndpoint, pid: process.pid }
}

/**
 * Resolve with the first line the run sends, or undefined if it sends none.
 * The socket keeps flowing afterwards, so its end is seen when the run's
 * process closes it.
 */
function firstLine(socket: Socket): Promise<string | undefined> {
  return new Promise((resolve) => {
    let buffered = ''
    const finish = (line: string | undefined): void => {
      socket.off('data', onData)
      socket.off('end', onEnd)
      socket.resume()
      resolve(line)
    }
    const onData = (chunk: Buffer): void => {
      buffered += chunk.toString('utf8')
      const end = buffered.indexOf('\n')
      if (end !== -1) finish(buffered.slice(0, end))
      else if (buffered.length > MAX_REQUEST_BYTES) finish(undefined)
    }
    const onEnd = (): void => finish(undefined)
    socket.on('data', onData)
    socket.on('end', onEnd)
  })
}

function parseRequest(line: string | undefined): LeaseRequest | undefined {
  if (line === undefined) return undefined
  try {
    const hash = (JSON.parse(line) as { proxy_hash?: unknown } | null)?.proxy_hash
    return typeof hash === 'string' ? { proxy_hash: hash } : undefined
  } catch {
    return undefined
  }
}
