/**
 * Helpers that the IPC tests share.
 */
import { type ChildProcess, spawn } from 'node:child_process'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { decode as msgpackDecode } from '@msgpack/msgpack'
import type { EventEnvelope, EventId, RunId } from '@pithecene/quarry-sdk'
import { LENGTH_PREFIX_SIZE } from '../../src/ipc/frame.js'

const testDir = dirname(fileURLToPath(import.meta.url))
const tsxBin = resolve(testDir, '../../../node_modules/.bin/tsx')

/**
 * Decode all length-prefixed msgpack frames from a buffer.
 */
export function decodeFrames(data: Buffer): unknown[] {
  const frames: unknown[] = []
  let offset = 0
  while (offset + LENGTH_PREFIX_SIZE <= data.length) {
    const payloadLen = data.readUInt32BE(offset)
    offset += LENGTH_PREFIX_SIZE
    frames.push(msgpackDecode(data.subarray(offset, offset + payloadLen)))
    offset += payloadLen
  }
  return frames
}

/**
 * Run a child process fixture with tsx and collect its output and exit code.
 */
export function runFixture(
  name: string
): Promise<{ stdout: Buffer; stderr: string; exitCode: number | null }> {
  const fixture = resolve(testDir, 'fixtures', name)
  return new Promise((resolve, reject) => {
    let child: ChildProcess
    try {
      child = spawn(tsxBin, [fixture], {
        stdio: ['ignore', 'pipe', 'pipe']
      })
    } catch (err) {
      reject(err)
      return
    }

    const stdoutChunks: Buffer[] = []
    const stderrChunks: Buffer[] = []
    child.stdout!.on('data', (chunk: Buffer) => stdoutChunks.push(chunk))
    child.stderr!.on('data', (chunk: Buffer) => stderrChunks.push(chunk))
    child.on('error', reject)
    child.on('close', (code) => {
      resolve({
        stdout: Buffer.concat(stdoutChunks),
        stderr: Buffer.concat(stderrChunks).toString('utf-8'),
        exitCode: code
      })
    })
  })
}

/**
 * Helper to create a minimal valid event envelope for testing.
 */
export function makeEnvelope(
  overrides: Partial<EventEnvelope<'item'>> = {}
): EventEnvelope<'item'> {
  return {
    contract_version: '0.1.0',
    event_id: 'evt-123' as EventId,
    run_id: 'run-456' as RunId,
    seq: 1,
    type: 'item',
    ts: '2024-01-01T00:00:00.000Z',
    payload: { item_type: 'test', data: {} },
    attempt: 1,
    ...overrides
  }
}
