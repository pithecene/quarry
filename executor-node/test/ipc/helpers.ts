/**
 * Helpers that the IPC tests share.
 */
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { decode as msgpackDecode } from '@msgpack/msgpack'
import type { EventEnvelope, EventId, RunId } from '@pithecene/quarry-sdk'
import { expect } from 'vitest'
import { LENGTH_PREFIX_SIZE } from '../../src/ipc/frame.js'
import { runProcess } from '../run-process.js'

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

/**
 * Run a fixture that must exit cleanly, and decode the frames of its stdout.
 */
export async function runFrames(
  name: string
): Promise<{ frames: Record<string, unknown>[]; stderr: string }> {
  const { stdout, stderr, exitCode } = await runProcess(tsxBin, [
    resolve(testDir, 'fixtures', name)
  ])
  expect(exitCode, `child stderr: ${stderr}`).toBe(0)
  return { frames: decodeFrames(stdout) as Record<string, unknown>[], stderr }
}
