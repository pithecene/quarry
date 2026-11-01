#!/usr/bin/env npx tsx
/**
 * Generates msgpack IPC fixtures for cross-language testing.
 *
 * These fixtures are consumed by Go tests in quarry/ipc/ to validate
 * that the Go decoder correctly handles Node-encoded msgpack frames.
 *
 * Run: npx tsx scripts/generate-ipc-fixtures.ts
 * Output: ../quarry/ipc/testdata/*.bin
 */

import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { encode as msgpackEncode } from '@msgpack/msgpack'

const __dirname = dirname(fileURLToPath(import.meta.url))
const OUTPUT_DIR = join(__dirname, '../../quarry/ipc/testdata')

// Ensure output directory exists
mkdirSync(OUTPUT_DIR, { recursive: true })

/**
 * Encode a payload with 4-byte big-endian length prefix.
 */
function encodeFrame(payload: Uint8Array): Buffer {
  const frame = Buffer.alloc(4 + payload.length)
  frame.writeUInt32BE(payload.length, 0)
  frame.set(payload, 4)
  return frame
}

/**
 * Generate item event fixture.
 */
function generateItemEvent(): Buffer {
  const envelope = {
    contract_version: '0.1.0',
    event_id: 'evt-fixture-001',
    run_id: 'run-fixture-001',
    seq: 1,
    type: 'item',
    ts: '2024-01-15T10:00:00.000Z',
    attempt: 1,
    payload: {
      item_type: 'product',
      data: {
        name: 'Test Product',
        price: 99.99,
        tags: ['electronics', 'sale']
      }
    }
  }
  return encodeFrame(msgpackEncode(envelope))
}

/**
 * Generate log event fixture.
 */
function generateLogEvent(): Buffer {
  const envelope = {
    contract_version: '0.1.0',
    event_id: 'evt-fixture-002',
    run_id: 'run-fixture-001',
    seq: 2,
    type: 'log',
    ts: '2024-01-15T10:00:01.000Z',
    attempt: 1,
    payload: {
      level: 'info',
      message: 'Processing started',
      fields: {
        url: 'https://example.com',
        duration_ms: 150
      }
    }
  }
  return encodeFrame(msgpackEncode(envelope))
}

/**
 * Generate run_complete event fixture.
 */
function generateRunComplete(): Buffer {
  const envelope = {
    contract_version: '0.1.0',
    event_id: 'evt-fixture-003',
    run_id: 'run-fixture-001',
    seq: 3,
    type: 'run_complete',
    ts: '2024-01-15T10:00:02.000Z',
    attempt: 1,
    payload: {
      summary: {
        items_scraped: 42,
        duration_ms: 5000
      }
    }
  }
  return encodeFrame(msgpackEncode(envelope))
}

/**
 * Generate run_error event fixture.
 */
function generateRunError(): Buffer {
  const envelope = {
    contract_version: '0.1.0',
    event_id: 'evt-fixture-004',
    run_id: 'run-fixture-002',
    seq: 1,
    type: 'run_error',
    ts: '2024-01-15T10:00:00.000Z',
    attempt: 1,
    payload: {
      error_type: 'script_error',
      message: 'Failed to load page',
      stack: 'Error: Failed to load page\n    at scrape (script.ts:42:5)'
    }
  }
  return encodeFrame(msgpackEncode(envelope))
}

/**
 * Generate artifact event fixture.
 */
function generateArtifactEvent(): Buffer {
  const envelope = {
    contract_version: '0.1.0',
    event_id: 'evt-fixture-005',
    run_id: 'run-fixture-001',
    seq: 4,
    type: 'artifact',
    ts: '2024-01-15T10:00:03.000Z',
    attempt: 1,
    payload: {
      artifact_id: 'art-fixture-001',
      name: 'screenshot.png',
      content_type: 'image/png',
      size_bytes: 1024
    }
  }
  return encodeFrame(msgpackEncode(envelope))
}

/**
 * Generate artifact open fixture.
 */
function generateArtifactOpen(): Buffer {
  const frame = {
    type: 'artifact_open',
    artifact_id: 'art-fixture-001',
    size_bytes: 25,
    expected_sha256: '0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0'
  }
  return encodeFrame(msgpackEncode(frame))
}

/**
 * Generate artifact range fixture.
 */
function generateArtifactRange(): Buffer {
  const frame = {
    type: 'artifact_range',
    artifact_id: 'art-fixture-001',
    range_id: 1,
    offset: 0,
    data: Buffer.from('fake png data for testing')
  }
  return encodeFrame(msgpackEncode(frame))
}

/**
 * Generate event with optional fields (job_id, parent_run_id).
 */
function generateEventWithLineage(): Buffer {
  const envelope = {
    contract_version: '0.1.0',
    event_id: 'evt-fixture-006',
    run_id: 'run-fixture-003',
    seq: 1,
    type: 'item',
    ts: '2024-01-15T10:00:00.000Z',
    attempt: 2,
    job_id: 'job-fixture-001',
    parent_run_id: 'run-fixture-002',
    payload: {
      item_type: 'retry_item',
      data: { retry: true }
    }
  }
  return encodeFrame(msgpackEncode(envelope))
}

/**
 * Generate a sequence of multiple events (simulating a full run).
 */
function generateEventSequence(): Buffer {
  const events = [
    {
      contract_version: '0.1.0',
      event_id: 'evt-seq-001',
      run_id: 'run-seq-001',
      seq: 1,
      type: 'item',
      ts: '2024-01-15T10:00:00.000Z',
      attempt: 1,
      payload: { item_type: 'product', data: { id: 1 } }
    },
    {
      contract_version: '0.1.0',
      event_id: 'evt-seq-002',
      run_id: 'run-seq-001',
      seq: 2,
      type: 'log',
      ts: '2024-01-15T10:00:01.000Z',
      attempt: 1,
      payload: { level: 'info', message: 'Done' }
    },
    {
      contract_version: '0.1.0',
      event_id: 'evt-seq-003',
      run_id: 'run-seq-001',
      seq: 3,
      type: 'run_complete',
      ts: '2024-01-15T10:00:02.000Z',
      attempt: 1,
      payload: {}
    }
  ]

  const frames = events.map((e) => encodeFrame(msgpackEncode(e)))
  return Buffer.concat(frames)
}

// Generate all fixtures
const fixtures: Record<string, Buffer> = {
  'item_event.bin': generateItemEvent(),
  'log_event.bin': generateLogEvent(),
  'run_complete.bin': generateRunComplete(),
  'run_error.bin': generateRunError(),
  'artifact_event.bin': generateArtifactEvent(),
  'artifact_open.bin': generateArtifactOpen(),
  'artifact_range.bin': generateArtifactRange(),
  'event_with_lineage.bin': generateEventWithLineage(),
  'event_sequence.bin': generateEventSequence()
}

// Write fixtures
for (const [name, data] of Object.entries(fixtures)) {
  const path = join(OUTPUT_DIR, name)
  writeFileSync(path, data)
  console.log(`Generated: ${path} (${data.length} bytes)`)
}

console.log('\nFixtures generated successfully.')
console.log('Run Go tests with: cd quarry && go test ./ipc/...')
