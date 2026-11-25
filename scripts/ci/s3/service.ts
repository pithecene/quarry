// What the S3 smokes share: a local service's storage, and a bounded Quarry CLI.
// Both read the arguments <quarry binary> <endpoint>, with AWS_* credentials.

import assert from 'node:assert/strict'
import { execFile, spawn } from 'node:child_process'
import { mkdtemp } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { promisify } from 'node:util'
import type { ReaderStorage } from '../../../sdk/src/index.ts'

export const [binary = '', endpoint = ''] = process.argv.slice(2)
// A smoke writes and prunes, so it accepts a local service only.
assert.match(endpoint, /^http:\/\/127\.0\.0\.1:\d+$/, 'expected a loopback endpoint')
export const BUDGET_MS = 300_000
const KILL_GRACE_MS = 30_000
export const signal = AbortSignal.timeout(BUDGET_MS)
export const quarryPath = resolve(binary)
/** The working directory of the CLI, which the smoke removes. */
export const dir = await mkdtemp(join(tmpdir(), 'quarry-s3-'))

/** The reader's storage at prefix of the smoke's bucket. */
export const storageAt = (prefix: string): ReaderStorage => ({
  backend: 's3',
  path: `smoke/${prefix}`,
  region: 'us-east-1',
  endpoint,
  s3PathStyle: true
})

/** The CLI's flags for storageAt(prefix). */
export const flagsAt = (prefix: string): string[] => [
  '--storage-backend=s3',
  `--storage-path=smoke/${prefix}`,
  '--storage-region=us-east-1',
  `--storage-endpoint=${endpoint}`,
  '--storage-s3-path-style'
]

export async function quarry(
  ...args: string[]
): Promise<{ code: number; out: string; err: string }> {
  const child = spawn(quarryPath, args, { cwd: dir, signal })
  // An abort reports an error first. The close follows it, and ends the wait.
  // An aborted CLI gets SIGTERM and time to stop its executor, then SIGKILL.
  let failure: Error | undefined
  let kill: NodeJS.Timeout | undefined
  child.on('error', (error) => {
    failure = error
    kill = setTimeout(() => {
      if (child.kill('SIGKILL')) console.error('quarry did not stop in its grace period: killed')
    }, KILL_GRACE_MS)
  })
  let out = ''
  let err = ''
  child.stdout.on('data', (data) => {
    out += data
  })
  child.stderr.on('data', (data) => {
    err += data
  })
  const code = await new Promise<number>((done) => child.on('close', done))
  clearTimeout(kill)
  if (failure) throw failure
  return { code, out, err }
}

/** Reads one signed S3 URL below the smoke's bucket. */
export async function s3(path: string): Promise<string> {
  const { AWS_ACCESS_KEY_ID: id, AWS_SECRET_ACCESS_KEY: secret } = process.env
  const auth = ['--aws-sigv4', 'aws:amz:us-east-1:s3', '--user', `${id}:${secret}`]
  const url = `${endpoint}/smoke${path}`
  const args = ['--silent', '--show-error', '--fail', ...auth, url]
  return (await promisify(execFile)('curl', args, { signal })).stdout
}

/** Lists every key of the bucket, which must all be below a storage prefix, without it. */
export async function storedKeys(storagePrefix: string): Promise<string[]> {
  const listing = await s3('?list-type=2&max-keys=1000')
  assert.match(listing, /<IsTruncated>false</, 'the smoke holds one page of keys')
  const prefix = `${storagePrefix}/`
  return [...listing.matchAll(/<Key>([^<]*)<\/Key>/g)].map(([, key = '']) => {
    assert.ok(key.startsWith(prefix), `a key outside the storage path: ${key}`)
    return key.slice(prefix.length)
  })
}
