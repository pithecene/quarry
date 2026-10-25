/**
 * The JSR build turns each bare npm import into a dependency of the npm
 * package it generates. Without a jsr.json import map, the range comes from
 * package.json's devDependencies, so the published SDK would require the
 * development Puppeteer version instead of the supported peer range.
 */
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

type Manifest = Record<string, Record<string, string> | undefined>

function readManifest(file: string): Manifest {
  return JSON.parse(readFileSync(join(import.meta.dirname, '..', file), 'utf8')) as Manifest
}

describe('jsr.json import map', () => {
  it('maps puppeteer to the package.json peer range', () => {
    const peer = readManifest('package.json').peerDependencies?.puppeteer
    const mapped = readManifest('jsr.json').imports?.puppeteer
    expect(peer).toBeDefined()
    expect(mapped).toBe(`npm:puppeteer@${peer}`)
  })
})
