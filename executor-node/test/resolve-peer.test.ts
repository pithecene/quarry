import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { resolvePeer } from '../src/executor.js'

let dir: string

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'quarry-resolve-peer-'))
  mkdirSync(join(dir, 'project', 'scripts'), { recursive: true })
})

afterEach(() => {
  rmSync(dir, { recursive: true, force: true })
})

function install(modules: string): void {
  mkdirSync(join(modules, 'peer-under-test'), { recursive: true })
  writeFileSync(join(modules, 'peer-under-test', 'index.js'), 'module.exports = 1\n')
}

const script = () => join(dir, 'project', 'scripts', 'script.mjs')

describe('resolvePeer', () => {
  it('finds a package in an ancestor of the script', () => {
    install(join(dir, 'project', 'node_modules'))
    expect(resolvePeer('peer-under-test', script())).toContain('peer-under-test')
  })

  it('finds a linked package, as pnpm installs it', () => {
    install(join(dir, 'store'))
    mkdirSync(join(dir, 'project', 'node_modules'))
    symlinkSync(
      join(dir, 'store', 'peer-under-test'),
      join(dir, 'project', 'node_modules', 'peer-under-test')
    )
    expect(realpathSync(resolvePeer('peer-under-test', script()))).toBe(
      realpathSync(join(dir, 'store', 'peer-under-test', 'index.js'))
    )
  })

  it('refuses a package that no searched directory holds', () => {
    install(join(dir, 'elsewhere', 'node_modules'))
    expect(() => resolvePeer('peer-under-test', script())).toThrow(/Cannot find/)
  })
})
