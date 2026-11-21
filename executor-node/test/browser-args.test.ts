import { afterEach, describe, expect, it, vi } from 'vitest'
import { browserProxyArgs, chromiumArgs } from '../src/browser-args.js'

describe('chromiumArgs', () => {
  afterEach(() => {
    delete process.env.QUARRY_NO_SANDBOX
  })

  it('always includes --disable-dev-shm-usage', () => {
    expect(chromiumArgs()).toContain('--disable-dev-shm-usage')
  })

  it('includes --disable-dev-shm-usage first', () => {
    expect(chromiumArgs()[0]).toBe('--disable-dev-shm-usage')
  })

  it('does not include sandbox flags by default', () => {
    const args = chromiumArgs()
    expect(args).not.toContain('--no-sandbox')
    expect(args).not.toContain('--disable-setuid-sandbox')
  })

  it('includes sandbox flags when QUARRY_NO_SANDBOX=1', () => {
    process.env.QUARRY_NO_SANDBOX = '1'
    const args = chromiumArgs()
    expect(args).toContain('--no-sandbox')
    expect(args).toContain('--disable-setuid-sandbox')
  })

  it('passes through extra args', () => {
    const args = chromiumArgs(['--proxy-server=http://proxy:8080'])
    expect(args).toContain('--proxy-server=http://proxy:8080')
    expect(args).toContain('--disable-dev-shm-usage')
  })

  it('preserves extra args order after --disable-dev-shm-usage', () => {
    const args = chromiumArgs(['--proxy-server=http://proxy:8080'])
    expect(args.indexOf('--disable-dev-shm-usage')).toBeLessThan(
      args.indexOf('--proxy-server=http://proxy:8080')
    )
  })
})

describe('browserProxyArgs', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
  })

  it('is empty without QUARRY_BROWSER_PROXY', () => {
    vi.stubEnv('QUARRY_BROWSER_PROXY', undefined)
    expect(browserProxyArgs()).toEqual([])
  })

  it('routes the browser through QUARRY_BROWSER_PROXY', () => {
    vi.stubEnv('QUARRY_BROWSER_PROXY', 'http://proxy.example:8080')
    expect(browserProxyArgs()).toEqual(['--proxy-server=http://proxy.example:8080'])
  })
})
