import childProcess from 'node:child_process'
import { chmodSync, existsSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { PassThrough } from 'node:stream'
import type { RunId } from '@pithecene/quarry-sdk'
import { describe, expect, it, vi } from 'vitest'
import { _resetPuppeteerForTesting, execute } from '../src/executor.js'

function groupOf(pid: number | undefined): string {
  return childProcess
    .execFileSync('ps', ['-o', 'pgid=', '-p', String(pid)])
    .toString()
    .trim()
}

describe("a run's browser", () => {
  // Puppeteer's own launcher starts a stand-in for Chrome, which reports its
  // process group and exits, so no browser is needed.
  it("starts in the executor's process group, and no other child joins it", async () => {
    _resetPuppeteerForTesting()
    const dir = mkdtempSync(join(tmpdir(), 'quarry-group-'))
    const chrome = join(dir, 'chrome')
    const report = join(dir, 'pgid')
    const observed = join(dir, 'observed')
    // It holds the launch open until the helper has been observed, for 10 seconds at most.
    writeFileSync(
      chrome,
      `#!/bin/sh\nps -o pgid= -p $$ > '${report}'\ni=0\nwhile [ ! -e '${observed}' ] && [ $i -lt 100 ]; do sleep 0.1; i=$((i+1)); done\nexit 1\n`
    )
    chmodSync(chrome, 0o755)
    const script = join(dir, 'script.mjs')
    writeFileSync(script, 'export default async () => {}\n')

    // A helper that other code detaches while the launch is pending.
    const observeHelper = async () => {
      await vi.waitFor(() => expect(existsSync(report)).toBe(true), { timeout: 5000 })
      const child = childProcess.spawn('sleep', ['30'], { detached: true, stdio: 'ignore' })
      try {
        return { pid: child.pid, group: groupOf(child.pid) }
      } finally {
        child.kill('SIGKILL')
        writeFileSync(observed, '')
      }
    }
    const output = new PassThrough()
    output.resume()
    const run = execute({
      scriptPath: script,
      job: {},
      run: { run_id: 'run-1' as RunId, attempt: 1 },
      output,
      stealth: false,
      puppeteerOptions: { executablePath: chrome }
    })
    const [result, { pid, group }] = await Promise.all([run, observeHelper()])

    expect(result.outcome.status).not.toBe('completed')
    await vi.waitFor(() => expect(readFileSync(report, 'utf8').trim()).toBe(groupOf(process.pid)))
    expect(group).toBe(String(pid))
  })
})
