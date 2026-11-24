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
    writeFileSync(chrome, `#!/bin/sh\nps -o pgid= -p $$ > '${report}'\nsleep 1\nexit 1\n`)
    chmodSync(chrome, 0o755)
    const script = join(dir, 'script.mjs')
    writeFileSync(script, 'export default async () => {}\n')

    // A helper that other code detaches while the launch is pending.
    const helper = new Promise<{ pid: number | undefined; group: string }>((done, fail) => {
      const timer = setInterval(() => {
        if (!existsSync(report)) return
        clearInterval(timer)
        try {
          const child = childProcess.spawn('sleep', ['30'], { detached: true, stdio: 'ignore' })
          done({ pid: child.pid, group: groupOf(child.pid) })
          child.kill('SIGKILL')
        } catch (err) {
          fail(err)
        }
      }, 20)
    })
    const output = new PassThrough()
    output.resume()
    const result = await execute({
      scriptPath: script,
      job: {},
      run: { run_id: 'run-1' as RunId, attempt: 1 },
      output,
      stealth: false,
      puppeteerOptions: { executablePath: chrome }
    })

    expect(result.outcome.status).not.toBe('completed')
    await vi.waitFor(() => expect(readFileSync(report, 'utf8').trim()).toBe(groupOf(process.pid)))
    const { pid, group } = await helper
    expect(group).toBe(String(pid))
  })
})
