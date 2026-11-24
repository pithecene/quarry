/**
 * The child process runner that the integration tests share.
 */
import { spawn } from 'node:child_process'

/** The output and exit code of a child process. */
export type ProcessResult = { stdout: Buffer; stderr: string; exitCode: number | null }

/** Run a command with env added to this process's, and collect its output. */
export function runProcess(
  command: string,
  args: string[],
  env?: Record<string, string>
): Promise<ProcessResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, ...env }
    })
    const stdout: Buffer[] = []
    const stderr: Buffer[] = []
    child.stdout.on('data', (chunk: Buffer) => stdout.push(chunk))
    child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk))
    child.on('error', reject)
    child.on('close', (exitCode) => {
      resolve({
        stdout: Buffer.concat(stdout),
        stderr: Buffer.concat(stderr).toString('utf-8'),
        exitCode
      })
    })
  })
}
