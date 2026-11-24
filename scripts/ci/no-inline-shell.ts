/**
 * Fails when a workflow, composite action, or Taskfile holds shell logic, which
 * belongs in scripts/, where shellcheck reads it.
 *
 * A command in YAML is one plain line of an allowlist of characters, so every
 * shell operator, continuation, block, and GitHub expression is refused without
 * a list of them.
 *
 * Usage: node scripts/ci/no-inline-shell.ts
 */
import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'

const FILES = /^(\.github\/workflows\/.+\.ya?ml|(.+\/)?action\.ya?ml|Taskfile\.ya?ml)$/
const PLAIN = /^(?:[\w ./:=@%+,*"'{}[\]-]|\$[A-Z_]+\b)*$/i
const COMMAND = /^(\s*)(?:- )?["']?(?:run|cmd|sh)["']?:\s*(.*)$/
const FLOW = /[{,]\s*["']?(?:run|cmd|sh)["']?:/
const ITEM = /^(\s*)- (.*)$/
const KEY = /^\s*[\w-]+:(\s|$)/

const indent = (line: string): number => line.search(/\S/)
const withoutComment = (value: string): string =>
  /["']/.test(value) ? value : value.replace(/(^|\s)#.*$/, '')

/** The numbers of the lines that hold shell logic. */
const shellLines = (file: string, text: string): number[] => {
  const lines = text.split('\n')
  return lines.flatMap((line, i) => {
    const command = COMMAND.exec(line)
    const item = file.startsWith('Taskfile') ? ITEM.exec(line) : null
    const value = command?.[2] ?? item?.[2]
    if (FLOW.test(line)) return [i + 1]
    if (value === undefined) return []
    const next = lines.slice(i + 1).find((l) => l.trim() !== '' && !l.trim().startsWith('#')) ?? ''
    const column = command ? line.search(/["']?(?:run|cmd|sh)["']?:/) : indent(line)
    // A deeper line continues a command. Under a list item, it may be a mapping's key.
    const continued = indent(next) > column && !(command === null && KEY.test(next))
    return continued || !PLAIN.test(withoutComment(value).trim()) ? [i + 1] : []
  })
}

const tracked = execFileSync('git', ['ls-files', '-z'], { encoding: 'utf8' }).split('\0')
const found = tracked
  .filter((file) => FILES.test(file))
  .flatMap((file) => shellLines(file, readFileSync(file, 'utf8')).map((n) => `${file}:${n}`))
for (const place of found) console.error(`::error::${place}: shell in YAML; move it to a script`)
process.exitCode = found.length > 0 ? 1 : 0
