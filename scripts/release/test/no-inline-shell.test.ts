import { spawnSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { expect, it, onTestFinished } from 'vitest'

const guard = resolve(import.meta.dirname, '../../ci/no-inline-shell.sh')

const run = (path: string, body: string): number | null => {
  const root = mkdtempSync(join(tmpdir(), 'no-inline-shell-'))
  onTestFinished(() => rmSync(root, { recursive: true, force: true }))
  for (const [file, text] of [
    ['.github/workflows/ci.yml', ''],
    ['Taskfile.yaml', ''],
    [path, body]
  ] as const) {
    mkdirSync(dirname(join(root, file)), { recursive: true })
    writeFileSync(join(root, file), `${text}\n`)
  }
  return spawnSync(guard, { cwd: root }).status
}

const WORKFLOW = '.github/workflows/ci.yml'

it.each([
  [WORKFLOW, '      - run: |\n          echo hi'],
  [WORKFLOW, '      - run: >-\n          echo hi'],
  [WORKFLOW, '        run: echo $(date)'],
  [WORKFLOW, '        run: task a && task b'],
  [WORKFLOW, '        run: a | b'],
  [WORKFLOW, '        run: echo x >> "$GITHUB_OUTPUT"'],
  [WORKFLOW, `        run: publish "\${GITHUB_REF_NAME#v}"`],
  [WORKFLOW, `        run: publish \${{ inputs.version }}`],
  ['.github/actions/setup/action.yaml', '      run: |\n        echo hi'],
  ['.github/actions/deep/er/action.yml', '      run: a; b'],
  ['Taskfile.yaml', '      - cmd: |\n          cd sdk'],
  ['Taskfile.yaml', '      - cd sdk && pnpm test'],
  ['Taskfile.yaml', '        sh: a || b']
])('refuses shell in %s: %s', (path, body) => {
  expect(run(path, body)).toBe(1)
})

it.each([
  [WORKFLOW, '        run: task lint'],
  [WORKFLOW, '        run: scripts/version-check.sh "$GITHUB_REF_NAME"'],
  [WORKFLOW, `    if: >-\n      \${{ !cancelled() && true }}`],
  ['Taskfile.yaml', '      - go build -o quarry ./cmd/quarry']
])('allows a plain command in %s: %s', (path, body) => {
  expect(run(path, body)).toBe(0)
})
