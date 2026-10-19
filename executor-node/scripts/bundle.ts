#!/usr/bin/env tsx
import { readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
/**
 * Bundle the executor into a standalone ESM file for embedding in the Go binary.
 *
 * This bundles:
 * - Executor code
 * - SDK (@pithecene/quarry-sdk)
 * - msgpack (@msgpack/msgpack)
 *
 * External (not bundled):
 * - puppeteer (peer dependency, user must install)
 * - Node built-ins
 */
import * as esbuild from 'esbuild'

const __dirname = dirname(fileURLToPath(import.meta.url))
const root = join(__dirname, '..')

// Read version from quarry/types/version.go for embedding
function getQuarryVersion(): string {
  const versionFile = join(root, '..', 'quarry', 'types', 'version.go')
  try {
    const content = readFileSync(versionFile, 'utf-8')
    const match = content.match(/const Version = "([^"]+)"/)
    if (match) {
      return match[1]
    }
  } catch {
    // Ignore
  }
  return '0.0.0-unknown'
}

async function bundle() {
  const version = getQuarryVersion()
  console.log(`Bundling executor v${version}...`)

  // Write directly to go:embed source path — single canonical location
  const embedDir = join(root, '..', 'quarry', 'executor', 'bundle')
  const bundlePath = join(embedDir, 'executor.mjs')

  const result = await esbuild.build({
    entryPoints: [join(root, 'src', 'bin', 'executor.ts')],
    bundle: true,
    platform: 'node',
    target: 'node22',
    format: 'esm',
    outfile: bundlePath,
    external: [
      'puppeteer',
      'puppeteer-extra',
      'puppeteer-extra-plugin-stealth',
      'puppeteer-extra-plugin-adblocker'
    ],
    minify: false, // Keep readable for debugging
    sourcemap: false, // No sourcemaps in embedded bundle
    banner: {
      js: `// Quarry Executor Bundle v${version}
// This is a bundled version for embedding in the quarry binary.
// Do not edit directly - regenerate with: task executor:bundle
`
    },
    define: {
      'process.env.QUARRY_EXECUTOR_VERSION': JSON.stringify(version)
    },
    metafile: true
  })

  // Write metafile for analysis (stays in dist/)
  writeFileSync(join(root, 'dist', 'bundle', 'meta.json'), JSON.stringify(result.metafile, null, 2))

  // Ensure bundle has exactly one shebang at the start for direct execution
  let bundleContent = readFileSync(bundlePath, 'utf-8')
  // Remove any shebangs that might be in the middle (from source)
  bundleContent = bundleContent.replace(/^#!.*\n/gm, '')
  // Add shebang at the very start
  bundleContent = '#!/usr/bin/env node\n' + bundleContent
  writeFileSync(bundlePath, bundleContent)

  // Get bundle size
  const stats = readFileSync(bundlePath)
  const sizeKB = (stats.length / 1024).toFixed(1)

  console.log(`Bundle created: quarry/executor/bundle/executor.mjs (${sizeKB} KB)`)
  console.log(`Version: ${version}`)
}

bundle().catch((err) => {
  console.error('Bundle failed:', err)
  process.exit(1)
})
