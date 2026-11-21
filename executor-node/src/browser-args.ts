/**
 * Build Chromium launch arguments.
 * Always includes --disable-dev-shm-usage; conditionally adds sandbox flags.
 */
export function chromiumArgs(extra: string[] = []): string[] {
  const args = ['--disable-dev-shm-usage', ...extra]
  if (process.env.QUARRY_NO_SANDBOX === '1') {
    args.push('--no-sandbox', '--disable-setuid-sandbox')
  }
  return args
}

/**
 * Chromium arguments that route a shared browser through the run's proxy,
 * which the Go runtime passes in QUARRY_BROWSER_PROXY. Both the reusable
 * browser server and the managed fan-out browser launch with them, so a
 * child run that connects to either browser uses the proxy it resolved.
 */
export function browserProxyArgs(): string[] {
  const proxyUrl = process.env.QUARRY_BROWSER_PROXY
  return proxyUrl ? [`--proxy-server=${proxyUrl}`] : []
}
