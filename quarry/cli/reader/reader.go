// Package reader provides the read-side data access layer for the quarry CLI.
//
// This package isolates all read operations from runtime internals.
// All read-only commands use this wrapper exclusively per the implementation plan.
//
// The package uses dependency injection via SetReader() to allow swapping
// between stub and real implementations. Default is StubReader.
package reader

// InspectRun returns details for a specific run.
// Delegates to the package-level reader.
func InspectRun(runID string) *InspectRunResponse {
	return defaultReader.InspectRun(runID)
}

// InspectProxy returns details for a specific proxy pool.
// Delegates to the package-level reader.
func InspectProxy(poolName string) *InspectProxyPoolResponse {
	return defaultReader.InspectProxy(poolName)
}

// StatsRuns returns run statistics.
// Delegates to the package-level reader.
func StatsRuns() *RunStats {
	return defaultReader.StatsRuns()
}

// StatsProxies returns proxy statistics.
// Delegates to the package-level reader.
func StatsProxies() []ProxyStats {
	return defaultReader.StatsProxies()
}

// StatsMetrics returns contract metrics snapshot.
// Delegates to the package-level reader.
func StatsMetrics() *MetricsSnapshot {
	return defaultReader.StatsMetrics()
}

// ListRuns returns a list of runs with optional filtering.
// Delegates to the package-level reader.
func ListRuns(opts ListRunsOptions) []ListRunItem {
	return defaultReader.ListRuns(opts)
}

// ListPools returns a list of proxy pools.
// Delegates to the package-level reader.
func ListPools() []ListPoolItem {
	return defaultReader.ListPools()
}

// DebugResolveProxy resolves a proxy endpoint from a pool.
// If commit is true, advances rotation counters (ephemeral mutation).
// Delegates to the package-level reader.
func DebugResolveProxy(pool string, commit bool) (*ResolveProxyResponse, error) {
	return defaultReader.DebugResolveProxy(pool, commit)
}

// DebugIPC returns IPC debug information.
// Delegates to the package-level reader.
func DebugIPC(verbose bool) *IPCDebugResponse {
	return defaultReader.DebugIPC(verbose)
}
