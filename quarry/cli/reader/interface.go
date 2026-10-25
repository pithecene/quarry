package reader

// Reader abstracts read-only data access for CLI commands.
// Implementations may connect to Lode, use stubs, or aggregate from multiple sources.
//
// All methods are read-only and must not mutate state (except ephemeral
// mutations like debug.resolve-proxy --commit, which are in-memory only).
type Reader interface {
	// Inspect operations
	InspectRun(runID string) *InspectRunResponse
	InspectProxy(poolName string) *InspectProxyPoolResponse

	// Stats operations
	StatsRuns() *RunStats
	StatsProxies() []ProxyStats
	StatsMetrics() *MetricsSnapshot

	// List operations
	ListRuns(opts ListRunsOptions) []ListRunItem
	ListPools() []ListPoolItem

	// Debug operations
	DebugResolveProxy(pool string, commit bool) (*ResolveProxyResponse, error)
	DebugIPC(verbose bool) *IPCDebugResponse
}

// defaultReader is the package-level reader instance.
// Initialized to StubReader by default.
var defaultReader Reader = NewStubReader()

// SetReader sets the package-level reader instance.
// Call this during initialization to wire up the real implementation.
func SetReader(r Reader) {
	defaultReader = r
}

// GetReader returns the current package-level reader instance.
func GetReader() Reader {
	return defaultReader
}
