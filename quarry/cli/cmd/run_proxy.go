package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/pithecene/quarry/proxy"
	"github.com/pithecene/quarry/types"
)

// proxyChoice holds parsed proxy configuration.
type proxyChoice struct {
	configPath string
	poolName   string
	strategy   string
	stickyKey  string
	domain     string
	origin     string
}

// selectProxy loads proxy pools and selects an endpoint.
// Note: The selector is created fresh per invocation (CLI is one-shot).
// Round-robin counters and sticky maps do not persist across runs.
// This is intentional - each run is independent.
//
// configPools are pools defined inline in a quarry.yaml config file.
// They take priority over --proxy-config when present.
func selectProxy(config proxyChoice, runMeta *types.RunMeta, configPools []types.ProxyPool) (*types.ProxyEndpoint, error) {
	var selector *proxy.Selector
	var pools []types.ProxyPool

	switch {
	case len(configPools) > 0:
		// Use inline pools from config file
		pools = configPools
		selector = proxy.NewSelector()
		for _, pool := range pools {
			p := pool
			if err := selector.RegisterPool(&p); err != nil {
				return nil, fmt.Errorf("failed to register pool %q: %w", p.Name, err)
			}
		}
	case config.configPath != "":
		// Use legacy --proxy-config JSON file
		var err error
		selector, err = loadAndRegisterPools(config.configPath)
		if err != nil {
			return nil, err
		}
		pools, _ = loadProxyPools(config.configPath)
	default:
		return nil, errors.New("--proxy-config or config file proxies: required when --proxy-pool is specified")
	}

	// Warn about domain/origin sticky scopes without the required input.
	for _, pool := range pools {
		if pool.Name == config.poolName && pool.Sticky != nil {
			scope := pool.Sticky.Scope
			// Check if required input is missing for the scope
			switch scope {
			case types.ProxyStickyDomain:
				if config.stickyKey == "" && config.domain == "" {
					fmt.Fprintf(os.Stderr, "Warning: pool %q uses domain sticky scope but no --proxy-sticky-key or --proxy-domain provided\n", pool.Name)
				}
			case types.ProxyStickyOrigin:
				if config.stickyKey == "" && config.origin == "" {
					fmt.Fprintf(os.Stderr, "Warning: pool %q uses origin sticky scope but no --proxy-sticky-key or --proxy-origin provided\n", pool.Name)
				}
			case types.ProxyStickyJob:
				// The job scope keys on the job ID, which comes with the run, not a flag.
			}
		}
	}

	// Build selection request (commit for actual runs)
	req := proxy.SelectRequest{
		Pool:      config.poolName,
		StickyKey: config.stickyKey,
		Domain:    config.domain,
		Origin:    config.origin,
		Commit:    true,
	}

	// Set job ID for sticky scope derivation
	if runMeta.JobID != nil {
		req.JobID = *runMeta.JobID
	}

	// Set strategy override if specified
	if config.strategy != "" {
		strategy := types.ProxyStrategy(config.strategy)
		req.StrategyOverride = &strategy
	}

	// Select endpoint
	endpoint, err := selector.Select(req)
	if err != nil {
		return nil, fmt.Errorf("selection failed: %w", err)
	}

	return endpoint, nil
}

// loadAndRegisterPools loads proxy pools from a config file and returns a ready selector.
func loadAndRegisterPools(configPath string) (*proxy.Selector, error) {
	pools, err := loadProxyPools(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load proxy pools: %w", err)
	}
	selector := proxy.NewSelector()
	for _, pool := range pools {
		if err := selector.RegisterPool(&pool); err != nil {
			return nil, fmt.Errorf("failed to register pool %q: %w", pool.Name, err)
		}
	}
	return selector, nil
}

// loadProxyPools loads proxy pools from a JSON config file.
func loadProxyPools(path string) ([]types.ProxyPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var pools []types.ProxyPool
	if err := json.Unmarshal(data, &pools); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	return pools, nil
}
