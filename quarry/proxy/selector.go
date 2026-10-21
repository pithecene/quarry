// Package proxy implements proxy pool selection per CONTRACT_PROXY.md.
package proxy

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"

	"github.com/pithecene/quarry/types"
)

// Selector manages proxy selection from pools.
// Thread-safe for concurrent access.
type Selector struct {
	mu    sync.Mutex
	pools map[string]*poolState
}

// poolState holds runtime state for a single pool.
type poolState struct {
	pool      *types.ProxyPool
	rrIndex   int64                   // round-robin counter
	stickyMap map[string]*stickyEntry // sticky key -> entry

	// recencyRing is a fixed-size ring buffer of recently-used endpoint indices.
	// Only populated when pool.RecencyWindow is set.
	recencyRing []int // ring buffer (capacity = recency window size)
	recencyHead int   // next write position in the ring
	recencyFill int   // number of valid entries in the ring (0..cap)
}

// stickyEntry holds a sticky assignment with optional TTL.
type stickyEntry struct {
	endpointIdx int
	expiresAt   *time.Time
}

// NewSelector creates a new proxy selector.
func NewSelector() *Selector {
	return &Selector{
		pools: make(map[string]*poolState),
	}
}

// RegisterPool registers a proxy pool.
// Returns error if pool validation fails.
// Emits soft warnings per CONTRACT_PROXY.md to stderr.
func (s *Selector) RegisterPool(pool *types.ProxyPool) error {
	if err := pool.Validate(); err != nil {
		return fmt.Errorf("pool validation failed: %w", err)
	}

	// Emit soft warnings per CONTRACT_PROXY.md
	warnings := pool.Warnings()
	for _, w := range warnings {
		fmt.Fprintf(os.Stderr, "Warning: %s\n", w)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state := &poolState{
		pool:      pool,
		rrIndex:   0,
		stickyMap: make(map[string]*stickyEntry),
	}

	// Initialize recency ring buffer only for random strategy.
	// Per CONTRACT_PROXY.md, recency_window is only meaningful for random.
	// Sticky internally delegates to random for new assignments, but should
	// not apply recency exclusion — the sticky scope controls reuse, not recency.
	if pool.Strategy == types.ProxyStrategyRandom && pool.RecencyWindow != nil && *pool.RecencyWindow > 0 {
		state.recencyRing = make([]int, *pool.RecencyWindow)
	}

	s.pools[pool.Name] = state

	return nil
}

// SelectRequest contains parameters for endpoint selection.
type SelectRequest struct {
	// Pool is the pool name to select from.
	Pool string
	// StrategyOverride optionally overrides the pool's strategy.
	StrategyOverride *types.ProxyStrategy
	// StickyKey is the key for sticky selection.
	// Required when strategy is sticky (either from pool or override).
	StickyKey string
	// Domain is used to derive sticky key when scope is "domain".
	Domain string
	// Origin is used to derive sticky key when scope is "origin" (scheme+host+port).
	Origin string
	// JobID is used to derive sticky key when scope is "job".
	JobID string
	// Commit determines whether to advance rotation counters.
	// When false, returns what would be selected without mutating state.
	Commit bool
}

// Select selects a proxy endpoint from the specified pool.
// Returns error if pool is not found or selection fails.
func (s *Selector) Select(req SelectRequest) (*types.ProxyEndpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.pools[req.Pool]
	if !ok {
		return nil, fmt.Errorf("pool %q not found", req.Pool)
	}

	// Determine effective strategy
	strategy := state.pool.Strategy
	if req.StrategyOverride != nil {
		strategy = *req.StrategyOverride
	}

	var idx int
	var err error

	switch strategy {
	case types.ProxyStrategyRoundRobin:
		idx = s.selectRoundRobin(state, req.Commit)
	case types.ProxyStrategyRandom:
		idx, err = s.selectRandom(state, req.Commit)
		if err != nil {
			return nil, err
		}
	case types.ProxyStrategySticky:
		idx, err = s.selectSticky(state, req, req.Commit)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown strategy %q", strategy)
	}

	// Return a copy of the endpoint
	ep := state.pool.Endpoints[idx]
	return &ep, nil
}

// selectRoundRobin selects using round-robin.
// Increments counter only when commit is true.
func (s *Selector) selectRoundRobin(state *poolState, commit bool) int {
	idx := int(state.rrIndex % int64(len(state.pool.Endpoints)))
	if commit {
		state.rrIndex++
	}
	return idx
}

// selectRandom selects uniformly at random, excluding recently-used indices
// when a recency window is configured.
//
// When recencyRing is non-nil:
//   - Builds excluded set from the ring buffer
//   - If all endpoints are excluded (window >= endpoint count), uses LRU fallback
//   - On commit, records the selected index in the ring buffer
//   - On peek (commit=false), does not advance the ring
func (s *Selector) selectRandom(state *poolState, commit bool) (int, error) {
	n := len(state.pool.Endpoints)
	if n == 1 {
		// Only one endpoint — always return it, record if committing
		if commit && state.recencyRing != nil {
			s.recordRecency(state, 0)
		}
		return 0, nil
	}

	// No recency window — simple random selection
	if state.recencyRing == nil {
		return s.randomIndex(n)
	}

	// Build excluded set from ring buffer
	excluded := s.recencyExcluded(state)

	// If all endpoints are excluded, fall back to LRU
	if len(excluded) >= n {
		idx := s.recencyLRU(state)
		if commit {
			s.recordRecency(state, idx)
		}
		return idx, nil
	}

	// Build candidates (indices not in excluded set)
	candidates := make([]int, 0, n-len(excluded))
	for i := range n {
		if !excluded[i] {
			candidates = append(candidates, i)
		}
	}

	// Random selection from candidates
	candidateIdx, err := s.randomIndex(len(candidates))
	if err != nil {
		return 0, err
	}
	idx := candidates[candidateIdx]

	if commit {
		s.recordRecency(state, idx)
	}

	return idx, nil
}

// randomIndex returns a cryptographically random index in [0, n).
func (s *Selector) randomIndex(n int) (int, error) {
	bigN := big.NewInt(int64(n))
	bigIdx, err := rand.Int(rand.Reader, bigN)
	if err != nil {
		return 0, fmt.Errorf("random selection failed: %w", err)
	}
	return int(bigIdx.Int64()), nil
}

// recencyExcluded returns the set of endpoint indices in the ring buffer.
func (s *Selector) recencyExcluded(state *poolState) map[int]bool {
	excluded := make(map[int]bool, state.recencyFill)
	cap := len(state.recencyRing)
	for i := range state.recencyFill {
		// Walk backwards from head to get valid entries
		pos := (state.recencyHead - 1 - i + cap) % cap
		excluded[state.recencyRing[pos]] = true
	}
	return excluded
}

// recencyLRU returns the least-recently-used endpoint index (oldest entry in the ring).
func (s *Selector) recencyLRU(state *poolState) int {
	cap := len(state.recencyRing)
	// Oldest entry is at (head - fill) mod cap
	oldest := (state.recencyHead - state.recencyFill + cap) % cap
	return state.recencyRing[oldest]
}

// recordRecency writes an endpoint index into the ring buffer.
func (s *Selector) recordRecency(state *poolState, idx int) {
	cap := len(state.recencyRing)
	state.recencyRing[state.recencyHead] = idx
	state.recencyHead = (state.recencyHead + 1) % cap
	if state.recencyFill < cap {
		state.recencyFill++
	}
}

// selectSticky selects using sticky assignment.
// Stores new assignment only when commit is true.
func (s *Selector) selectSticky(state *poolState, req SelectRequest, commit bool) (int, error) {
	// Derive sticky key per CONTRACT_PROXY.md precedence
	stickyKey := s.deriveStickyKey(state, req)
	if stickyKey == "" {
		return 0, errors.New("sticky selection requires a sticky key")
	}

	now := time.Now()

	// Check existing entry
	if entry, ok := state.stickyMap[stickyKey]; ok {
		// Check TTL expiration
		if entry.expiresAt == nil || entry.expiresAt.After(now) {
			return entry.endpointIdx, nil
		}
		// Entry expired, remove it
		delete(state.stickyMap, stickyKey)
	}

	// Select new endpoint (use random for new assignments)
	idx, err := s.selectRandom(state, commit)
	if err != nil {
		return 0, err
	}

	// Store assignment only when commit is true
	if commit {
		entry := &stickyEntry{endpointIdx: idx}
		if state.pool.Sticky != nil && state.pool.Sticky.TTLMs != nil {
			ttl := time.Duration(*state.pool.Sticky.TTLMs) * time.Millisecond
			expiresAt := now.Add(ttl)
			entry.expiresAt = &expiresAt
		}
		state.stickyMap[stickyKey] = entry
	}

	return idx, nil
}

// deriveStickyKey derives the sticky key per CONTRACT_PROXY.md precedence:
// 1. req.StickyKey if provided
// 2. if scope = job: req.JobID
// 3. if scope = domain: req.Domain
// 4. if scope = origin: req.Origin
func (s *Selector) deriveStickyKey(state *poolState, req SelectRequest) string {
	// 1. Explicit sticky key takes precedence
	if req.StickyKey != "" {
		return req.StickyKey
	}

	// 2. Derive from scope
	if state.pool.Sticky == nil {
		// No sticky config, use JobID as default
		return req.JobID
	}

	switch state.pool.Sticky.Scope {
	case types.ProxyStickyJob:
		return req.JobID
	case types.ProxyStickyDomain:
		return req.Domain
	case types.ProxyStickyOrigin:
		return req.Origin
	default:
		return req.JobID
	}
}

// PoolStats returns statistics for a pool.
type PoolStats struct {
	RoundRobinIndex int64
	StickyEntries   int
	RecencyWindow   *int // nil if not configured
	RecencyFill     int  // number of entries in the recency ring
}

// Stats returns statistics for a pool.
func (s *Selector) Stats(poolName string) (*PoolStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.pools[poolName]
	if !ok {
		return nil, fmt.Errorf("pool %q not found", poolName)
	}

	stats := &PoolStats{
		RoundRobinIndex: state.rrIndex,
		StickyEntries:   len(state.stickyMap),
		RecencyFill:     state.recencyFill,
	}
	if state.pool.RecencyWindow != nil {
		w := *state.pool.RecencyWindow
		stats.RecencyWindow = &w
	}
	return stats, nil
}

// CleanExpiredSticky removes expired sticky entries from all pools.
// Call periodically to prevent unbounded growth.
func (s *Selector) CleanExpiredSticky() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, state := range s.pools {
		for key, entry := range state.stickyMap {
			if entry.expiresAt != nil && entry.expiresAt.Before(now) {
				delete(state.stickyMap, key)
			}
		}
	}
}
