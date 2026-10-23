package proxy

import (
	"testing"
	"time"

	"github.com/pithecene/quarry/types"
)

func TestSelector_RoundRobin(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategyRoundRobin,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p3.example.com", Port: 8080},
		},
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Select in round-robin order (Commit: true to advance counter)
	hosts := make([]string, 6)
	for i := 0; i < 6; i++ {
		ep, err := s.Select(SelectRequest{Pool: "test", Commit: true})
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
		hosts[i] = ep.Host
	}

	// Should cycle through endpoints
	expected := []string{
		"p1.example.com",
		"p2.example.com",
		"p3.example.com",
		"p1.example.com",
		"p2.example.com",
		"p3.example.com",
	}

	for i, exp := range expected {
		if hosts[i] != exp {
			t.Errorf("hosts[%d] = %q, want %q", i, hosts[i], exp)
		}
	}
}

func TestSelector_Random(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategyRandom,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p3.example.com", Port: 8080},
		},
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Select multiple times - should not panic
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		ep, err := s.Select(SelectRequest{Pool: "test"})
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
		seen[ep.Host] = true
	}

	// With 100 selections, we should see all endpoints (probabilistically)
	if len(seen) < 2 {
		t.Errorf("random selection seems broken: only saw %d unique hosts", len(seen))
	}
}

func TestSelector_Sticky_Job(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategySticky,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p3.example.com", Port: 8080},
		},
		Sticky: &types.ProxySticky{
			Scope: types.ProxyStickyJob,
		},
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Same job should get same endpoint (Commit: true to store sticky entry)
	ep1, err := s.Select(SelectRequest{Pool: "test", JobID: "job-123", Commit: true})
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	ep2, err := s.Select(SelectRequest{Pool: "test", JobID: "job-123", Commit: true})
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	if ep1.Host != ep2.Host {
		t.Errorf("same job got different endpoints: %q vs %q", ep1.Host, ep2.Host)
	}

	// Different job may get different endpoint (not guaranteed, but verify no error)
	_, err = s.Select(SelectRequest{Pool: "test", JobID: "job-456", Commit: true})
	if err != nil {
		t.Fatalf("Select failed for different job: %v", err)
	}
}

func TestSelector_Sticky_WithTTL(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	ttl := int64(50) // 50ms TTL
	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategySticky,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
		},
		Sticky: &types.ProxySticky{
			Scope: types.ProxyStickyJob,
			TTLMs: &ttl,
		},
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Get initial assignment (Commit: true to store sticky entry)
	ep1, err := s.Select(SelectRequest{Pool: "test", JobID: "job-123", Commit: true})
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	// After TTL, may get different endpoint
	ep2, err := s.Select(SelectRequest{Pool: "test", JobID: "job-123", Commit: true})
	if err != nil {
		t.Fatalf("Select failed after TTL: %v", err)
	}

	// Note: ep2 may or may not equal ep1 (random selection after TTL)
	// We just verify it doesn't error
	_ = ep1
	_ = ep2
}

func TestSelector_Sticky_ExplicitKey(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategySticky,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
		},
		Sticky: &types.ProxySticky{
			Scope: types.ProxyStickyDomain,
		},
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Explicit sticky key takes precedence over domain (Commit: true to store)
	ep1, err := s.Select(SelectRequest{
		Pool:      "test",
		StickyKey: "my-explicit-key",
		Domain:    "example.com",
		Commit:    true,
	})
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	ep2, err := s.Select(SelectRequest{
		Pool:      "test",
		StickyKey: "my-explicit-key",
		Domain:    "different.com", // Different domain, but same explicit key
		Commit:    true,
	})
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	if ep1.Host != ep2.Host {
		t.Errorf("explicit sticky key should give same endpoint: %q vs %q", ep1.Host, ep2.Host)
	}
}

func TestSelector_StrategyOverride(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategyRoundRobin, // Default is round-robin
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
		},
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Override to random
	randomStrategy := types.ProxyStrategyRandom
	_, err := s.Select(SelectRequest{
		Pool:             "test",
		StrategyOverride: &randomStrategy,
	})
	if err != nil {
		t.Fatalf("Select with strategy override failed: %v", err)
	}
}

func TestSelector_PoolNotFound(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	_, err := s.Select(SelectRequest{Pool: "nonexistent"})
	if err == nil {
		t.Error("expected error for nonexistent pool")
	}
}

func TestSelector_ValidationFailure(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	pool := &types.ProxyPool{
		Name:      "test",
		Strategy:  types.ProxyStrategyRoundRobin,
		Endpoints: []types.ProxyEndpoint{}, // Invalid: no endpoints
	}

	err := s.RegisterPool(pool)
	if err == nil {
		t.Error("expected validation error for empty endpoints")
	}
}

func TestSelector_Commit_RoundRobin(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategyRoundRobin,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p3.example.com", Port: 8080},
		},
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Peek (Commit: false) should return same endpoint repeatedly
	ep1, _ := s.Select(SelectRequest{Pool: "test", Commit: false})
	ep2, _ := s.Select(SelectRequest{Pool: "test", Commit: false})
	ep3, _ := s.Select(SelectRequest{Pool: "test", Commit: false})

	if ep1.Host != ep2.Host || ep2.Host != ep3.Host {
		t.Errorf("peek should return same endpoint: got %q, %q, %q", ep1.Host, ep2.Host, ep3.Host)
	}

	// Commit should advance the counter
	epCommit, _ := s.Select(SelectRequest{Pool: "test", Commit: true})
	if epCommit.Host != ep1.Host {
		t.Errorf("first commit should return same as peek: got %q, want %q", epCommit.Host, ep1.Host)
	}

	// Now peek should return next endpoint
	epPeek, _ := s.Select(SelectRequest{Pool: "test", Commit: false})
	if epPeek.Host == ep1.Host {
		t.Errorf("peek after commit should return next endpoint, got same: %q", epPeek.Host)
	}
}

func TestSelector_Commit_Sticky(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategySticky,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
		},
		Sticky: &types.ProxySticky{
			Scope: types.ProxyStickyJob,
		},
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Peek (Commit: false) should NOT store the sticky entry
	ep1, _ := s.Select(SelectRequest{Pool: "test", JobID: "job-new", Commit: false})

	// Check stats - should have 0 sticky entries since we didn't commit
	stats, _ := s.Stats("test")
	if stats.StickyEntries != 0 {
		t.Errorf("StickyEntries after peek = %d, want 0", stats.StickyEntries)
	}

	// Now commit - should store the entry
	_, _ = s.Select(SelectRequest{Pool: "test", JobID: "job-new", Commit: true})
	stats, _ = s.Stats("test")
	if stats.StickyEntries != 1 {
		t.Errorf("StickyEntries after commit = %d, want 1", stats.StickyEntries)
	}

	// Subsequent selects (even peek) should return the stored assignment
	ep2, _ := s.Select(SelectRequest{Pool: "test", JobID: "job-new", Commit: false})
	// Note: ep2 should equal the committed assignment, not necessarily ep1
	// (ep1 may differ due to random selection without commit)
	_ = ep1
	_ = ep2
}

func TestSelector_Stats(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategySticky,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
		},
		Sticky: &types.ProxySticky{
			Scope: types.ProxyStickyJob,
		},
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Make some selections (Commit: true to store sticky entries)
	_, _ = s.Select(SelectRequest{Pool: "test", JobID: "job-1", Commit: true})
	_, _ = s.Select(SelectRequest{Pool: "test", JobID: "job-2", Commit: true})

	stats, err := s.Stats("test")
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if stats.StickyEntries != 2 {
		t.Errorf("StickyEntries = %d, want 2", stats.StickyEntries)
	}
}

// A recency window keeps consecutive selections from repeating an
// endpoint. A window smaller than the pool avoids the recent endpoints; a
// window that covers the pool falls back to the least recently used one,
// which rotates once the ring is full.
func TestSelector_RecencyWindow(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name               string
		window, selections int
		checkFrom          int // first index compared with its predecessor
	}{
		{"avoids recent endpoints", 2, 9, 1},
		{"least recently used fallback", 3, 6, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSelector()
			window := tc.window
			pool := &types.ProxyPool{
				Name:     "test",
				Strategy: types.ProxyStrategyRandom,
				Endpoints: []types.ProxyEndpoint{
					{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
					{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
					{Protocol: types.ProxyProtocolHTTP, Host: "p3.example.com", Port: 8080},
				},
				RecencyWindow: &window,
			}
			if err := s.RegisterPool(pool); err != nil {
				t.Fatalf("RegisterPool failed: %v", err)
			}
			seen := make([]string, 0, tc.selections)
			for range tc.selections {
				ep, err := s.Select(SelectRequest{Pool: "test", Commit: true})
				if err != nil {
					t.Fatalf("Select failed: %v", err)
				}
				seen = append(seen, ep.Host)
			}
			for i := tc.checkFrom; i < len(seen); i++ {
				if seen[i] == seen[i-1] {
					t.Errorf("consecutive selections at %d and %d both got %q", i-1, i, seen[i])
				}
			}
		})
	}
}

func TestSelector_RecencyWindow_PeekDoesNotAdvanceRing(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	window := 2
	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategyRandom,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p3.example.com", Port: 8080},
		},
		RecencyWindow: &window,
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Peek should not advance ring
	_, _ = s.Select(SelectRequest{Pool: "test", Commit: false})
	_, _ = s.Select(SelectRequest{Pool: "test", Commit: false})
	_, _ = s.Select(SelectRequest{Pool: "test", Commit: false})

	// Ring should be empty (nothing committed)
	stats, _ := s.Stats("test")
	if stats.RecencyFill != 0 {
		t.Errorf("RecencyFill after peeks = %d, want 0", stats.RecencyFill)
	}

	// Now commit
	_, _ = s.Select(SelectRequest{Pool: "test", Commit: true})
	stats, _ = s.Stats("test")
	if stats.RecencyFill != 1 {
		t.Errorf("RecencyFill after 1 commit = %d, want 1", stats.RecencyFill)
	}
}

func TestSelector_RecencyWindow_Stats(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	window := 3
	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategyRandom,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p3.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p4.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p5.example.com", Port: 8080},
		},
		RecencyWindow: &window,
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	stats, _ := s.Stats("test")
	if stats.RecencyWindow == nil || *stats.RecencyWindow != 3 {
		t.Errorf("RecencyWindow = %v, want 3", stats.RecencyWindow)
	}
	if stats.RecencyFill != 0 {
		t.Errorf("RecencyFill = %d, want 0", stats.RecencyFill)
	}

	// Make 2 committed selections
	_, _ = s.Select(SelectRequest{Pool: "test", Commit: true})
	_, _ = s.Select(SelectRequest{Pool: "test", Commit: true})

	stats, _ = s.Stats("test")
	if stats.RecencyFill != 2 {
		t.Errorf("RecencyFill after 2 commits = %d, want 2", stats.RecencyFill)
	}

	// Fill the ring
	_, _ = s.Select(SelectRequest{Pool: "test", Commit: true})
	_, _ = s.Select(SelectRequest{Pool: "test", Commit: true})

	stats, _ = s.Stats("test")
	if stats.RecencyFill != 3 {
		t.Errorf("RecencyFill should cap at window size, got %d, want 3", stats.RecencyFill)
	}
}

func TestSelector_RecencyWindow_NoRecencyWithoutConfig(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	// Pool without recency window
	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategyRandom,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
		},
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Should work fine without recency window
	for i := 0; i < 10; i++ {
		_, err := s.Select(SelectRequest{Pool: "test", Commit: true})
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
	}

	stats, _ := s.Stats("test")
	if stats.RecencyWindow != nil {
		t.Errorf("RecencyWindow should be nil, got %d", *stats.RecencyWindow)
	}
	if stats.RecencyFill != 0 {
		t.Errorf("RecencyFill should be 0 without window, got %d", stats.RecencyFill)
	}
}

func TestSelector_RecencyWindow_SingleEndpoint(t *testing.T) {
	t.Parallel()

	s := NewSelector()

	window := 1
	pool := &types.ProxyPool{
		Name:     "test",
		Strategy: types.ProxyStrategyRandom,
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
		},
		RecencyWindow: &window,
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Should always return the single endpoint (LRU fallback)
	for i := 0; i < 5; i++ {
		ep, err := s.Select(SelectRequest{Pool: "test", Commit: true})
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
		if ep.Host != "p1.example.com" {
			t.Errorf("expected p1.example.com, got %s", ep.Host)
		}
	}
}

func TestSelector_RecencyWindow_StickyIgnoresRecency(t *testing.T) {
	t.Parallel()

	// Per CONTRACT_PROXY.md, recency_window is only meaningful for random.
	// A sticky pool with recency_window set should NOT have recency applied.
	w := 2
	s := NewSelector()
	pool := &types.ProxyPool{
		Name:     "sticky-recency",
		Strategy: types.ProxyStrategySticky,
		Sticky: &types.ProxySticky{
			Scope: types.ProxyStickyJob,
		},
		Endpoints: []types.ProxyEndpoint{
			{Protocol: types.ProxyProtocolHTTP, Host: "p1.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p2.example.com", Port: 8080},
			{Protocol: types.ProxyProtocolHTTP, Host: "p3.example.com", Port: 8080},
		},
		RecencyWindow: &w,
	}

	if err := s.RegisterPool(pool); err != nil {
		t.Fatalf("RegisterPool failed: %v", err)
	}

	// Sticky selection should return the same endpoint for the same key
	ep1, err := s.Select(SelectRequest{
		Pool: "sticky-recency", StickyKey: "job-1", Commit: true,
	})
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	// Repeated selection with same key must return same endpoint (sticky)
	for range 5 {
		ep, err := s.Select(SelectRequest{
			Pool: "sticky-recency", StickyKey: "job-1", Commit: true,
		})
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
		if ep.Host != ep1.Host {
			t.Errorf("sticky should return same host, got %s then %s", ep1.Host, ep.Host)
		}
	}

	// Verify recency ring was NOT initialized
	stats, err := s.Stats("sticky-recency")
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.RecencyFill != 0 {
		t.Errorf("expected RecencyFill=0 for sticky pool, got %d", stats.RecencyFill)
	}
}
