// Package testutil holds helpers that Quarry's tests share. It is internal,
// so no consumer of the module depends on it.
package testutil

import "testing"

// Serial declares that t runs serially. reason names the process-wide state
// the test depends on; the testparallel analyzer requires it to be a
// non-empty string literal (AGENTS.md § Testing).
func Serial(t *testing.T, reason string) {
	t.Helper()
	if reason == "" {
		t.Fatal("testutil.Serial: a reason is required: name the process-wide state the test depends on")
	}
}
