// Package iox provides I/O helpers for resource cleanup.
package iox

import "io"

// DiscardClose closes c and discards the error.
// Use in defer statements where close errors are unactionable:
//
//	defer iox.DiscardClose(f)
func DiscardClose(c io.Closer) { _ = c.Close() }

// CloseFunc returns a cleanup function that closes c.
// Designed for t.Cleanup and b.Cleanup registration:
//
//	t.Cleanup(iox.CloseFunc(client))
func CloseFunc(c io.Closer) func() {
	return func() { _ = c.Close() }
}
