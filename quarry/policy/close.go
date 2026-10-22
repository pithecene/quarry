package policy

import (
	"errors"
	"sync"
	"time"
)

// ErrCloseUnflushed is returned by Close when accepted data is still buffered
// after the close-time flush, so it was never persisted.
var ErrCloseUnflushed = errors.New("policy closed with unflushed data")

// closeFlushTimeout is the deadline of the best-effort flush a policy performs
// on Close. It bounds Close for sinks that honour cancellation (see Sink).
var closeFlushTimeout = 30 * time.Second // var so tests can shorten it

// onceCloser makes a policy's Close one-shot, so a policy closed by one owner
// and closed again by another (a deferred Close) flushes and writes only once.
type onceCloser struct {
	once sync.Once
	err  error
}

// close runs fn on the first call and returns its error on every call.
func (c *onceCloser) close(fn func() error) error {
	c.once.Do(func() { c.err = fn() })
	return c.err
}
