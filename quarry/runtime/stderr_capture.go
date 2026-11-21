package runtime

import (
	"fmt"
	"io"
)

// maxStderrBytes bounds the executor stderr that a run keeps. The tail is
// kept, since an executor reports the failure that ends it last.
const maxStderrBytes = 1 << 20

// stderrCapture drains an executor's stderr while the run ingests stdout, so
// that an executor that writes more stderr than the pipe holds never blocks.
// It keeps the last maxStderrBytes bytes and counts the bytes it dropped.
type stderrCapture struct {
	tail    []byte
	dropped int64
	done    chan struct{}
}

// startStderrCapture drains r on its own goroutine until r reaches EOF, which
// happens when the executor exits and closes its end of the pipe.
func startStderrCapture(r io.Reader) *stderrCapture {
	c := &stderrCapture{done: make(chan struct{})}
	go func() {
		defer close(c.done)
		// A read error ends the capture early. The executor's exit status,
		// not its stderr, decides the run, so the capture keeps what it read.
		_, _ = io.Copy(c, r)
	}()
	return c
}

// Write keeps the last maxStderrBytes bytes written.
func (c *stderrCapture) Write(p []byte) (int, error) {
	n := len(p)
	c.tail = append(c.tail, p...)
	if excess := len(c.tail) - maxStderrBytes; excess > 0 {
		c.dropped += int64(excess)
		c.tail = append(c.tail[:0], c.tail[excess:]...)
	}
	return n, nil
}

// wait blocks until the capture has drained the stream and returns what it
// kept, preceded by a note of the bytes it dropped.
func (c *stderrCapture) wait() []byte {
	<-c.done
	if c.dropped == 0 {
		return c.tail
	}
	note := fmt.Sprintf("[stderr truncated: the first %d bytes were dropped]\n", c.dropped)
	return append([]byte(note), c.tail...)
}
