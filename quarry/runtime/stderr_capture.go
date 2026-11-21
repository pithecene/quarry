package runtime

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

// maxStderrBytes bounds the executor stderr that a run keeps. The tail is
// kept, since an executor reports the failure that ends it last.
const maxStderrBytes = 1 << 20

// stderrCapture drains an executor's stderr while the run ingests stdout, so
// that an executor that writes more stderr than the pipe holds never blocks.
// It keeps the last maxStderrBytes bytes and counts the bytes it dropped.
type stderrCapture struct {
	mu         sync.Mutex
	ready      *sync.Cond
	tail       []byte
	dropped    int64
	readOffset int64
	readErr    error
}

// startStderrCapture drains r on its own goroutine until r reaches EOF, which
// happens when the executor exits and closes its end of the pipe.
func startStderrCapture(r io.Reader) *stderrCapture {
	c := &stderrCapture{}
	c.ready = sync.NewCond(&c.mu)
	go func() {
		_, err := io.Copy(c, r)
		if err == nil {
			err = io.EOF
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.readErr = err
		c.ready.Broadcast()
	}()
	return c
}

// Write keeps the last maxStderrBytes bytes written.
func (c *stderrCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := len(p)
	c.tail = append(c.tail, p...)
	if excess := len(c.tail) - maxStderrBytes; excess > 0 {
		c.dropped += int64(excess)
		c.tail = append(c.tail[:0], c.tail[excess:]...)
	}
	c.ready.Broadcast()
	return n, nil
}

// stderrReader exposes the captured bytes without exposing the capture writer.
type stderrReader struct {
	capture *stderrCapture
}

func (r stderrReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c := r.capture
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.readOffset >= c.dropped+int64(len(c.tail)) && c.readErr == nil {
		c.ready.Wait()
	}
	c.readOffset = max(c.readOffset, c.dropped)
	n := copy(p, c.tail[c.readOffset-c.dropped:])
	c.readOffset += int64(n)
	if n == 0 {
		return 0, c.readErr
	}
	return n, nil
}

// wait blocks until the capture has drained the stream and returns what it
// kept, preceded by a note of the bytes it dropped.
func (c *stderrCapture) wait() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.readErr == nil {
		c.ready.Wait()
	}
	// Give Wait callers their own bytes, since Stderr can still be read.
	if c.dropped == 0 {
		return bytes.Clone(c.tail)
	}
	note := fmt.Sprintf("[stderr truncated: the first %d bytes were dropped]\n", c.dropped)
	return append([]byte(note), c.tail...)
}
