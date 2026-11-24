package runtime

import (
	"errors"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/pithecene/quarry/iox"
)

// drainLimit is the most that an ended pipe still gives: what a pipe holds by
// default on Linux and Darwin, so a writer that remains cannot keep it going.
const drainLimit = 64 << 10

// outputPipe reads an executor's output. Once ended, it reads up to drainLimit
// of what the pipe holds and then reports its end, without waiting for a
// writer. It has one reader. It does not embed its file, whose WriteTo io.Copy
// would call in place of Read.
type outputPipe struct {
	file    *os.File
	drained int
}

// newOutputPipe returns the pipe and the end that the executor writes. It
// refuses a pipe whose reads cannot be woken.
func newOutputPipe() (*outputPipe, *os.File, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	if err := r.SetReadDeadline(time.Time{}); err != nil {
		iox.DiscardClose(r)
		iox.DiscardClose(w)
		return nil, nil, err
	}
	return &outputPipe{file: r}, w, nil
}

// Close implements io.Closer.
func (p *outputPipe) Close() error { return p.file.Close() }

// end stops waiting for writers. It wakes a read in progress.
func (p *outputPipe) end() error { return p.file.SetReadDeadline(time.Unix(1, 0)) }

// Read implements io.Reader.
func (p *outputPipe) Read(b []byte) (int, error) {
	n, err := p.file.Read(b)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		return n, err
	}
	b = b[:min(len(b), drainLimit-p.drained)]
	if len(b) == 0 {
		return 0, io.EOF
	}
	conn, err := p.file.SyscallConn()
	if err != nil {
		return 0, err
	}
	// The descriptor does not block, and Control ignores the deadline.
	var readErr error
	if err := conn.Control(func(fd uintptr) {
		for {
			n, readErr = syscall.Read(int(fd), b)
			if !errors.Is(readErr, syscall.EINTR) {
				return
			}
		}
	}); err != nil {
		return 0, err
	}
	switch {
	case errors.Is(readErr, syscall.EAGAIN) || (readErr == nil && n == 0):
		return 0, io.EOF
	case readErr != nil:
		return 0, readErr
	}
	p.drained += n
	return n, nil
}
