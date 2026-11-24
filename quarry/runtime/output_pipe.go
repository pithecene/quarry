package runtime

import (
	"errors"
	"io"
	"os"
	"syscall"
	"time"
)

// outputPipe reads an executor's output. Once ended, it reads what the pipe
// already holds and then reports its end, without waiting for a writer.
type outputPipe struct {
	*os.File
}

// end stops waiting for writers. It wakes a read in progress.
func (p outputPipe) end() { _ = p.SetReadDeadline(time.Unix(1, 0)) }

// Read implements io.Reader.
func (p outputPipe) Read(b []byte) (int, error) {
	n, err := p.File.Read(b)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		return n, err
	}
	conn, err := p.SyscallConn()
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
	case errors.Is(readErr, syscall.EAGAIN) || (readErr == nil && n == 0 && len(b) > 0):
		return 0, io.EOF
	case readErr != nil:
		return 0, readErr
	}
	return n, nil
}
