package runtime

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/pithecene/quarry/iox"
)

// browserReportEnv names the descriptor on which the executor reports a
// per-run browser: "launched <pid>" after the launch and "closed <pid>" once
// the browser's process has exited.
const browserReportEnv = "QUARRY_BROWSER_PID_FD=4"

// browserReports holds the executor's reports of the per-run browsers it
// launched. Puppeteer starts Chromium in a process group of its own, outside
// the executor's, so the runtime stops each such group itself once the
// executor has ended. The reports wait in the pipe until then: a run launches
// one browser, so they never fill it.
type browserReports struct {
	pipe *os.File
}

// newBrowserReports returns the reports and the pipe the executor writes.
func newBrowserReports() (*browserReports, *os.File, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, nil, fmt.Errorf("browser report pipe: %w", err)
	}
	return &browserReports{pipe: r}, w, nil
}

// killOpen sends SIGKILL to the process group of every browser reported
// launched and not closed. The executor must have ended.
func (b *browserReports) killOpen() {
	open := map[int]bool{}
	for line := range strings.Lines(string(b.drain())) {
		event, pidText, _ := strings.Cut(strings.TrimSuffix(line, "\n"), " ")
		pid, err := strconv.Atoi(pidText)
		if err != nil || pid <= 1 {
			continue // kill(-1) would signal every process
		}
		switch event {
		case "launched":
			open[pid] = true
		case "closed":
			delete(open, pid)
		}
	}
	for pid := range open {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}
}

// drain returns what the pipe holds now. A browser that inherited the pipe
// keeps it open, so drain does not wait for the end of the stream.
func (b *browserReports) drain() []byte {
	conn, err := b.pipe.SyscallConn()
	if err != nil {
		return nil
	}
	var reports []byte
	_ = conn.Read(func(fd uintptr) bool {
		reports, _ = io.ReadAll(nonblocking(fd))
		return true
	})
	return reports
}

// nonblocking reads a non-blocking descriptor and ends where it would block.
type nonblocking uintptr

func (fd nonblocking) Read(p []byte) (int, error) {
	for {
		n, err := syscall.Read(int(fd), p)
		switch {
		case errors.Is(err, syscall.EINTR):
			continue
		case err != nil || n <= 0:
			return 0, io.EOF
		}
		return n, nil
	}
}

// close releases the pipe.
func (b *browserReports) close() { iox.DiscardClose(b.pipe) }
