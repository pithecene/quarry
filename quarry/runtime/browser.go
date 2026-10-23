package runtime

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/pithecene/quarry/iox"
)

// ManagedBrowser represents a Quarry-managed browser process.
// Used by fan-out to share a single browser across all child executor runs.
type ManagedBrowser struct {
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	WSEndpoint string
}

// LaunchManagedBrowser starts a shared browser via the executor's --launch-browser mode.
// The executor resolves puppeteer from the script's directory and launches Chrome.
// The WS endpoint is read from the executor's stdout (first line).
// The browser stays alive until Close() is called.
func LaunchManagedBrowser(ctx context.Context, executorPath, scriptPath string) (*ManagedBrowser, error) {
	cmd := exec.CommandContext(ctx, executorPath, "--launch-browser", scriptPath)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	// stdin pipe kept open — closing it signals the browser server to shut down
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start browser server: %w", err)
	}

	// Read the WS endpoint URL from stdout (first line)
	scanner := bufio.NewScanner(stdout)

	// Use a timeout channel — if the browser doesn't print the WS URL within 30s, bail
	wsURLCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		if scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.HasPrefix(line, "ws://") || strings.HasPrefix(line, "wss://") {
				wsURLCh <- line
				return
			}
			errCh <- fmt.Errorf("unexpected browser server output: %q", line)
			return
		}
		if err := scanner.Err(); err != nil {
			errCh <- fmt.Errorf("reading browser server stdout: %w", err)
			return
		}
		errCh <- errors.New("browser server exited without printing WS endpoint")
	}()

	select {
	case wsURL := <-wsURLCh:
		return &ManagedBrowser{
			cmd:        cmd,
			stdin:      stdin,
			WSEndpoint: wsURL,
		}, nil
	case err := <-errCh:
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, err
	case <-time.After(30 * time.Second):
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, errors.New("timed out waiting for browser server WS endpoint")
	case <-ctx.Done():
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, ctx.Err()
	}
}

// Close shuts down the managed browser by closing stdin (signaling the
// browser server to exit) and then waiting for the process.
func (mb *ManagedBrowser) Close() error {
	if mb.cmd == nil || mb.cmd.Process == nil {
		return nil
	}

	// Close stdin to signal shutdown
	iox.DiscardClose(mb.stdin)

	// Give the browser server a few seconds to shut down gracefully
	done := make(chan struct{})
	go func() {
		// Shutdown reports no exit status: the server exits non-zero when
		// stdin closes or when it is killed below, and neither is a failure.
		_ = mb.cmd.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(5 * time.Second):
		// Force kill if graceful shutdown timed out
		_ = mb.cmd.Process.Kill()
		<-done
		return nil
	}
}
