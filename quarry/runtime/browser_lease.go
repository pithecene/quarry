package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"syscall"
	"time"

	"github.com/pithecene/quarry/iox"
)

// leaseSocketName is the browser server's Unix socket in the discovery
// directory. A run holds one connection to it for as long as the run lasts.
const leaseSocketName = "server.sock"

// leaseReplyTimeout bounds the wait for a server's reply. A server that does
// not reply in time is not shared, and is left alone.
const leaseReplyTimeout = 5 * time.Second

// maxSocketPathLen is the longest Unix socket path every supported platform
// accepts: macOS allows 103 bytes, Linux 107.
const maxSocketPathLen = 103

var (
	// errNoServer means no server is listening: the socket is missing,
	// refuses the connection, or was closed without a reply, as by a server
	// that stopped listening while the connection waited.
	errNoServer = errors.New("no browser server is listening")

	errProxyMismatch = errors.New("proxy mismatch")
)

// leaseRequest and leaseReply are the one exchange on a lease connection,
// each a line of JSON. executor-node/src/browser-lease-server.ts is the
// other side.
type leaseRequest struct {
	ProxyHash string `json:"proxy_hash"`
}

type leaseReply struct {
	WSEndpoint string `json:"ws_endpoint"`
	PID        int    `json:"pid"`
	Refused    string `json:"refused"`
}

// browserLease is a granted lease: the browser's endpoint, held until the
// context it was requested with is done.
type browserLease struct {
	WSEndpoint string
	ServerPID  int
	// Release ends the lease before its context does.
	Release func()
}

// requestLease connects to the server at socketPath and asks for its
// browser. On a grant, the connection stays open until ctx is done, which
// is what keeps the server alive for the run; the operating system closes
// it if the process ends first. Cancelling ctx also ends a wait for the
// reply. Failures that show no server is listening
// wrap errNoServer.
func requestLease(ctx context.Context, socketPath, proxyHash string) (browserLease, error) {
	if len(socketPath) > maxSocketPathLen {
		return browserLease{}, fmt.Errorf("socket path is %d bytes, over the %d-byte limit: %s", len(socketPath), maxSocketPathLen, socketPath)
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
			return browserLease{}, fmt.Errorf("%w: %w", errNoServer, err)
		}
		return browserLease{}, fmt.Errorf("connect: %w", err)
	}

	// The connection lives exactly as long as ctx: closing it when ctx ends
	// both releases a granted lease and cuts short a stalled exchange.
	stop := context.AfterFunc(ctx, func() { iox.DiscardClose(conn) })
	release := func() {
		stop()
		iox.DiscardClose(conn)
	}
	lease, err := exchange(conn, proxyHash)
	if err != nil {
		release()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return browserLease{}, ctxErr
		}
		return browserLease{}, err
	}
	lease.Release = release
	return lease, nil
}

// exchange sends the request and reads the one reply.
func exchange(conn net.Conn, proxyHash string) (browserLease, error) {
	if err := conn.SetDeadline(time.Now().Add(leaseReplyTimeout)); err != nil {
		return browserLease{}, fmt.Errorf("set deadline: %w", err)
	}
	if err := json.NewEncoder(conn).Encode(leaseRequest{ProxyHash: proxyHash}); err != nil {
		return browserLease{}, classifyExchangeError("send request", err)
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return browserLease{}, classifyExchangeError("read reply", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return browserLease{}, fmt.Errorf("clear deadline: %w", err)
	}

	var reply leaseReply
	if err := json.Unmarshal(line, &reply); err != nil {
		return browserLease{}, fmt.Errorf("parse reply: %w", err)
	}
	switch {
	case reply.Refused == errProxyMismatch.Error():
		return browserLease{}, errProxyMismatch
	case reply.Refused != "":
		return browserLease{}, fmt.Errorf("server refused the lease: %s", reply.Refused)
	case reply.WSEndpoint == "":
		return browserLease{}, errors.New("server reply has no endpoint")
	}
	return browserLease{WSEndpoint: reply.WSEndpoint, ServerPID: reply.PID}, nil
}

// classifyExchangeError reports a connection closed without a reply as
// errNoServer: a server closes its listener, resetting connections still
// waiting to be accepted, only as it shuts down. A timeout or any other
// error leaves the server's state unknown.
func classifyExchangeError(step string, err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return fmt.Errorf("%w: %s: %w", errNoServer, step, err)
	}
	if isTimeout(err) {
		return fmt.Errorf("%s: no reply within %s", step, leaseReplyTimeout)
	}
	return fmt.Errorf("%s: %w", step, err)
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
