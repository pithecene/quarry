package lode

import (
	"errors"
	"fmt"
	"io/fs"
	"syscall"
	"testing"
)

func TestClassifyError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		errMsg   string
		wantKind error
	}{
		// write_timeout / timeout
		{
			name:     "context deadline exceeded",
			errMsg:   "context deadline exceeded",
			wantKind: ErrTimeout,
		},
		{
			name:     "operation timed out",
			errMsg:   "operation timed out",
			wantKind: ErrTimeout,
		},
		{
			name:     "timeout in message",
			errMsg:   "connection timeout after 30s",
			wantKind: ErrTimeout,
		},

		// access_denied (before permission_denied in table)
		{
			name:     "AccessDenied response",
			errMsg:   "AccessDenied: you do not have access",
			wantKind: ErrAccessDenied,
		},
		{
			name:     "Forbidden response",
			errMsg:   "Forbidden",
			wantKind: ErrAccessDenied,
		},
		{
			name:     "HTTP 403",
			errMsg:   "received status 403",
			wantKind: ErrAccessDenied,
		},

		// permission_denied
		{
			name:     "permission denied",
			errMsg:   "permission denied for /data/output",
			wantKind: ErrPermissionDenied,
		},
		{
			name:     "EACCES errno",
			errMsg:   "open /tmp/file: EACCES",
			wantKind: ErrPermissionDenied,
		},

		// disk_full
		{
			name:     "no space left on device",
			errMsg:   "write /data/output: no space left on device",
			wantKind: ErrDiskFull,
		},
		{
			name:     "ENOSPC errno",
			errMsg:   "ENOSPC: write failed",
			wantKind: ErrDiskFull,
		},
		{
			name:     "disk full",
			errMsg:   "disk full, cannot write",
			wantKind: ErrDiskFull,
		},
		{
			name:     "quota exceeded",
			errMsg:   "quota exceeded for user",
			wantKind: ErrDiskFull,
		},

		// path_exists (immutability violation)
		{
			name:     "path already exists",
			errMsg:   "path already exists: datasets/quarry/files/report.pdf",
			wantKind: ErrPathExists,
		},
		{
			name:     "file exists errno",
			errMsg:   "open /data/report.pdf: file exists (EEXIST)",
			wantKind: ErrPathExists,
		},

		// not_found
		{
			name:     "no such file",
			errMsg:   "no such file or directory",
			wantKind: ErrNotFound,
		},
		{
			name:     "ENOENT errno",
			errMsg:   "open /missing: ENOENT",
			wantKind: ErrNotFound,
		},
		{
			name:     "NoSuchKey S3",
			errMsg:   "NoSuchKey: The specified key does not exist",
			wantKind: ErrNotFound,
		},
		{
			name:     "HTTP 404",
			errMsg:   "received status 404",
			wantKind: ErrNotFound,
		},

		// rate_limited
		{
			name:     "HTTP 429",
			errMsg:   "received status 429",
			wantKind: ErrThrottled,
		},
		{
			name:     "SlowDown S3",
			errMsg:   "SlowDown: please reduce request rate",
			wantKind: ErrThrottled,
		},
		{
			name:     "TooManyRequests",
			errMsg:   "TooManyRequests: rate limit exceeded",
			wantKind: ErrThrottled,
		},
		{
			name:     "throttled message",
			errMsg:   "request was throttled",
			wantKind: ErrThrottled,
		},

		// auth
		{
			name:     "NoCredentialProviders",
			errMsg:   "NoCredentialProviders: no valid credential providers",
			wantKind: ErrAuth,
		},
		{
			name:     "ExpiredToken",
			errMsg:   "ExpiredToken: the security token has expired",
			wantKind: ErrAuth,
		},
		{
			name:     "HTTP 401",
			errMsg:   "received status 401",
			wantKind: ErrAuth,
		},

		// network
		{
			name:     "connection refused",
			errMsg:   "dial tcp 127.0.0.1:9000: connection refused",
			wantKind: ErrNetwork,
		},
		{
			name:     "no route to host",
			errMsg:   "no route to host",
			wantKind: ErrNetwork,
		},
		{
			name:     "DNS resolution failure",
			errMsg:   "DNS lookup failed for bucket.s3.amazonaws.com",
			wantKind: ErrNetwork,
		},

		// unknown (fallback)
		{
			name:   "unrecognized error",
			errMsg: "something completely unexpected happened",
			// classifyError returns a new errors.New("storage error") for unknown
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			got := classifyError(err)

			if tt.wantKind != nil {
				if !errors.Is(got, tt.wantKind) {
					t.Errorf("classifyError(%q) = %v, want %v", tt.errMsg, got, tt.wantKind)
				}
			} else {
				// Fallback: should return an error with "storage error" message
				if got == nil {
					t.Errorf("classifyError(%q) = nil, want non-nil fallback", tt.errMsg)
				} else if got.Error() != "storage error" {
					t.Errorf("classifyError(%q) = %q, want %q", tt.errMsg, got.Error(), "storage error")
				}
			}
		})
	}
}

// statusError mimics an AWS SDK response error: an HTTP status exposed via
// HTTPStatusCode, with a message that need not agree with it.
type statusError struct {
	status int
	msg    string
}

func (e statusError) Error() string       { return e.msg }
func (e statusError) HTTPStatusCode() int { return e.status }

// Typed errors decide the kind even when the message (which embeds paths and
// keys) matches a different pattern.
func TestClassifyError_TypedBeforeMessage(t *testing.T) {
	t.Parallel()

	pathErr := func(op, path string, errno syscall.Errno) error {
		return fmt.Errorf("lode: fs put: %w", &fs.PathError{Op: op, Path: path, Err: errno})
	}
	tests := []struct {
		name     string
		err      error
		wantKind error
	}{
		{"EACCES under a path containing 403", pathErr("mkdir", "/tmp/TestX1403/001/readonly/data", syscall.EACCES), ErrPermissionDenied},
		{"ENOENT under a path containing 429", pathErr("open", "/tmp/run-4291/events", syscall.ENOENT), ErrNotFound},
		{"EEXIST under a path containing 404", pathErr("link", "/data/run-404/file", syscall.EEXIST), ErrPathExists},
		{"ENOSPC under a path containing 401", pathErr("write", "/data/run-401/file", syscall.ENOSPC), ErrDiskFull},
		{"HTTP 403 whose message says not found", statusError{403, "get runs/404/x: not found"}, ErrAccessDenied},
		{"HTTP 404 whose key contains 403", statusError{404, "get runs/403/x: api error"}, ErrNotFound},
		{"unmapped HTTP status falls back to the message", statusError{503, "SlowDown: please reduce request rate"}, ErrThrottled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyError(tt.err); !errors.Is(got, tt.wantKind) {
				t.Errorf("classifyError(%v) = %v, want %v", tt.err, got, tt.wantKind)
			}
		})
	}
}

func TestClassifyError_Nil(t *testing.T) {
	t.Parallel()

	got := classifyError(nil)
	if got != nil {
		t.Errorf("classifyError(nil) = %v, want nil", got)
	}
}
