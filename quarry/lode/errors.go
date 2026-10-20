// Package lode provides storage error classification for Quarry.
//
// This file defines sentinel errors and error wrappers for classifying
// storage failures. These enable callers to use errors.Is/errors.As
// for typed assertions rather than string matching.
package lode

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"syscall"
)

// Sentinel errors for storage failure classification.
// Use errors.Is(err, ErrXxx) for typed assertions.
var (
	// ErrPermissionDenied indicates a permission/access failure (EACCES, 403).
	ErrPermissionDenied = errors.New("permission denied")

	// ErrNotFound indicates the target path/resource does not exist (ENOENT, 404).
	ErrNotFound = errors.New("not found")

	// ErrDiskFull indicates storage is out of space (ENOSPC).
	ErrDiskFull = errors.New("no space left on device")

	// ErrTimeout indicates an operation timed out.
	ErrTimeout = errors.New("operation timed out")

	// ErrThrottled indicates rate limiting (429, SlowDown).
	ErrThrottled = errors.New("rate limited")

	// ErrAuth indicates authentication failure (no credentials, expired token).
	ErrAuth = errors.New("authentication failed")

	// ErrAccessDenied indicates authorization failure (valid creds but no permission).
	ErrAccessDenied = errors.New("access denied")

	// ErrNetwork indicates a network-level failure (connection refused, DNS).
	ErrNetwork = errors.New("network error")

	// ErrPathExists indicates a write to an existing immutable path.
	ErrPathExists = errors.New("path already exists")
)

// StorageError wraps an underlying error with storage classification.
// It preserves the original error in the chain for inspection via errors.As.
type StorageError struct {
	// Kind is the sentinel error for classification (e.g., ErrPermissionDenied).
	Kind error
	// Op is the operation that failed (e.g., "write", "read", "list").
	Op string
	// Path is the storage path involved, if any.
	Path string
	// Err is the underlying error.
	Err error
}

func (e *StorageError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("%s %s: %v: %v", e.Op, e.Path, e.Kind, e.Err)
	}
	return fmt.Sprintf("%s: %v: %v", e.Op, e.Kind, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As chain traversal.
func (e *StorageError) Unwrap() error {
	return e.Err
}

// Is reports whether the error matches the target sentinel.
func (e *StorageError) Is(target error) bool {
	return errors.Is(e.Kind, target)
}

// NewStorageError creates a classified storage error.
func NewStorageError(kind error, op, path string, err error) *StorageError {
	return &StorageError{
		Kind: kind,
		Op:   op,
		Path: path,
		Err:  err,
	}
}

// WrapWriteError classifies and wraps a write operation error.
// Returns nil if err is nil.
func WrapWriteError(err error, path string) error {
	if err == nil {
		return nil
	}
	kind := classifyError(err)
	return NewStorageError(kind, "write", path, err)
}

// WrapReadError classifies and wraps a read operation error.
// Returns nil if err is nil.
func WrapReadError(err error, path string) error {
	if err == nil {
		return nil
	}
	kind := classifyError(err)
	return NewStorageError(kind, "read", path, err)
}

// WrapInitError classifies and wraps a client initialization error.
// Returns nil if err is nil.
func WrapInitError(err error, dataset string) error {
	if err == nil {
		return nil
	}
	kind := classifyError(err)
	return NewStorageError(kind, "init", dataset, err)
}

// errorPattern pairs a set of message substrings with a sentinel error.
// Order matters: more-specific patterns must appear before general ones.
type errorPattern struct {
	patterns []string
	kind     error
}

// typedClassifierTable maps standard error values to sentinels. Typed errors
// are authoritative and are checked before any message matching: an error
// message embeds paths and keys, whose digits and words can otherwise match a
// message pattern (a temp dir containing "403" read as an S3 403).
var typedClassifierTable = []struct {
	target error
	kind   error
}{
	{fs.ErrPermission, ErrPermissionDenied},
	{fs.ErrNotExist, ErrNotFound},
	{fs.ErrExist, ErrPathExists},
	{syscall.ENOSPC, ErrDiskFull},
}

// httpStatusKinds maps HTTP response statuses (exposed by AWS SDK response
// errors via HTTPStatusCode) to sentinels.
var httpStatusKinds = map[int]error{
	401: ErrAuth,
	403: ErrAccessDenied,
	404: ErrNotFound,
	429: ErrThrottled,
}

// classifierTable is a declarative list of error message patterns, used for
// errors that carry no typed classification.
// Entries are checked in order; the first match wins.
// ErrAccessDenied appears before ErrPermissionDenied so that
// "AccessDenied"/"Forbidden"/"403" is not shadowed by "access denied".
var classifierTable = []errorPattern{
	{[]string{"AccessDenied", "access denied", "Forbidden", "403"}, ErrAccessDenied},
	{[]string{"permission denied", "EACCES"}, ErrPermissionDenied},
	{[]string{"path already exists", "path exists", "already exists", "file exists", "EEXIST"}, ErrPathExists},
	{[]string{"no such file", "does not exist", "not found", "ENOENT", "404", "NoSuchKey"}, ErrNotFound},
	{[]string{"no space left", "disk full", "ENOSPC", "quota exceeded"}, ErrDiskFull},
	{[]string{"timeout", "timed out", "deadline exceeded"}, ErrTimeout},
	{[]string{"SlowDown", "rate exceeded", "throttl", "429", "TooManyRequests"}, ErrThrottled},
	{[]string{"NoCredentialProviders", "credentials", "InvalidAccessKeyId",
		"SignatureDoesNotMatch", "ExpiredToken", "401", "Unauthorized"}, ErrAuth},
	{[]string{"connection refused", "no route to host", "network unreachable",
		"DNS", "dial tcp", "i/o timeout"}, ErrNetwork},
}

// classifyError determines the appropriate sentinel error for the given error.
// Classification checks typed errors first, then walks the classifier table.
func classifyError(err error) error {
	if err == nil {
		return nil
	}
	if kind := classifyTyped(err); kind != nil {
		return kind
	}

	// Walk the classifier table (first match wins)
	errStr := err.Error()
	for _, entry := range classifierTable {
		if containsAny(errStr, entry.patterns...) {
			return entry.kind
		}
	}

	return errors.New("storage error")
}

// classifyTyped returns the sentinel for err's typed chain, or nil if no
// typed error in the chain is recognized.
func classifyTyped(err error) error {
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return ErrTimeout
	}
	for _, entry := range typedClassifierTable {
		if errors.Is(err, entry.target) {
			return entry.kind
		}
	}
	var statusErr interface{ HTTPStatusCode() int }
	if errors.As(err, &statusErr) {
		return httpStatusKinds[statusErr.HTTPStatusCode()]
	}
	return nil
}

// containsAny checks if s contains any of the substrings (case-insensitive).
func containsAny(s string, substrs ...string) bool {
	lower := strings.ToLower(s)
	for _, sub := range substrs {
		if strings.Contains(lower, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}
