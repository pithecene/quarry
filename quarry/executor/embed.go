// Package executor provides embedded executor management.
//
// The executor bundle is embedded at build time and extracted to a
// temporary directory on first use. This allows the quarry binary to
// be self-contained without requiring a separate executor installation.
package executor

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pithecene/quarry/types"
)

//go:embed bundle/executor.mjs
var embeddedExecutor []byte

// extractOnce ensures extraction happens only once per process.
var extractOnce sync.Once
var extractedPath string
var extractErr error

// EmbeddedVersion returns the version of the embedded executor.
// This should match types.Version for lockstep validation.
func EmbeddedVersion() string {
	return types.Version
}

// EmbeddedSize returns the size of the embedded executor in bytes.
func EmbeddedSize() int {
	return len(embeddedExecutor)
}

// EmbeddedChecksum returns the SHA256 checksum of the embedded executor.
func EmbeddedChecksum() string {
	hash := sha256.Sum256(embeddedExecutor)
	return hex.EncodeToString(hash[:])
}

// IsEmbedded returns true if an executor is embedded in this binary.
func IsEmbedded() bool {
	return len(embeddedExecutor) > 0
}

// ExtractedPath returns the path to the extracted executor.
// Extracts on first call; subsequent calls return cached path.
// Returns error if extraction fails.
func ExtractedPath() (string, error) {
	extractOnce.Do(func() {
		extractedPath, extractErr = extractExecutor()
	})
	return extractedPath, extractErr
}

// extractExecutor extracts the embedded executor to a temp directory.
func extractExecutor() (string, error) {
	if !IsEmbedded() {
		return "", errors.New("no embedded executor available")
	}

	// Create quarry-specific temp directory
	// Use a hash-based name to allow multiple versions to coexist
	checksum := EmbeddedChecksum()[:16] // First 16 chars of SHA256
	dirName := fmt.Sprintf("quarry-executor-%s-%s", types.Version, checksum)
	tempDir := filepath.Join(os.TempDir(), dirName)

	executorPath := filepath.Join(tempDir, "executor.mjs")

	// Check if already extracted (idempotent)
	if info, err := os.Stat(executorPath); err == nil && info.Size() == int64(len(embeddedExecutor)) {
		return executorPath, nil
	}

	// Create directory
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Write executor file
	if err := os.WriteFile(executorPath, embeddedExecutor, 0o755); err != nil {
		return "", fmt.Errorf("failed to write executor: %w", err)
	}

	return executorPath, nil
}

// Cleanup removes the extracted executor directory.
// Safe to call multiple times or if extraction never happened.
func Cleanup() error {
	if extractedPath == "" {
		return nil
	}

	dir := filepath.Dir(extractedPath)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("failed to cleanup executor: %w", err)
	}

	return nil
}
