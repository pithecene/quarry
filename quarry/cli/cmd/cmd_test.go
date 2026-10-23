package cmd

import (
	"testing"
)

func TestReadOnlyFlags_IncludesTUI(t *testing.T) {
	t.Parallel()

	flags := ReadOnlyFlags()

	hasTUI := false
	for _, f := range flags {
		if f.Names()[0] == "tui" {
			hasTUI = true
			break
		}
	}

	if !hasTUI {
		t.Error("ReadOnlyFlags should include --tui flag for explicit error handling")
	}
}

func TestTUIReadOnlyFlags_IncludesTUI(t *testing.T) {
	t.Parallel()

	flags := TUIReadOnlyFlags()

	hasTUI := false
	for _, f := range flags {
		if f.Names()[0] == "tui" {
			hasTUI = true
			break
		}
	}

	if !hasTUI {
		t.Error("TUIReadOnlyFlags should include --tui flag")
	}
}

func TestIsStderrTTY(t *testing.T) {
	t.Parallel()

	// This test documents the function exists and can be called.
	// Actual TTY behavior depends on runtime environment.
	_ = isStderrTTY()
}
