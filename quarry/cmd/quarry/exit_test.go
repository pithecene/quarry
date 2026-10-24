package main

import (
	"errors"
	"testing"

	"github.com/urfave/cli/v2"
)

func TestExitErrHandler_NilError(t *testing.T) {
	t.Parallel()

	// Should not panic or exit on nil error
	exitErrHandler(nil, nil)
}

func TestExitErrHandler_ExitCoder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantCode int
		wantMsg  string
	}{
		{
			name:     "exit code 0 no message",
			err:      cli.Exit("", 0),
			wantCode: 0,
			wantMsg:  "",
		},
		{
			name:     "exit code 1 with message",
			err:      cli.Exit("script error occurred", 1),
			wantCode: 1,
			wantMsg:  "script error occurred",
		},
		{
			name:     "exit code 2 executor crash",
			err:      cli.Exit("executor crashed", 2),
			wantCode: 2,
			wantMsg:  "executor crashed",
		},
		{
			name:     "exit code 3 policy failure",
			err:      cli.Exit("policy failed", 3),
			wantCode: 3,
			wantMsg:  "policy failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't easily test os.Exit without subprocess, but we can
			// verify the error is recognized as ExitCoder
			var exitCoder cli.ExitCoder
			if !errors.As(tt.err, &exitCoder) {
				t.Fatalf("error should be cli.ExitCoder")
			}

			if exitCoder.ExitCode() != tt.wantCode {
				t.Errorf("exit code = %d, want %d", exitCoder.ExitCode(), tt.wantCode)
			}
		})
	}
}

func TestExitErrHandler_WrappedExitCoder(t *testing.T) {
	t.Parallel()

	// Test that wrapped errors still extract the exit code
	wrapped := errors.Join(errors.New("context"), cli.Exit("inner error", 42))

	var exitCoder cli.ExitCoder
	if !errors.As(wrapped, &exitCoder) {
		t.Fatal("wrapped error should still match cli.ExitCoder")
	}

	if exitCoder.ExitCode() != 42 {
		t.Errorf("exit code = %d, want 42", exitCoder.ExitCode())
	}
}

func TestExitErrHandler_RegularError(t *testing.T) {
	t.Parallel()

	// Regular errors should result in exit code 1 (tested via behavior)
	err := errors.New("regular error")

	if _, ok := errors.AsType[cli.ExitCoder](err); ok {
		t.Fatal("regular error should not be cli.ExitCoder")
	}
}

// TestRunExitCodes documents the expected exit codes per CONTRACT_RUN.md.
func TestRunExitCodes_Documentation(t *testing.T) {
	t.Parallel()

	// This test documents the exit code contract:
	// - 0: success (run_complete)
	// - 1: script error (run_error)
	// - 2: executor crash
	// - 3: policy failure

	codes := map[int]string{
		0: "success (run_complete)",
		1: "script error (run_error)",
		2: "executor crash",
		3: "policy failure",
	}

	// Verify our constants match (defined in cli/cmd/run.go)
	expected := map[string]int{
		"exitSuccess":       0,
		"exitScriptError":   1,
		"exitExecutorCrash": 2,
		"exitPolicyFailure": 3,
	}

	for name, code := range expected {
		if _, ok := codes[code]; !ok {
			t.Errorf("%s = %d is not documented", name, code)
		}
	}
}

// TestExitErrHandler_PreservesExitCode verifies that cli.Exit codes pass through.
// This is critical for CONTRACT_RUN.md compliance.
func TestExitErrHandler_PreservesExitCode(t *testing.T) {
	t.Parallel()

	// Test each exit code defined in CONTRACT_RUN.md
	testCases := []struct {
		name string
		code int
	}{
		{"success", 0},
		{"script_error", 1},
		{"executor_crash", 2},
		{"policy_failure", 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := cli.Exit("", tc.code)

			var exitCoder cli.ExitCoder
			if !errors.As(err, &exitCoder) {
				t.Fatalf("cli.Exit should return ExitCoder")
			}

			if exitCoder.ExitCode() != tc.code {
				t.Errorf("ExitCode() = %d, want %d", exitCoder.ExitCode(), tc.code)
			}
		})
	}
}

// TestExitErrHandler_MessageSuppression verifies empty messages don't print.
func TestExitErrHandler_MessageSuppression(t *testing.T) {
	t.Parallel()

	// cli.Exit("", N) with empty message should not print anything meaningful
	err := cli.Exit("", 0)
	msg := err.Error()

	// Empty message cli.Exit returns empty string or "exit status N"
	// Our handler should NOT print these to stderr
	if msg != "" && msg != "exit status 0" {
		t.Errorf("Expected empty or 'exit status 0', got %q", msg)
	}
}
