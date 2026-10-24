package cmd

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/quarry/internal/testutil"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/adapter/redisstream"
	quarryconfig "github.com/pithecene/quarry/cli/config"
	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/runtime"
	"github.com/pithecene/quarry/types"
)

func TestValidatePolicyConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		choice      policyChoice
		wantErr     bool
		errContains string
	}{
		{
			name:    "strict policy valid",
			choice:  policyChoice{name: "strict", flushMode: "at_least_once"},
			wantErr: false,
		},
		{
			name:    "buffered with events limit valid",
			choice:  policyChoice{name: "buffered", flushMode: "at_least_once", maxEvents: 1000},
			wantErr: false,
		},
		{
			name:    "buffered with bytes limit valid",
			choice:  policyChoice{name: "buffered", flushMode: "at_least_once", maxBytes: 1048576},
			wantErr: false,
		},
		{
			name:        "buffered without limits invalid",
			choice:      policyChoice{name: "buffered", flushMode: "at_least_once"},
			wantErr:     true,
			errContains: "buffer limits",
		},
		{
			name:        "invalid policy name",
			choice:      policyChoice{name: "invalid"},
			wantErr:     true,
			errContains: "invalid --policy",
		},
		{
			name:        "invalid flush mode",
			choice:      policyChoice{name: "buffered", flushMode: "invalid", maxEvents: 100},
			wantErr:     true,
			errContains: "invalid --flush-mode",
		},
		{
			name:    "buffered with chunks_first valid",
			choice:  policyChoice{name: "buffered", flushMode: "chunks_first", maxEvents: 100},
			wantErr: false,
		},
		{
			name:    "buffered with two_phase valid",
			choice:  policyChoice{name: "buffered", flushMode: "two_phase", maxBytes: 1000},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePolicyConfig(tt.choice)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateStorageConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      storageChoice
		wantErr     bool
		errContains string
	}{
		{
			name:    "fs with valid directory",
			config:  storageChoice{backend: "fs", path: "/tmp"},
			wantErr: false,
		},
		{
			name:        "fs with nonexistent path",
			config:      storageChoice{backend: "fs", path: "/nonexistent/path/that/does/not/exist"},
			wantErr:     true,
			errContains: "does not exist",
		},
		{
			name:        "fs with file instead of directory",
			config:      storageChoice{backend: "fs", path: "/etc/passwd"},
			wantErr:     true,
			errContains: "not a directory",
		},
		{
			name:    "s3 with path",
			config:  storageChoice{backend: "s3", path: "my-bucket/prefix"},
			wantErr: false,
		},
		{
			name:        "s3 without path",
			config:      storageChoice{backend: "s3", path: ""},
			wantErr:     true,
			errContains: "--storage-path required",
		},
		{
			name:        "invalid backend",
			config:      storageChoice{backend: "invalid", path: "/tmp"},
			wantErr:     true,
			errContains: "invalid --storage-backend",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateStorageConfig(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestErrorMessagesAreActionable(t *testing.T) {
	t.Parallel()

	// Test that error messages include actionable guidance
	tests := []struct {
		name        string
		config      storageChoice
		mustContain []string
		description string
	}{
		{
			name:        "nonexistent path suggests mkdir",
			config:      storageChoice{backend: "fs", path: "/nonexistent/test/path"},
			mustContain: []string{"mkdir -p"},
			description: "should suggest creating directory",
		},
		{
			name:        "s3 missing path explains format",
			config:      storageChoice{backend: "s3", path: ""},
			mustContain: []string{"bucket-name", "Format:"},
			description: "should explain S3 path format",
		},
		{
			name:        "invalid backend lists options",
			config:      storageChoice{backend: "gcs", path: "/tmp"},
			mustContain: []string{"fs", "s3", "Valid options"},
			description: "should list valid storage backends",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateStorageConfig(tt.config)
			if err == nil {
				t.Fatal("expected error")
			}
			errMsg := err.Error()
			for _, must := range tt.mustContain {
				if !strings.Contains(errMsg, must) {
					t.Errorf("%s: error message should contain %q for actionability\nGot: %s",
						tt.description, must, errMsg)
				}
			}
		})
	}
}

func TestPolicyErrorMessagesAreActionable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		choice      policyChoice
		mustContain []string
		description string
	}{
		{
			name:        "buffered without limits suggests flags",
			choice:      policyChoice{name: "buffered", flushMode: "at_least_once"},
			mustContain: []string{"--buffer-events", "--buffer-bytes"},
			description: "should suggest buffer limit flags",
		},
		{
			name:        "invalid policy lists options",
			choice:      policyChoice{name: "unknown"},
			mustContain: []string{"strict", "buffered", "Valid options"},
			description: "should list valid policies",
		},
		{
			name:        "invalid flush mode lists options",
			choice:      policyChoice{name: "buffered", flushMode: "unknown", maxEvents: 100},
			mustContain: []string{"at_least_once", "chunks_first", "two_phase"},
			description: "should list valid flush modes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePolicyConfig(tt.choice)
			if err == nil {
				t.Fatal("expected error")
			}
			errMsg := err.Error()
			for _, must := range tt.mustContain {
				if !strings.Contains(errMsg, must) {
					t.Errorf("%s: error message should contain %q for actionability\nGot: %s",
						tt.description, must, errMsg)
				}
			}
		})
	}
}

func TestResolveExecutor(t *testing.T) {
	t.Parallel()

	// Create a temp file to use as mock executor
	tmpDir := t.TempDir()
	mockExecutor := filepath.Join(tmpDir, "mock-executor.js")
	if err := os.WriteFile(mockExecutor, []byte("// mock"), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Run("explicit path found", func(t *testing.T) {
		path, err := resolveExecutor(mockExecutor)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if path != mockExecutor {
			t.Errorf("got path %q, want %q", path, mockExecutor)
		}
	})

	t.Run("explicit path not found", func(t *testing.T) {
		_, err := resolveExecutor("/nonexistent/executor.js")
		if err == nil {
			t.Error("expected error, got nil")
		} else if !strings.Contains(err.Error(), "executor not found") {
			t.Errorf("error %q should contain %q", err.Error(), "executor not found")
		}
	})

	t.Run("auto-resolution returns valid path or error", func(t *testing.T) {
		// This test verifies the auto-resolution behavior without being environment-dependent.
		// It succeeds if either: (1) a valid path is returned, or (2) an actionable error is returned.
		path, err := resolveExecutor("")
		if err != nil {
			// Verify error is actionable
			if !strings.Contains(err.Error(), "executor not found") {
				t.Errorf("error should mention 'executor not found', got: %v", err)
			}
			if !strings.Contains(err.Error(), "pnpm") {
				t.Errorf("error should include build instructions, got: %v", err)
			}
		} else {
			// Verify returned path exists
			if _, statErr := os.Stat(path); statErr != nil {
				t.Errorf("resolved path %q does not exist: %v", path, statErr)
			}
		}
	})
}

func TestResolveExecutorErrorIsActionable(t *testing.T) {
	t.Parallel()

	// Test that executor not found error includes actionable guidance
	_, err := resolveExecutor("")
	if err == nil {
		t.Skip("executor found in environment, cannot test not-found error")
	}

	errMsg := err.Error()
	mustContain := []string{
		"executor not found",
		"pnpm",       // build instruction
		"--executor", // manual override option
	}

	for _, must := range mustContain {
		if !strings.Contains(errMsg, must) {
			t.Errorf("error should contain %q for actionability\nGot: %s", must, errMsg)
		}
	}
}

func TestParseJobPayload(t *testing.T) {
	t.Parallel()

	t.Run("neither flag returns empty object", func(t *testing.T) {
		job, err := parseJobPayload("", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(job) != 0 {
			t.Errorf("expected empty map, got %v", job)
		}
	})

	t.Run("inline object accepted", func(t *testing.T) {
		job, err := parseJobPayload(`{"url": "https://example.com", "page": 1}`, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if job["url"] != "https://example.com" {
			t.Errorf("expected url=https://example.com, got %v", job["url"])
		}
	})

	t.Run("file object accepted", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "job.json")
		if err := os.WriteFile(tmpFile, []byte(`{"target": "test"}`), 0o644); err != nil {
			t.Fatal(err)
		}

		job, err := parseJobPayload("", tmpFile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if job["target"] != "test" {
			t.Errorf("expected target=test, got %v", job["target"])
		}
	})

	t.Run("inline array rejected", func(t *testing.T) {
		_, err := parseJobPayload(`[1, 2, 3]`, "")
		if err == nil {
			t.Fatal("expected error for array payload")
		}
		if !strings.Contains(err.Error(), "must be a JSON object") {
			t.Errorf("error should mention 'must be a JSON object', got: %v", err)
		}
		if !strings.Contains(err.Error(), "array") {
			t.Errorf("error should mention 'array', got: %v", err)
		}
	})

	t.Run("file array rejected", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "array.json")
		if err := os.WriteFile(tmpFile, []byte(`["item1", "item2"]`), 0o644); err != nil {
			t.Fatal(err)
		}

		_, err := parseJobPayload("", tmpFile)
		if err == nil {
			t.Fatal("expected error for array payload")
		}
		if !strings.Contains(err.Error(), "must contain a JSON object") {
			t.Errorf("error should mention 'must contain a JSON object', got: %v", err)
		}
		if !strings.Contains(err.Error(), "array") {
			t.Errorf("error should mention 'array', got: %v", err)
		}
	})

	t.Run("inline primitive string rejected", func(t *testing.T) {
		_, err := parseJobPayload(`"just a string"`, "")
		if err == nil {
			t.Fatal("expected error for primitive payload")
		}
		if !strings.Contains(err.Error(), "must be a JSON object") {
			t.Errorf("error should mention 'must be a JSON object', got: %v", err)
		}
		if !strings.Contains(err.Error(), "string") {
			t.Errorf("error should mention 'string', got: %v", err)
		}
	})

	t.Run("inline primitive number rejected", func(t *testing.T) {
		_, err := parseJobPayload(`42`, "")
		if err == nil {
			t.Fatal("expected error for primitive payload")
		}
		if !strings.Contains(err.Error(), "must be a JSON object") {
			t.Errorf("error should mention 'must be a JSON object', got: %v", err)
		}
		if !strings.Contains(err.Error(), "number") {
			t.Errorf("error should mention 'number', got: %v", err)
		}
	})

	t.Run("file primitive rejected", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "primitive.json")
		if err := os.WriteFile(tmpFile, []byte(`"hello"`), 0o644); err != nil {
			t.Fatal(err)
		}

		_, err := parseJobPayload("", tmpFile)
		if err == nil {
			t.Fatal("expected error for primitive payload")
		}
		if !strings.Contains(err.Error(), "must contain a JSON object") {
			t.Errorf("error should mention 'must contain a JSON object', got: %v", err)
		}
	})

	t.Run("inline null rejected", func(t *testing.T) {
		_, err := parseJobPayload(`null`, "")
		if err == nil {
			t.Fatal("expected error for null payload")
		}
		if !strings.Contains(err.Error(), "must be a JSON object") {
			t.Errorf("error should mention 'must be a JSON object', got: %v", err)
		}
		if !strings.Contains(err.Error(), "null") {
			t.Errorf("error should mention 'null', got: %v", err)
		}
	})

	t.Run("file null rejected", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "null.json")
		if err := os.WriteFile(tmpFile, []byte(`null`), 0o644); err != nil {
			t.Fatal(err)
		}

		_, err := parseJobPayload("", tmpFile)
		if err == nil {
			t.Fatal("expected error for null payload")
		}
		if !strings.Contains(err.Error(), "must contain a JSON object") {
			t.Errorf("error should mention 'must contain a JSON object', got: %v", err)
		}
	})

	t.Run("malformed inline JSON rejected", func(t *testing.T) {
		_, err := parseJobPayload(`{invalid}`, "")
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "malformed --job JSON") {
			t.Errorf("error should mention 'malformed --job JSON', got: %v", err)
		}
	})

	t.Run("malformed file JSON rejected", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "bad.json")
		if err := os.WriteFile(tmpFile, []byte(`{not valid json}`), 0o644); err != nil {
			t.Fatal(err)
		}

		_, err := parseJobPayload("", tmpFile)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "malformed JSON in job file") {
			t.Errorf("error should mention 'malformed JSON in job file', got: %v", err)
		}
	})

	t.Run("file not found error", func(t *testing.T) {
		_, err := parseJobPayload("", "/nonexistent/job.json")
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "job file not found") {
			t.Errorf("error should mention 'job file not found', got: %v", err)
		}
	})

	t.Run("both flags set rejected", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "job.json")
		if err := os.WriteFile(tmpFile, []byte(`{}`), 0o644); err != nil {
			t.Fatal(err)
		}

		_, err := parseJobPayload(`{"inline": true}`, tmpFile)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "cannot use both --job and --job-json") {
			t.Errorf("error should mention conflict, got: %v", err)
		}
	})
}

func TestParseJobPayloadErrorsAreActionable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		jobInline   string
		jobFile     string
		mustContain []string
		description string
	}{
		{
			name:        "conflict lists both options",
			jobInline:   `{}`,
			jobFile:     "/some/file.json",
			mustContain: []string{"--job", "--job-json", "ONE of"},
			description: "should list both options and clarify to use one",
		},
		{
			name:        "file not found suggests ls",
			jobInline:   "",
			jobFile:     "/nonexistent/payload.json",
			mustContain: []string{"not found", "ls -la"},
			description: "should suggest checking file existence",
		},
		{
			name:        "malformed inline shows examples",
			jobInline:   `{broken`,
			jobFile:     "",
			mustContain: []string{"--job '{}'", `{"key": "value"}`},
			description: "should show valid JSON examples",
		},
		{
			name:        "array inline shows object requirement",
			jobInline:   `[]`,
			jobFile:     "",
			mustContain: []string{"must be a JSON object", "array", "not an array"},
			description: "should explain object-only requirement",
		},
		{
			name:        "null inline shows object requirement",
			jobInline:   `null`,
			jobFile:     "",
			mustContain: []string{"must be a JSON object", "null"},
			description: "should explain object-only requirement for null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseJobPayload(tt.jobInline, tt.jobFile)
			if err == nil {
				t.Fatal("expected error")
			}
			errMsg := err.Error()
			for _, must := range tt.mustContain {
				if !strings.Contains(errMsg, must) {
					t.Errorf("%s: error should contain %q\nGot: %s", tt.description, must, errMsg)
				}
			}
		})
	}
}

func TestDescribeJSONType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    any
		contains string
	}{
		{nil, "null"},
		{map[string]any{}, "object"},
		{[]any{1, 2}, "array"},
		{"hello", "string"},
		{float64(42), "number"},
		{true, "boolean"},
	}

	for _, tt := range tests {
		t.Run(tt.contains, func(t *testing.T) {
			result := describeJSONType(tt.input)
			if !strings.Contains(result, tt.contains) {
				t.Errorf("describeJSONType(%v) = %q, should contain %q", tt.input, result, tt.contains)
			}
		})
	}
}

func TestExitCodeConstants(t *testing.T) {
	t.Parallel()

	// Verify exit code semantics
	if exitConfigError != exitExecutorCrash {
		t.Error("exitConfigError should map to exitExecutorCrash (non-script error)")
	}
	if exitScriptError == exitConfigError {
		t.Error("exitScriptError should differ from exitConfigError")
	}
}

// --- Config precedence and validation tests ---

// newTestCLIContext builds a minimal *cli.Context with the given flags set.
// flagValues maps flag names to their string values. All listed flags are
// registered and marked as explicitly set (c.IsSet returns true).
// defaultFlags maps flag names to default values (not explicitly set).
func newTestCLIContext(t *testing.T, flagValues, defaultFlags map[string]string) *cli.Context {
	t.Helper()
	app := cli.NewApp()

	// Register all flags
	allFlags := make(map[string]string)
	for k, v := range defaultFlags {
		allFlags[k] = v
	}
	for k, v := range flagValues {
		allFlags[k] = v
	}

	var cliFlags []cli.Flag
	for name, val := range allFlags {
		cliFlags = append(cliFlags, &cli.StringFlag{Name: name, Value: val})
	}
	app.Flags = cliFlags

	// Build a flagset with only the explicitly set flags
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	for name, val := range allFlags {
		fs.String(name, val, "")
	}

	// Only set the flagValues (not defaults) so c.IsSet works
	for name, val := range flagValues {
		if err := fs.Set(name, val); err != nil {
			t.Fatalf("failed to set flag %s: %v", name, err)
		}
	}

	return cli.NewContext(app, fs, nil)
}

func TestResolveString_CLIWins(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	c := newTestCLIContext(t, map[string]string{"source": "cli-val"}, nil)
	got := resolveString(c, "source", "config-val")
	if got != "cli-val" {
		t.Errorf("expected CLI to win, got %q", got)
	}
}

// When the CLI flag is unset, resolveString falls back to the config value,
// then to the urfave default.
func TestResolveString_Fallback(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	for _, tc := range []struct{ name, flag, flagDefault, config, want string }{
		{"config value", "source", "", "config-val", "config-val"},
		{"urfave default", "category", "default", "", "default"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCLIContext(t, nil, map[string]string{tc.flag: tc.flagDefault})
			if got := resolveString(c, tc.flag, tc.config); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestConfigVal_NilConfig(t *testing.T) {
	t.Parallel()

	got := configVal(nil, func(c *quarryconfig.Config) string { return c.Source })
	if got != "" {
		t.Errorf("expected empty for nil config, got %q", got)
	}
}

func TestConfigVal_NonNil(t *testing.T) {
	t.Parallel()

	cfg := &quarryconfig.Config{Source: "from-config"}
	got := configVal(cfg, func(c *quarryconfig.Config) string { return c.Source })
	if got != "from-config" {
		t.Errorf("expected from-config, got %q", got)
	}
}

func TestResolveInt_CLIWins(t *testing.T) {
	t.Parallel()

	app := cli.NewApp()
	app.Flags = []cli.Flag{&cli.IntFlag{Name: "buffer-events"}}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Int("buffer-events", 0, "")
	_ = fs.Set("buffer-events", "500")
	c := cli.NewContext(app, fs, nil)

	got := resolveInt(c, "buffer-events", 1000)
	if got != 500 {
		t.Errorf("expected CLI to win with 500, got %d", got)
	}
}

func TestResolveInt_ConfigFallback(t *testing.T) {
	t.Parallel()

	app := cli.NewApp()
	app.Flags = []cli.Flag{&cli.IntFlag{Name: "buffer-events"}}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Int("buffer-events", 0, "")
	c := cli.NewContext(app, fs, nil)

	got := resolveInt(c, "buffer-events", 1000)
	if got != 1000 {
		t.Errorf("expected config fallback 1000, got %d", got)
	}
}

func TestResolveBool_CLIWins(t *testing.T) {
	t.Parallel()

	app := cli.NewApp()
	app.Flags = []cli.Flag{&cli.BoolFlag{Name: "storage-s3-path-style"}}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Bool("storage-s3-path-style", false, "")
	_ = fs.Set("storage-s3-path-style", "true")
	c := cli.NewContext(app, fs, nil)

	got := resolveBool(c, "storage-s3-path-style", false)
	if !got {
		t.Error("expected CLI true to win")
	}
}

func TestResolveDuration_CLIWins(t *testing.T) {
	t.Parallel()

	app := cli.NewApp()
	app.Flags = []cli.Flag{&cli.DurationFlag{Name: "adapter-timeout"}}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Duration("adapter-timeout", 0, "")
	_ = fs.Set("adapter-timeout", "30s")
	c := cli.NewContext(app, fs, nil)

	got := resolveDuration(c, "adapter-timeout", 10*time.Second)
	if got != 30*time.Second {
		t.Errorf("expected CLI 30s to win, got %v", got)
	}
}

func TestResolveDuration_ConfigFallback(t *testing.T) {
	t.Parallel()

	app := cli.NewApp()
	app.Flags = []cli.Flag{&cli.DurationFlag{Name: "adapter-timeout"}}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Duration("adapter-timeout", 0, "")
	c := cli.NewContext(app, fs, nil)

	got := resolveDuration(c, "adapter-timeout", 10*time.Second)
	if got != 10*time.Second {
		t.Errorf("expected config fallback 10s, got %v", got)
	}
}

// newTestApp creates a cli.App with RunCommand wired up and ExitErrHandler
// suppressed so errors are returned instead of calling os.Exit.
func newTestApp() *cli.App {
	app := cli.NewApp()
	cmd := RunCommand()
	app.Commands = []*cli.Command{cmd}
	app.ExitErrHandler = func(c *cli.Context, err error) {} // suppress os.Exit
	return app
}

// TestRunAction_MissingSource validates that runAction returns actionable error
// when source is missing from both CLI and config.
func TestRunAction_MissingSource(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	app := newTestApp()

	// Invoke with script and run-id but no source, no config
	err := app.Run([]string{
		"quarry", "run",
		"--script", "./test.ts",
		"--run-id", "run-001",
	})
	if err == nil {
		t.Fatal("expected error for missing source")
	}
	if !strings.Contains(err.Error(), "--source is required") {
		t.Errorf("error should mention --source is required, got: %v", err)
	}
}

// runAction rejects incomplete arguments with an actionable error.
func TestRunAction_RejectsIncompleteArguments(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	base := []string{"quarry", "run", "--script", "./test.ts", "--run-id", "run-001"}
	for _, tc := range []struct {
		name    string
		args    []string
		mention string
	}{
		{"missing storage backend", []string{"--source", "test"}, "--storage-backend is required"},
		{"missing storage path", []string{"--source", "test", "--storage-backend", "fs"}, "--storage-path is required"},
		{"config file not found", []string{"--config", "/nonexistent/quarry.yaml"}, "config file not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := newTestApp().Run(append(append([]string{}, base...), tc.args...))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.mention) {
				t.Errorf("error should mention %q, got: %v", tc.mention, err)
			}
		})
	}
}

// TestRunAction_ConfigProvidesRequiredFields validates that a config file
// can satisfy source, storage-backend, storage-path requirements.
func TestRunAction_ConfigProvidesRequiredFields(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	// Create a config file with required fields
	dir := t.TempDir()
	storageDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "quarry.yaml")
	configContent := "source: test-source\nstorage:\n  backend: fs\n  path: " + storageDir + "\n"
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatal(err)
	}

	app := newTestApp()

	// Run with --config providing the required fields. Execution will fail
	// at executor resolution but that's past the validation we're testing.
	err := app.Run([]string{
		"quarry", "run",
		"--config", configPath,
		"--script", "./test.ts",
		"--run-id", "run-001",
	})
	if err == nil {
		t.Skip("executor found; cannot test validation-only path")
	}
	// Should NOT be a missing-required-field error
	errMsg := err.Error()
	if strings.Contains(errMsg, "--source is required") {
		t.Error("source should be satisfied by config file")
	}
	if strings.Contains(errMsg, "--storage-backend is required") {
		t.Error("storage-backend should be satisfied by config file")
	}
	if strings.Contains(errMsg, "--storage-path is required") {
		t.Error("storage-path should be satisfied by config file")
	}
}

// TestRunAction_CLIOverridesConfig validates that CLI flags take precedence
// over config file values.
func TestRunAction_CLIOverridesConfig(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	dir := t.TempDir()
	storageDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "quarry.yaml")
	configContent := "source: config-source\nstorage:\n  backend: fs\n  path: " + storageDir + "\n"
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatal(err)
	}

	app := newTestApp()

	// Pass --source on CLI (should override config-source).
	// The run will fail at executor resolution but that's past the validation gate.
	err := app.Run([]string{
		"quarry", "run",
		"--config", configPath,
		"--script", "./test.ts",
		"--run-id", "run-001",
		"--source", "cli-source",
	})
	if err == nil {
		t.Skip("executor found; cannot test validation-only path")
	}
	// Should NOT fail on source validation
	if strings.Contains(err.Error(), "--source is required") {
		t.Error("CLI --source should override config")
	}
}

// TestRunAction_ProxyConflict validates that --proxy-config and config
// proxies: together is an error.
func TestRunAction_ProxyConflict(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	dir := t.TempDir()
	storageDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		t.Fatal(err)
	}

	configPath := filepath.Join(dir, "quarry.yaml")
	configContent := `source: test
storage:
  backend: fs
  path: ` + storageDir + `
proxies:
  pool_a:
    strategy: round_robin
    endpoints:
      - protocol: http
        host: proxy.example.com
        port: 8080
proxy:
  pool: pool_a
`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Also create a dummy proxy config JSON
	proxyConfigPath := filepath.Join(dir, "proxies.json")
	if err := os.WriteFile(proxyConfigPath, []byte(`[]`), 0o644); err != nil {
		t.Fatal(err)
	}

	app := newTestApp()

	err := app.Run([]string{
		"quarry", "run",
		"--config", configPath,
		"--script", "./test.ts",
		"--run-id", "run-001",
		"--proxy-config", proxyConfigPath,
	})
	if err == nil {
		t.Fatal("expected error for proxy config conflict")
	}
	if !strings.Contains(err.Error(), "cannot use --proxy-config and config file proxies") {
		t.Errorf("error should mention conflict, got: %v", err)
	}
}

// --- outcomeToExitCode ---

func TestOutcomeToExitCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status types.OutcomeStatus
		want   int
	}{
		{types.OutcomeSuccess, exitSuccess},
		{types.OutcomeScriptError, exitScriptError},
		{types.OutcomeExecutorCrash, exitExecutorCrash},
		{types.OutcomePolicyFailure, exitPolicyFailure},
	}
	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			if got := outcomeToExitCode(tt.status); got != tt.want {
				t.Errorf("outcomeToExitCode(%q) = %d, want %d", tt.status, got, tt.want)
			}
		})
	}
}

func TestOutcomeToExitCode_UnknownDefaultsToScriptError(t *testing.T) {
	t.Parallel()

	got := outcomeToExitCode(types.OutcomeStatus("unknown_status"))
	if got != exitScriptError {
		t.Errorf("unknown status should map to exitScriptError (%d), got %d", exitScriptError, got)
	}
}

func TestOutcomeToExitCode_ContractValues(t *testing.T) {
	t.Parallel()

	// Verify the actual numeric values per CONTRACT_RUN.md
	if exitSuccess != 0 {
		t.Errorf("exitSuccess should be 0, got %d", exitSuccess)
	}
	if exitScriptError != 1 {
		t.Errorf("exitScriptError should be 1, got %d", exitScriptError)
	}
	if exitExecutorCrash != 2 {
		t.Errorf("exitExecutorCrash should be 2, got %d", exitExecutorCrash)
	}
	if exitPolicyFailure != 3 {
		t.Errorf("exitPolicyFailure should be 3, got %d", exitPolicyFailure)
	}
}

// --- buildStoragePath ---

func TestBuildStoragePath_FS(t *testing.T) {
	t.Parallel()

	sc := storageChoice{backend: "fs", path: "/var/quarry/data"}
	got := buildStoragePath(sc, "quarry", "my-source", "default", "2026-02-08", "run-001")

	// Must be file:// scheme with absolute path
	if !strings.HasPrefix(got, "file:///") {
		t.Errorf("fs path should start with file:///, got %q", got)
	}
	// Must contain Hive-partitioned segments
	for _, segment := range []string{
		"datasets/quarry/partitions",
		"source=my-source",
		"category=default",
		"day=2026-02-08",
		"run_id=run-001",
	} {
		if !strings.Contains(got, segment) {
			t.Errorf("fs path should contain %q, got %q", segment, got)
		}
	}
}

func TestBuildStoragePath_S3(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, path, want string }{
		{"with prefix", "my-bucket/quarry-data", "s3://my-bucket/quarry-data/datasets/quarry/partitions/source=src/category=cat/day=2026-01-01/run_id=run-x"},
		{"bucket only", "my-bucket", "s3://my-bucket/datasets/quarry/partitions/source=src/category=cat/day=2026-01-01/run_id=run-x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc := storageChoice{backend: "s3", path: tc.path}
			if got := buildStoragePath(sc, "quarry", "src", "cat", "2026-01-01", "run-x"); got != tc.want {
				t.Errorf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

func TestBuildStoragePath_UnknownBackend(t *testing.T) {
	t.Parallel()

	sc := storageChoice{backend: "gcs", path: "/tmp"}
	got := buildStoragePath(sc, "quarry", "src", "cat", "2026-01-01", "run-x")

	// Unknown backend returns bare partition path (no scheme prefix)
	if strings.Contains(got, "://") {
		t.Errorf("unknown backend should not include scheme, got %q", got)
	}
	if !strings.HasPrefix(got, "datasets/") {
		t.Errorf("unknown backend should return bare partition path, got %q", got)
	}
}

// --- buildRunCompletedEvent ---

func TestBuildRunCompletedEvent_BasicFields(t *testing.T) {
	t.Parallel()

	result := &runtime.RunResult{
		RunMeta: &types.RunMeta{
			RunID:   "run-001",
			Attempt: 1,
		},
		Outcome: &types.RunOutcome{
			Status: types.OutcomeSuccess,
		},
		EventCount: 42,
	}
	sc := storageChoice{backend: "fs", path: "/tmp/data"}
	event := buildRunCompletedEvent(result, sc, "quarry", "src", "cat", "2026-02-08", 5*time.Second)

	if event.ContractVersion != types.ContractVersion {
		t.Errorf("ContractVersion = %q, want %q", event.ContractVersion, types.ContractVersion)
	}
	if event.EventType != "run_completed" {
		t.Errorf("EventType = %q, want %q", event.EventType, "run_completed")
	}
	if event.RunID != "run-001" {
		t.Errorf("RunID = %q, want %q", event.RunID, "run-001")
	}
	if event.Source != "src" {
		t.Errorf("Source = %q, want %q", event.Source, "src")
	}
	if event.Category != "cat" {
		t.Errorf("Category = %q, want %q", event.Category, "cat")
	}
	if event.Day != "2026-02-08" {
		t.Errorf("Day = %q, want %q", event.Day, "2026-02-08")
	}
	if event.Outcome != "success" {
		t.Errorf("Outcome = %q, want %q", event.Outcome, "success")
	}
	if event.Attempt != 1 {
		t.Errorf("Attempt = %d, want %d", event.Attempt, 1)
	}
	if event.EventCount != 42 {
		t.Errorf("EventCount = %d, want %d", event.EventCount, 42)
	}
	if event.DurationMs != 5000 {
		t.Errorf("DurationMs = %d, want %d", event.DurationMs, 5000)
	}
	if event.StoragePath == "" {
		t.Error("StoragePath should not be empty")
	}
	if event.Timestamp == "" {
		t.Error("Timestamp should not be empty")
	}
}

func TestBuildRunCompletedEvent_WithJobID(t *testing.T) {
	t.Parallel()

	jobID := "job-abc"
	result := &runtime.RunResult{
		RunMeta: &types.RunMeta{
			RunID:   "run-001",
			JobID:   &jobID,
			Attempt: 1,
		},
		Outcome: &types.RunOutcome{Status: types.OutcomeSuccess},
	}
	sc := storageChoice{backend: "fs", path: "/tmp"}
	event := buildRunCompletedEvent(result, sc, "quarry", "src", "cat", "2026-02-08", time.Second)

	if event.JobID != "job-abc" {
		t.Errorf("JobID = %q, want %q", event.JobID, "job-abc")
	}
}

func TestBuildRunCompletedEvent_WithoutJobID(t *testing.T) {
	t.Parallel()

	result := &runtime.RunResult{
		RunMeta: &types.RunMeta{
			RunID:   "run-001",
			Attempt: 1,
		},
		Outcome: &types.RunOutcome{Status: types.OutcomeScriptError},
	}
	sc := storageChoice{backend: "fs", path: "/tmp"}
	event := buildRunCompletedEvent(result, sc, "quarry", "src", "cat", "2026-02-08", time.Second)

	if event.JobID != "" {
		t.Errorf("JobID should be empty when RunMeta.JobID is nil, got %q", event.JobID)
	}
}

func TestBuildRunCompletedEvent_OutcomeMapsCorrectly(t *testing.T) {
	t.Parallel()

	for _, status := range []types.OutcomeStatus{
		types.OutcomeSuccess,
		types.OutcomeScriptError,
		types.OutcomeExecutorCrash,
		types.OutcomePolicyFailure,
	} {
		t.Run(string(status), func(t *testing.T) {
			result := &runtime.RunResult{
				RunMeta: &types.RunMeta{RunID: "r", Attempt: 1},
				Outcome: &types.RunOutcome{Status: status},
			}
			sc := storageChoice{backend: "fs", path: "/tmp"}
			event := buildRunCompletedEvent(result, sc, "q", "s", "c", "d", 0)

			if event.Outcome != string(status) {
				t.Errorf("Outcome = %q, want %q", event.Outcome, string(status))
			}
		})
	}
}

// --- validateFanOutConfig ---

func TestValidateFanOutConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		choice      fanOutChoice
		wantErr     bool
		errContains string
	}{
		{
			name:    "disabled (depth=0) is valid",
			choice:  fanOutChoice{depth: 0, maxRuns: 0, parallel: 1},
			wantErr: false,
		},
		{
			name:    "depth=1 with max-runs is valid",
			choice:  fanOutChoice{depth: 1, maxRuns: 10, parallel: 1},
			wantErr: false,
		},
		{
			name:    "depth=1 with parallel>1 is valid",
			choice:  fanOutChoice{depth: 1, maxRuns: 10, parallel: 4},
			wantErr: false,
		},
		{
			name:        "negative depth rejected",
			choice:      fanOutChoice{depth: -1, maxRuns: 0, parallel: 1},
			wantErr:     true,
			errContains: "--depth must be >= 0",
		},
		{
			name:        "depth>0 without max-runs rejected (safety rail)",
			choice:      fanOutChoice{depth: 1, maxRuns: 0, parallel: 1},
			wantErr:     true,
			errContains: "--max-runs is required when --depth > 0",
		},
		{
			name:        "negative max-runs rejected",
			choice:      fanOutChoice{depth: 0, maxRuns: -1, parallel: 1},
			wantErr:     true,
			errContains: "--max-runs must be >= 0",
		},
		{
			name:        "parallel=0 rejected",
			choice:      fanOutChoice{depth: 1, maxRuns: 10, parallel: 0},
			wantErr:     true,
			errContains: "--parallel must be >= 1",
		},
		{
			name:    "depth=0 max-runs=0 parallel=1 is default valid state",
			choice:  fanOutChoice{depth: 0, maxRuns: 0, parallel: 1},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFanOutConfig(tt.choice)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// --- parseAdapterConfigWithPrecedence ---

// newAdapterTestContext builds a CLI context with adapter-related flags.
func newAdapterTestContext(t *testing.T, flags map[string]string, sliceFlags map[string][]string) *cli.Context {
	t.Helper()
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: "adapter-url"},
		&cli.StringFlag{Name: "adapter-channel"},
		&cli.DurationFlag{Name: "adapter-timeout", Value: 10 * time.Second},
		&cli.IntFlag{Name: "adapter-retries", Value: 3},
		&cli.StringSliceFlag{Name: "adapter-header"},
	}

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.String("adapter-url", "", "")
	fs.String("adapter-channel", "", "")
	fs.Duration("adapter-timeout", 10*time.Second, "")
	fs.Int("adapter-retries", 3, "")

	// Register the string slice in the flagset via a multi-value approach.
	// urfave/cli uses its own internal plumbing for slices, so we handle
	// headers through the config path or by setting the flag manually.
	for name, val := range flags {
		if err := fs.Set(name, val); err != nil {
			t.Fatalf("failed to set flag %s: %v", name, err)
		}
	}

	c := cli.NewContext(app, fs, nil)

	// For string slice flags, we need to work within urfave's model.
	// The test cases that exercise headers use config-based headers instead
	// since urfave slices require the full app.Run() path.
	_ = sliceFlags
	return c
}

func TestParseAdapterConfig_WebhookValid(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	c := newAdapterTestContext(t, map[string]string{
		"adapter-url": "https://hooks.example.com/quarry",
	}, nil)

	ac, err := parseAdapterConfigWithPrecedence(c, nil, "webhook")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ac.adapterType != "webhook" {
		t.Errorf("adapterType = %q, want %q", ac.adapterType, "webhook")
	}
	if ac.url != "https://hooks.example.com/quarry" {
		t.Errorf("url = %q, want %q", ac.url, "https://hooks.example.com/quarry")
	}
}

func TestParseAdapterConfig_MissingURL(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	for _, tc := range []struct{ adapter, mention string }{
		{"webhook", "--adapter-url is required"},
		{"redis", "--adapter-url is required when --adapter=redis"},
	} {
		t.Run(tc.adapter, func(t *testing.T) {
			c := newAdapterTestContext(t, nil, nil)
			_, err := parseAdapterConfigWithPrecedence(c, nil, tc.adapter)
			if err == nil {
				t.Fatal("expected error for missing URL")
			}
			if !strings.Contains(err.Error(), tc.mention) {
				t.Errorf("error should mention %q, got: %v", tc.mention, err)
			}
		})
	}
}

func TestParseAdapterConfig_RedisValid(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	c := newAdapterTestContext(t, map[string]string{
		"adapter-url":     "redis://localhost:6379",
		"adapter-channel": "my-channel",
	}, nil)

	ac, err := parseAdapterConfigWithPrecedence(c, nil, "redis")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ac.adapterType != "redis" {
		t.Errorf("adapterType = %q, want %q", ac.adapterType, "redis")
	}
	if ac.channel != "my-channel" {
		t.Errorf("channel = %q, want %q", ac.channel, "my-channel")
	}
}

func TestParseAdapterConfig_UnknownType(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	c := newAdapterTestContext(t, map[string]string{
		"adapter-url": "https://example.com",
	}, nil)

	_, err := parseAdapterConfigWithPrecedence(c, nil, "kafka")
	if err == nil {
		t.Fatal("expected error for unknown adapter type")
	}
	if !strings.Contains(err.Error(), "unknown adapter type") {
		t.Errorf("error should mention unknown type, got: %v", err)
	}
	if !strings.Contains(err.Error(), "kafka") {
		t.Errorf("error should include the bad type name, got: %v", err)
	}
}

func TestParseAdapterConfig_ConfigProvidesURL(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	// CLI has no --adapter-url set; config provides it
	c := newAdapterTestContext(t, nil, nil)
	cfg := &quarryconfig.Config{
		Adapter: quarryconfig.AdapterConfig{
			URL: "https://from-config.example.com",
		},
	}

	ac, err := parseAdapterConfigWithPrecedence(c, cfg, "webhook")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ac.url != "https://from-config.example.com" {
		t.Errorf("url should come from config, got %q", ac.url)
	}
}

func TestParseAdapterConfig_CLIOverridesConfigURL(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	c := newAdapterTestContext(t, map[string]string{
		"adapter-url": "https://cli-url.example.com",
	}, nil)
	cfg := &quarryconfig.Config{
		Adapter: quarryconfig.AdapterConfig{
			URL: "https://config-url.example.com",
		},
	}

	ac, err := parseAdapterConfigWithPrecedence(c, cfg, "webhook")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ac.url != "https://cli-url.example.com" {
		t.Errorf("CLI should override config URL, got %q", ac.url)
	}
}

func TestParseAdapterConfig_ConfigProvidesRetries(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	c := newAdapterTestContext(t, map[string]string{
		"adapter-url": "https://example.com",
	}, nil)
	retries := 5
	cfg := &quarryconfig.Config{
		Adapter: quarryconfig.AdapterConfig{
			URL:     "https://example.com",
			Retries: &retries,
		},
	}

	ac, err := parseAdapterConfigWithPrecedence(c, cfg, "webhook")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ac.retries != 5 {
		t.Errorf("retries should come from config (5), got %d", ac.retries)
	}
}

func TestParseAdapterConfig_ConfigHeadersMerged(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	c := newAdapterTestContext(t, map[string]string{
		"adapter-url": "https://example.com",
	}, nil)
	cfg := &quarryconfig.Config{
		Adapter: quarryconfig.AdapterConfig{
			URL: "https://example.com",
			Headers: map[string]string{
				"X-Api-Key": "secret-123",
				"X-Source":  "quarry",
			},
		},
	}

	ac, err := parseAdapterConfigWithPrecedence(c, cfg, "webhook")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ac.headers["X-Api-Key"] != "secret-123" {
		t.Errorf("config header X-Api-Key not merged, got %v", ac.headers)
	}
	if ac.headers["X-Source"] != "quarry" {
		t.Errorf("config header X-Source not merged, got %v", ac.headers)
	}
}

func TestParseAdapterConfig_MalformedHeader(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	// Build an app context with a malformed --adapter-header via app.Run
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: "adapter-url"},
		&cli.StringSliceFlag{Name: "adapter-header"},
		&cli.DurationFlag{Name: "adapter-timeout", Value: 10 * time.Second},
		&cli.IntFlag{Name: "adapter-retries", Value: 3},
		&cli.StringFlag{Name: "adapter-channel"},
	}

	var parseErr error
	app.Action = func(c *cli.Context) error {
		_, parseErr = parseAdapterConfigWithPrecedence(c, nil, "webhook")
		return nil
	}

	_ = app.Run([]string{
		"test",
		"--adapter-url", "https://example.com",
		"--adapter-header", "no-equals-sign",
	})

	if parseErr == nil {
		t.Fatal("expected error for malformed header")
	}
	if !strings.Contains(parseErr.Error(), "invalid --adapter-header") {
		t.Errorf("error should mention invalid header, got: %v", parseErr)
	}
	if !strings.Contains(parseErr.Error(), "key=value") {
		t.Errorf("error should suggest key=value format, got: %v", parseErr)
	}
}

// --- --resolve-from validation ---

func TestRunAction_ResolveFromNonexistentPath(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	app := newTestApp()

	dir := t.TempDir()
	storageDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		t.Fatal(err)
	}

	err := app.Run([]string{
		"quarry", "run",
		"--script", "./test.ts",
		"--run-id", "run-001",
		"--source", "test",
		"--storage-backend", "fs",
		"--storage-path", storageDir,
		"--resolve-from", "/nonexistent/node_modules",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent --resolve-from path")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error should mention 'does not exist', got: %v", err)
	}
}

func TestRunAction_ResolveFromNotADirectory(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	app := newTestApp()

	dir := t.TempDir()
	storageDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create a regular file to use as --resolve-from (should fail)
	filePath := filepath.Join(dir, "not-a-dir.txt")
	if err := os.WriteFile(filePath, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := app.Run([]string{
		"quarry", "run",
		"--script", "./test.ts",
		"--run-id", "run-001",
		"--source", "test",
		"--storage-backend", "fs",
		"--storage-path", storageDir,
		"--resolve-from", filePath,
	})
	if err == nil {
		t.Fatal("expected error for file as --resolve-from")
	}
	if !strings.Contains(err.Error(), "not a directory") {
		t.Errorf("error should mention 'not a directory', got: %v", err)
	}
}

func TestRunAction_ResolveFromValidDirectory(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	app := newTestApp()

	dir := t.TempDir()
	storageDir := filepath.Join(dir, "data")
	nodeModulesDir := filepath.Join(dir, "node_modules")
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(nodeModulesDir, 0o755); err != nil {
		t.Fatal(err)
	}

	err := app.Run([]string{
		"quarry", "run",
		"--script", "./test.ts",
		"--run-id", "run-001",
		"--source", "test",
		"--storage-backend", "fs",
		"--storage-path", storageDir,
		"--resolve-from", nodeModulesDir,
	})
	// Should pass resolve-from validation and fail later (executor resolution)
	if err == nil {
		t.Skip("executor found; cannot test validation-only path")
	}
	if strings.Contains(err.Error(), "resolve-from") {
		t.Errorf("error should NOT be about --resolve-from, got: %v", err)
	}
}

func TestRunAction_ResolveFromConfigFile(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	dir := t.TempDir()
	storageDir := filepath.Join(dir, "data")
	nodeModulesDir := filepath.Join(dir, "node_modules")
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(nodeModulesDir, 0o755); err != nil {
		t.Fatal(err)
	}

	configPath := filepath.Join(dir, "quarry.yaml")
	configContent := "source: test\nstorage:\n  backend: fs\n  path: " + storageDir + "\nresolve_from: " + nodeModulesDir + "\n"
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatal(err)
	}

	app := newTestApp()
	err := app.Run([]string{
		"quarry", "run",
		"--config", configPath,
		"--script", "./test.ts",
		"--run-id", "run-001",
	})
	// Should pass resolve-from validation and fail later (executor resolution)
	if err == nil {
		t.Skip("executor found; cannot test validation-only path")
	}
	if strings.Contains(err.Error(), "resolve-from") {
		t.Errorf("config resolve_from should be accepted, got: %v", err)
	}
}

func TestParseAdapterConfig_RedisChannelFromConfig(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	c := newAdapterTestContext(t, map[string]string{
		"adapter-url": "redis://localhost:6379",
	}, nil)
	cfg := &quarryconfig.Config{
		Adapter: quarryconfig.AdapterConfig{
			URL:     "redis://localhost:6379",
			Channel: "custom-channel",
		},
	}

	ac, err := parseAdapterConfigWithPrecedence(c, cfg, "redis")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ac.channel != "custom-channel" {
		t.Errorf("channel should come from config, got %q", ac.channel)
	}
}

// --- --dry-run validation ---

// TestRunAction_DryRun_SkipsSourceRequirement validates that --dry-run does
// not require --source (script validation does not need partition keys).
func TestRunAction_DryRun_SkipsSourceRequirement(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	app := newTestApp()

	// With --dry-run but no --source: should NOT get "--source is required"
	err := app.Run([]string{
		"quarry", "run",
		"--script", "./test.ts",
		"--run-id", "run-001",
		"--dry-run",
	})
	// Will fail at executor resolution, but should NOT fail at --source validation
	if err == nil {
		t.Skip("executor found; cannot test validation-only path")
	}
	if strings.Contains(err.Error(), "--source is required") {
		t.Error("--dry-run should not require --source")
	}
}

// TestRunAction_DryRun_SkipsStorageRequirement validates that --dry-run does
// not require --storage-backend or --storage-path.
func TestRunAction_DryRun_SkipsStorageRequirement(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	app := newTestApp()

	err := app.Run([]string{
		"quarry", "run",
		"--script", "./test.ts",
		"--run-id", "run-001",
		"--dry-run",
	})
	if err == nil {
		t.Skip("executor found; cannot test validation-only path")
	}
	errMsg := err.Error()
	if strings.Contains(errMsg, "--storage-backend is required") {
		t.Error("--dry-run should not require --storage-backend")
	}
	if strings.Contains(errMsg, "--storage-path is required") {
		t.Error("--dry-run should not require --storage-path")
	}
}

// TestRunAction_DryRun_ExecutorMissing_ExitCode2 validates that when
// --dry-run cannot resolve the executor, the exit code is 2 (exitConfigError
// which maps to exitExecutorCrash).
func TestRunAction_DryRun_ExecutorMissing_ExitCode2(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	app := newTestApp()

	err := app.Run([]string{
		"quarry", "run",
		"--script", "./test.ts",
		"--run-id", "run-001",
		"--dry-run",
		"--executor", "/nonexistent/executor.js",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent executor")
	}
	exitErr, ok := err.(cli.ExitCoder)
	if !ok {
		t.Fatalf("expected cli.ExitCoder, got %T: %v", err, err)
	}
	if exitErr.ExitCode() != exitConfigError {
		t.Errorf("exit code = %d, want %d (exitConfigError)", exitErr.ExitCode(), exitConfigError)
	}
}

// TestRunAction_DryRun_ExecutorSpawnFailure_ExitCode2 validates the contract
// path: executor file exists (resolveExecutor passes) but the process fails to
// execute (runtime.ValidateScript returns an error) → exit 2 (exitExecutorCrash).
func TestRunAction_DryRun_ExecutorSpawnFailure_ExitCode2(t *testing.T) {
	testutil.Serial(t, "runs a urfave/cli app, which mutates the package-level flag definitions")
	// Create a temp file that exists on disk (passes os.Stat in resolveExecutor)
	// but is not a valid executable, so exec.CommandContext will fail.
	badExecutor := filepath.Join(t.TempDir(), "bad-executor")
	if err := os.WriteFile(badExecutor, []byte("not a valid executable"), 0o644); err != nil {
		t.Fatalf("writing bad executor: %v", err)
	}

	app := newTestApp()

	err := app.Run([]string{
		"quarry", "run",
		"--script", "./test.ts",
		"--run-id", "run-001",
		"--dry-run",
		"--executor", badExecutor,
	})
	if err == nil {
		t.Fatal("expected error for non-executable executor")
	}
	exitErr, ok := err.(cli.ExitCoder)
	if !ok {
		t.Fatalf("expected cli.ExitCoder, got %T: %v", err, err)
	}
	if exitErr.ExitCode() != exitExecutorCrash {
		t.Errorf("exit code = %d, want %d (exitExecutorCrash)", exitErr.ExitCode(), exitExecutorCrash)
	}
}

// TestRunDryRun_ExitCodes validates the exit code mapping for runDryRun.
func TestRunDryRun_ExitCodes(t *testing.T) {
	t.Parallel()

	t.Run("valid result exits 0", func(t *testing.T) {
		// runDryRun is not directly testable without an executor, but we can
		// verify the exit code constants match the contract.
		if exitSuccess != 0 {
			t.Errorf("exitSuccess = %d, want 0", exitSuccess)
		}
	})

	t.Run("script error exits 1", func(t *testing.T) {
		if exitScriptError != 1 {
			t.Errorf("exitScriptError = %d, want 1", exitScriptError)
		}
	})

	t.Run("executor crash exits 2", func(t *testing.T) {
		if exitExecutorCrash != 2 {
			t.Errorf("exitExecutorCrash = %d, want 2", exitExecutorCrash)
		}
	})
}

// TestChildRun_StorageDayAlignedWithBuildPolicy verifies the invariant that
// was broken before the day-drift fix: buildPolicy() and the RunConfig's
// StorageDay must derive the same day from a single captured timestamp.
//
// The original bug had two separate time.Now() calls — one for buildPolicy
// and one for StorageDay — which could straddle a UTC midnight boundary.
// The fix captures childStartTime once and passes it to both.
//
// This test observes buildPolicy's internally-derived day by writing a probe
// file through the returned FileWriter and inspecting the Hive-partitioned
// path on disk. The day= partition in the path is what buildPolicy computed
// from the timestamp; StorageDay is what RunConfig would pass to the executor.
// If someone reintroduces a second time.Now(), these would diverge.
func TestChildRun_StorageDayAlignedWithBuildPolicy(t *testing.T) {
	t.Parallel()

	// Use a timestamp at the UTC midnight boundary where the old bug would
	// have been most likely to manifest (two time.Now() calls straddling midnight).
	childStartTime := time.Date(2026, 2, 23, 23, 59, 59, 999_000_000, time.UTC)

	// Set up minimal fs-backend storage in a temp dir so buildPolicy succeeds
	storageDir := t.TempDir()
	storage := storageChoice{backend: "fs", path: storageDir}
	pol := policyChoice{name: "strict", flushMode: "at_least_once"}
	collector := metrics.NewCollector("strict", "executor.mjs", "fs", "run-001", "")

	// Call buildPolicy with the captured timestamp — this is exactly what
	// childFactory.Run() does at run.go:386-389
	childPol, _, childFileWriter, err := buildPolicy(t.Context(), pol, storage, "quarry", "src", "cat", "run-001", childStartTime, collector, nil)
	if err != nil {
		t.Fatalf("buildPolicy: %v", err)
	}
	defer iox.DiscardClose(childPol)

	// Write a probe file through the FileWriter to observe the day partition
	// that buildPolicy derived internally. The file lands at:
	//   datasets/quarry/partitions/source=src/category=cat/day=<DAY>/run_id=run-001/files/probe.txt
	// where <DAY> is what buildStorageSink computed via lode.DeriveDay(startTime).
	if err := childFileWriter.PutFile(t.Context(), "probe.txt", "text/plain", []byte("x")); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	// Glob for the day= partition directory to extract buildPolicy's derived day
	dayPattern := filepath.Join(storageDir, "datasets", "quarry", "partitions",
		"source=src", "category=cat", "day=*")
	dayDirs, err := filepath.Glob(dayPattern)
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(dayDirs) != 1 {
		t.Fatalf("expected 1 day= dir, found %d: %v", len(dayDirs), dayDirs)
	}
	buildPolicyDay := filepath.Base(dayDirs[0]) // "day=2026-02-23"

	// Compute StorageDay the same way childFactory.Run() does at run.go:410
	storageDay := "day=" + lode.DeriveDay(childStartTime)

	// The key assertion: buildPolicy's internally-derived day (observed via
	// the filesystem) must equal the StorageDay that RunConfig passes to the
	// executor. If these diverge, the executor computes storage keys using a
	// different day than the runtime writes data to.
	if buildPolicyDay != storageDay {
		t.Errorf("buildPolicy day = %q, StorageDay = %q (must match)", buildPolicyDay, storageDay)
	}

	// Verify the boundary: one ms later rolls to the next day
	nextMs := childStartTime.Add(time.Millisecond)
	if lode.DeriveDay(nextMs) == lode.DeriveDay(childStartTime) {
		t.Error("expected next millisecond to roll to a different day at UTC midnight")
	}
}

// --- Event sink config parsing tests ---

func TestParseEventSinkConfig_NoConfigDefaultsNil(t *testing.T) {
	t.Parallel()

	// No CLI flags, no config → nil (Lode-only default)
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.StringSliceFlag{Name: "event-sink"},
	}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := cli.NewContext(app, fs, nil)

	sinks, err := parseEventSinkConfig(c, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sinks != nil {
		t.Errorf("expected nil for default, got %v", sinks)
	}
}

func TestParseEventSinkConfig_ConfigFallback(t *testing.T) {
	t.Parallel()

	// No CLI flags, config has redis+lode → uses config
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.StringSliceFlag{Name: "event-sink"},
	}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := cli.NewContext(app, fs, nil)

	cfg := &quarryconfig.Config{
		Events: quarryconfig.EventSinksConfig{
			Sinks: []quarryconfig.EventSinkConfig{
				{Type: "lode"},
				{Type: "redis", URL: "redis://localhost:6379"},
			},
		},
	}

	sinks, err := parseEventSinkConfig(c, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sinks) != 2 {
		t.Fatalf("expected 2 sinks from config, got %d", len(sinks))
	}
	if sinks[0].sinkType != "lode" {
		t.Errorf("expected first sink lode, got %s", sinks[0].sinkType)
	}
	if sinks[1].sinkType != "redis" {
		t.Errorf("expected second sink redis, got %s", sinks[1].sinkType)
	}
}

func TestParseEventSinkFromConfig_DuplicateTypeRejected(t *testing.T) {
	t.Parallel()

	_, err := parseEventSinkFromConfig([]quarryconfig.EventSinkConfig{
		{Type: "redis", URL: "redis://localhost:6379"},
		{Type: "redis", URL: "redis://localhost:6380"},
	})
	if err == nil {
		t.Fatal("expected error for duplicate redis")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected 'duplicate' in error, got: %v", err)
	}
}

func TestParseEventSinkFromConfig_DuplicateLodeRejected(t *testing.T) {
	t.Parallel()

	_, err := parseEventSinkFromConfig([]quarryconfig.EventSinkConfig{
		{Type: "lode"},
		{Type: "lode"},
	})
	if err == nil {
		t.Fatal("expected error for duplicate lode")
	}
}

func TestParseEventSinkFromConfig_UnknownTypeRejected(t *testing.T) {
	t.Parallel()

	_, err := parseEventSinkFromConfig([]quarryconfig.EventSinkConfig{
		{Type: "kafka"},
	})
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
	if !strings.Contains(err.Error(), "unknown") {
		t.Errorf("expected 'unknown' in error, got: %v", err)
	}
}

func TestParseEventSinkFromConfig_RedisMissingURLRejected(t *testing.T) {
	t.Parallel()

	_, err := parseEventSinkFromConfig([]quarryconfig.EventSinkConfig{
		{Type: "redis"},
	})
	if err == nil {
		t.Fatal("expected error for missing redis URL")
	}
}

func TestParseEventSinkFromConfig_InvalidDeliveryRejected(t *testing.T) {
	t.Parallel()

	_, err := parseEventSinkFromConfig([]quarryconfig.EventSinkConfig{
		{Type: "lode", Delivery: "at_most_once"},
	})
	if err == nil {
		t.Fatal("expected error for invalid delivery mode")
	}
}

func TestParseEventSinkFromConfig_RedisRetriesDefault(t *testing.T) {
	t.Parallel()

	// When retries is omitted from config, the default (2) should be applied,
	// not the Go zero value (0 = no retries).
	choices, err := parseEventSinkFromConfig([]quarryconfig.EventSinkConfig{
		{Type: "redis", URL: "redis://localhost:6379"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(choices) != 1 {
		t.Fatalf("expected 1 choice, got %d", len(choices))
	}
	if choices[0].retries != redisstream.DefaultRetries {
		t.Errorf("expected default retries %d, got %d", redisstream.DefaultRetries, choices[0].retries)
	}
}

func TestParseEventSinkFromConfig_RedisRetriesExplicitZero(t *testing.T) {
	t.Parallel()

	// Explicit retries: 0 means "no retries" — must not be overridden to default.
	zero := 0
	choices, err := parseEventSinkFromConfig([]quarryconfig.EventSinkConfig{
		{Type: "redis", URL: "redis://localhost:6379", Retries: &zero},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if choices[0].retries != 0 {
		t.Errorf("expected 0 retries, got %d", choices[0].retries)
	}
}

func TestParseDeliveryMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		want    policy.DeliveryMode
		wantErr bool
	}{
		{"", policy.DeliveryMandatory, false},
		{"mandatory", policy.DeliveryMandatory, false},
		{"best_effort", policy.DeliveryBestEffort, false},
		{"at_most_once", 0, true},
	}
	for _, tt := range tests {
		got, err := parseDeliveryMode(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseDeliveryMode(%q): err=%v, wantErr=%v", tt.input, err, tt.wantErr)
		}
		if !tt.wantErr && got != tt.want {
			t.Errorf("parseDeliveryMode(%q)=%v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestChildFactory_InheritsEventSinks(t *testing.T) {
	t.Parallel()

	// Verify that childFactory stores event sink config and passes it
	// to buildPolicy. We test this by checking the struct field directly
	// since buildPolicy is the integration point already tested.
	sinks := []eventSinkChoice{
		{sinkType: "lode", delivery: policy.DeliveryMandatory},
		{sinkType: "redis", delivery: policy.DeliveryBestEffort, url: "redis://localhost:6379"},
	}
	factory := &childFactory{
		eventSinks: sinks,
	}
	if len(factory.eventSinks) != 2 {
		t.Fatalf("expected 2 event sinks, got %d", len(factory.eventSinks))
	}
	if factory.eventSinks[0].sinkType != "lode" {
		t.Errorf("expected first sink lode, got %s", factory.eventSinks[0].sinkType)
	}
	if factory.eventSinks[1].delivery != policy.DeliveryBestEffort {
		t.Errorf("expected second sink best_effort, got %v", factory.eventSinks[1].delivery)
	}
}
