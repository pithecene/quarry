package config

import (
	"testing"

	"github.com/pithecene/quarry/internal/testutil"
)

func TestExpandEnv_SetVar(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	t.Setenv("TEST_VAR", "hello")

	got := ExpandEnv("value: ${TEST_VAR}")
	want := "value: hello"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExpandEnv_UnsetVar(t *testing.T) {
	t.Parallel()

	got := ExpandEnv("value: ${UNSET_VAR_12345}")
	want := "value: "
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExpandEnv_DefaultUsedWhenUnset(t *testing.T) {
	t.Parallel()

	got := ExpandEnv("value: ${UNSET_VAR_12345:-fallback}")
	want := "value: fallback"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExpandEnv_DefaultIgnoredWhenSet(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	t.Setenv("TEST_VAR", "real")

	got := ExpandEnv("value: ${TEST_VAR:-fallback}")
	want := "value: real"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExpandEnv_DefaultUsedWhenEmpty(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	t.Setenv("TEST_VAR", "")

	got := ExpandEnv("value: ${TEST_VAR:-fallback}")
	want := "value: fallback"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExpandEnv_MultipleVars(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	t.Setenv("USER_A", "alice")
	t.Setenv("USER_B", "bob")

	got := ExpandEnv("${USER_A}:${USER_B}")
	want := "alice:bob"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExpandEnv_NoVars(t *testing.T) {
	t.Parallel()

	input := "no variables here"
	got := ExpandEnv(input)
	if got != input {
		t.Errorf("got %q, want %q", got, input)
	}
}

func TestExpandEnv_DollarWithoutBraces(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	t.Setenv("SOME_VAR", "value")

	// $VAR (no braces) must not be expanded — only ${VAR} is supported.
	got := ExpandEnv("path: $SOME_VAR/suffix")
	want := "path: $SOME_VAR/suffix"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExpandEnv_EmptyDefault(t *testing.T) {
	t.Parallel()

	// ${VAR:-} with empty default expands to empty string when unset.
	got := ExpandEnv("value: ${UNSET_VAR_99999:-}")
	want := "value: "
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExpandEnv_DefaultWithSpecialChars(t *testing.T) {
	t.Parallel()

	// Default value containing colons, slashes, and port numbers.
	got := ExpandEnv("url: ${UNSET_VAR_99999:-http://localhost:8080/path}")
	want := "url: http://localhost:8080/path"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExpandEnv_NestedInYAML(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	t.Setenv("PROXY_USER", "admin")
	t.Setenv("PROXY_PASS", "secret")

	input := `proxies:
  pool1:
    endpoints:
      - username: ${PROXY_USER}
        password: ${PROXY_PASS}`

	got := ExpandEnv(input)
	want := `proxies:
  pool1:
    endpoints:
      - username: admin
        password: secret`

	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}
