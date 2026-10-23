package config

import (
	"testing"

	"github.com/pithecene/quarry/internal/testutil"
)

func TestExpandEnv(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	cases := []struct {
		name string
		env  map[string]string
		in   string
		want string
	}{
		{"set variable", map[string]string{"TEST_VAR": "hello"}, "value: ${TEST_VAR}", "value: hello"},
		{"unset variable", nil, "value: ${UNSET_VAR_12345}", "value: "},
		{"default used when unset", nil, "value: ${UNSET_VAR_12345:-fallback}", "value: fallback"},
		{"default ignored when set", map[string]string{"TEST_VAR": "real"}, "value: ${TEST_VAR:-fallback}", "value: real"},
		{"default used when empty", map[string]string{"TEST_VAR": ""}, "value: ${TEST_VAR:-fallback}", "value: fallback"},
		{"multiple variables", map[string]string{"USER_A": "alice", "USER_B": "bob"}, "${USER_A}:${USER_B}", "alice:bob"},
		{"no variables", nil, "no variables here", "no variables here"},
		// $VAR (no braces) is not expanded: only ${VAR} is supported.
		{"dollar without braces", map[string]string{"SOME_VAR": "value"}, "path: $SOME_VAR/suffix", "path: $SOME_VAR/suffix"},
		{"empty default", nil, "value: ${UNSET_VAR_99999:-}", "value: "},
		{"default with colons and slashes", nil, "url: ${UNSET_VAR_99999:-http://localhost:8080/path}", "url: http://localhost:8080/path"},
		{
			"nested in YAML",
			map[string]string{"PROXY_USER": "admin", "PROXY_PASS": "secret"},
			"proxies:\n  pool1:\n    endpoints:\n      - username: ${PROXY_USER}\n        password: ${PROXY_PASS}",
			"proxies:\n  pool1:\n    endpoints:\n      - username: admin\n        password: secret",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := ExpandEnv(tc.in); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
