package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pithecene/quarry/internal/testutil"
)

// writeProxyPools writes a round-robin pool named default with two endpoints
// and returns the config path.
func writeProxyPools(t *testing.T) string {
	t.Helper()
	config := filepath.Join(t.TempDir(), "proxies.json")
	pools := `[{"name":"default","strategy":"round_robin","endpoints":[{"protocol":"http","host":"a.example","port":8080},{"protocol":"http","host":"b.example","port":8080}]}]`
	if err := os.WriteFile(config, []byte(pools), 0o600); err != nil {
		t.Fatal(err)
	}
	return config
}

// debug resolve proxy reports the endpoint that the pool's strategy would
// select, which a fresh round-robin pool takes from its first endpoint: json
// returns only the endpoint, and the table shows its fields.
func TestDebugResolveProxy_PeeksAnEndpoint(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	config := writeProxyPools(t)
	t.Run("json", func(t *testing.T) {
		out, err := runCLI(t, "debug", "resolve", "proxy", "--format", "json", "--proxy-config", config, "default")
		if err != nil {
			t.Fatalf("debug resolve proxy: %v", err)
		}
		var got map[string]map[string]any
		if err := json.Unmarshal([]byte(out), &got); err != nil {
			t.Fatalf("decode %q: %v", out, err)
		}
		if endpoint, ok := got["endpoint"]; !ok || len(got) != 1 || endpoint["host"] != "a.example" {
			t.Errorf("response %v, want only the endpoint a.example", got)
		}
	})
	t.Run("table", func(t *testing.T) {
		out, err := runCLI(t, "debug", "resolve", "proxy", "--format", "table", "--proxy-config", config, "default")
		if err != nil {
			t.Fatalf("debug resolve proxy: %v", err)
		}
		for _, want := range []string{"a.example", "8080", "http"} {
			if !strings.Contains(out, want) {
				t.Errorf("table %q lacks %q", out, want)
			}
		}
	})
}

// debug resolve proxy refuses a call without exactly one pool name, and the
// removed --commit flag before or after it, so an old invocation cannot
// appear to have committed a selection.
func TestDebugResolveProxy_RefusesBadArguments(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	config := writeProxyPools(t)
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no pool name", []string{"--proxy-config", config}, "pool name required"},
		{"--commit before the pool", []string{"--commit", "--proxy-config", config, "default"}, "flag provided but not defined"},
		{"--commit after the pool", []string{"--proxy-config", config, "default", "--commit"}, "takes one pool name"},
		{"two pool names", []string{"--proxy-config", config, "default", "other"}, "takes one pool name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := runCLI(t, append([]string{"debug", "resolve", "proxy"}, tc.args...)...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want one that contains %q", err, tc.want)
			}
			if out != "" {
				t.Errorf("wrote %q, want nothing", out)
			}
		})
	}
}
