package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/pithecene/quarry/internal/testutil"
)

// debug resolve proxy reports the endpoint that the pool's strategy would
// select, with no committed field, and refuses a missing pool name and the
// removed --commit flag.
func TestDebugResolveProxy_PeeksAnEndpoint(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	config := filepath.Join(t.TempDir(), "proxies.json")
	pools := `[{"name":"default","strategy":"round_robin","endpoints":[{"protocol":"http","host":"a.example","port":8080},{"protocol":"http","host":"b.example","port":8080}]}]`
	if err := os.WriteFile(config, []byte(pools), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := runCLI(t, "debug", "resolve", "proxy", "--format", "json", "--proxy-config", config, "default")
	if err != nil {
		t.Fatalf("debug resolve proxy: %v", err)
	}
	var got map[string]map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	endpoint, ok := got["endpoint"]
	if !ok || len(got) != 1 {
		t.Fatalf("response %v, want only an endpoint", got)
	}
	// A fresh round-robin pool selects its first endpoint.
	if host := endpoint["host"]; host != "a.example" {
		t.Errorf("endpoint host = %v, want a.example, the round-robin pool's first endpoint", host)
	}
	if _, err := runCLI(t, "debug", "resolve", "proxy", "--proxy-config", config); err == nil {
		t.Error("debug resolve proxy without a pool name: want error")
	}
	if _, err := runCLI(t, "debug", "resolve", "proxy", "--commit", "--proxy-config", config, "default"); err == nil {
		t.Error("debug resolve proxy --commit: want an undefined flag error")
	}
}
