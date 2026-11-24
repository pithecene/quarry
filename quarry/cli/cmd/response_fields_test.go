package cmd

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/pithecene/quarry/cli/reader"
)

// A response renders the same fields, with the same names, in YAML as in JSON.
func TestResponses_YAMLFieldsMatchJSON(t *testing.T) {
	t.Parallel()
	username := "user"
	for _, tc := range []struct {
		name     string
		response any
	}{
		{"version", VersionResponse{}},
		{"init", InitResponse{Created: []string{"a"}, Next: []string{"b"}}},
		{"empty metrics", reader.MetricsSnapshot{}},
		{"metrics", reader.MetricsSnapshot{RunsStarted: 1, JobID: "job", DroppedByType: map[string]int64{"log": 1}}},
		{"proxy", reader.ResolveProxyResponse{}},
		{"proxy with a username", reader.ResolveProxyResponse{Endpoint: reader.ProxyEndpoint{Username: &username}}},
		{"run without sidecar files", reader.InspectRunResponse{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var asYAML, asJSON map[string]any
			data, err := yaml.Marshal(tc.response)
			if err != nil {
				t.Fatal(err)
			}
			if err := yaml.Unmarshal(data, &asYAML); err != nil {
				t.Fatal(err)
			}
			if data, err = json.Marshal(tc.response); err != nil {
				t.Fatal(err)
			}
			// YAML reads JSON, so both forms get the same Go types.
			if err := yaml.Unmarshal(bytes.TrimSpace(data), &asJSON); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(asYAML, asJSON) {
				t.Errorf("%T in YAML = %v, want %v", tc.response, asYAML, asJSON)
			}
		})
	}
}
