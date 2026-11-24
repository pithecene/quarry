package cmd

import (
	"encoding/json"
	"fmt"
	"os"
)

// parseJobPayload parses job payload from --job (inline) or --job-json (file).
// Using both flags is an explicit error. If neither is specified, returns empty object.
// The payload must be a top-level JSON object. Arrays, primitives, and null are
// rejected with actionable error messages.
func parseJobPayload(jobInline, jobFile string) (map[string]any, error) {
	hasInline := jobInline != ""
	hasFile := jobFile != ""

	// Conflict: both specified
	if hasInline && hasFile {
		return nil, fmt.Errorf(`cannot use both --job and --job-json

Provide job payload via ONE of:
  --job '{"key": "value"}'     (inline JSON)
  --job-json ./payload.json    (path to JSON file)`)
	}

	// Load from file
	if hasFile {
		data, err := os.ReadFile(jobFile)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf(`job file not found: %s

Ensure the file exists:
  ls -la %s`, jobFile, jobFile)
			}
			return nil, fmt.Errorf("cannot read job file %q: %w", jobFile, err)
		}

		var raw any
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf(`malformed JSON in job file %s: %w

The file must contain valid JSON. Example:
  {"url": "https://example.com", "page": 1}`, jobFile, err)
		}

		job, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf(`job file %s must contain a JSON object

The payload must be a top-level JSON object (not an array, string, number, or null).

Valid:
  {}
  {"url": "https://example.com"}

Invalid:
  []            (array)
  "string"      (primitive)
  123           (primitive)
  null          (null)

Received: %s`, jobFile, describeJSONType(raw))
		}
		return job, nil
	}

	// Parse inline JSON
	if hasInline {
		var raw any
		if err := json.Unmarshal([]byte(jobInline), &raw); err != nil {
			return nil, fmt.Errorf(`malformed --job JSON: %w

The --job flag must contain valid JSON. Examples:
  --job '{}'
  --job '{"key": "value"}'
  --job '{"url": "https://example.com", "page": 1}'

Received: %s`, err, jobInline)
		}

		job, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf(`--job must be a JSON object

The payload must be a top-level JSON object (not an array, string, number, or null).

Valid:
  --job '{}'
  --job '{"url": "https://example.com"}'

Invalid:
  --job '[]'            (array)
  --job '"string"'      (primitive)
  --job '123'           (primitive)
  --job 'null'          (null)

Received: %s`, describeJSONType(raw))
		}
		return job, nil
	}

	// Neither specified: empty object
	return map[string]any{}, nil
}

// describeJSONType returns a human-readable description of a JSON value's type.
func describeJSONType(v any) string {
	switch v := v.(type) {
	case nil:
		return "null"
	case map[string]any:
		return "object"
	case []any:
		return fmt.Sprintf("array (length %d)", len(v))
	case string:
		return fmt.Sprintf("string (%q)", v)
	case float64:
		return fmt.Sprintf("number (%v)", v)
	case bool:
		return fmt.Sprintf("boolean (%v)", v)
	default:
		return fmt.Sprintf("unknown (%T)", v)
	}
}
