package render

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    Format
		wantErr bool
	}{
		{"json lowercase", "json", FormatJSON, false},
		{"json uppercase", "JSON", FormatJSON, false},
		{"table", "table", FormatTable, false},
		{"yaml", "yaml", FormatYAML, false},
		{"empty", "", "", false},
		{"invalid", "xml", "", true},
		{"invalid with message", "csv", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFormat(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseFormat(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseFormat(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseFormat_InvalidErrorMessage(t *testing.T) {
	t.Parallel()

	_, err := ParseFormat("xml")
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
	if !strings.Contains(err.Error(), "json, table, or yaml") {
		t.Errorf("error message should mention valid formats, got: %v", err)
	}
}

func TestRenderer_JSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	r := NewRendererWithWriter(FormatJSON, false, &buf)

	data := map[string]string{"key": "value"}
	if err := r.Render(data); err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	got := buf.String()
	if !strings.Contains(got, `"key"`) || !strings.Contains(got, `"value"`) {
		t.Errorf("JSON output missing expected content: %s", got)
	}
}

func TestRenderer_YAML(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	r := NewRendererWithWriter(FormatYAML, false, &buf)

	data := map[string]string{"key": "value"}
	if err := r.Render(data); err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	got := buf.String()
	if !strings.Contains(got, "key:") || !strings.Contains(got, "value") {
		t.Errorf("YAML output missing expected content: %s", got)
	}
}

func TestRenderer_Table_Struct(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	r := NewRendererWithWriter(FormatTable, false, &buf)

	type TestStruct struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	data := TestStruct{Name: "test", Value: 42}
	if err := r.Render(data); err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	got := buf.String()
	if !strings.Contains(got, "name:") || !strings.Contains(got, "test") {
		t.Errorf("Table output missing name field: %s", got)
	}
	if !strings.Contains(got, "value:") || !strings.Contains(got, "42") {
		t.Errorf("Table output missing value field: %s", got)
	}
}

func TestRenderer_Table_Slice(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	r := NewRendererWithWriter(FormatTable, false, &buf)

	type Item struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	data := []Item{
		{ID: "1", Name: "first"},
		{ID: "2", Name: "second"},
	}

	if err := r.Render(data); err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	got := buf.String()
	// Should have header row
	if !strings.Contains(got, "id") || !strings.Contains(got, "name") {
		t.Errorf("Table output missing headers: %s", got)
	}
	// Should have data rows
	if !strings.Contains(got, "first") || !strings.Contains(got, "second") {
		t.Errorf("Table output missing data: %s", got)
	}
}

func TestRenderer_Table_EmptySlice(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	r := NewRendererWithWriter(FormatTable, false, &buf)

	data := []string{}
	if err := r.Render(data); err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	got := buf.String()
	if !strings.Contains(got, "(no results)") {
		t.Errorf("Empty slice should show '(no results)', got: %s", got)
	}
}

func TestRenderer_NoColor_DoesNotAffectJSON(t *testing.T) {
	t.Parallel()

	// --no-color should not change JSON output
	var bufColor, bufNoColor bytes.Buffer

	rColor := NewRendererWithWriter(FormatJSON, false, &bufColor)
	rNoColor := NewRendererWithWriter(FormatJSON, true, &bufNoColor)

	data := map[string]string{"key": "value"}

	if err := rColor.Render(data); err != nil {
		t.Fatalf("Render with color failed: %v", err)
	}
	if err := rNoColor.Render(data); err != nil {
		t.Fatalf("Render without color failed: %v", err)
	}

	if bufColor.String() != bufNoColor.String() {
		t.Errorf("--no-color should not affect JSON output")
	}
}
