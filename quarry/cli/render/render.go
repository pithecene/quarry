// Package render provides centralized output rendering for the quarry CLI.
//
// Format selection rules per CONTRACT_CLI.md:
//   - If output is a TTY, default to table
//   - If output is not a TTY, default to json
//   - --format flag always overrides defaults
//   - Invalid formats are errors
//
// Color handling:
//   - --no-color affects table output only
//   - TUI mode is unaffected by --no-color (uses its own styling)
package render

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"text/tabwriter"

	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"

	"github.com/pithecene/quarry/cli/tui"
	"github.com/pithecene/quarry/iox"
)

// Format represents an output format.
type Format string

// Supported formats per CONTRACT_CLI.md.
const (
	FormatJSON  Format = "json"
	FormatTable Format = "table"
	FormatYAML  Format = "yaml"
)

// ParseFormat parses a format string, returning an error for invalid formats.
func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(s) {
	case "json":
		return FormatJSON, nil
	case "table":
		return FormatTable, nil
	case "yaml":
		return FormatYAML, nil
	case "":
		return "", nil // Let caller decide default
	default:
		return "", fmt.Errorf("invalid format: %q (must be json, table, or yaml)", s)
	}
}

// Renderer handles output formatting.
type Renderer struct {
	format  Format
	noColor bool
	out     io.Writer
}

// NewRenderer creates a renderer from CLI context.
// Applies format selection rules per CONTRACT_CLI.md.
func NewRenderer(c *cli.Context) (*Renderer, error) {
	formatStr := c.String("format")
	format, err := ParseFormat(formatStr)
	if err != nil {
		return nil, err
	}

	// Apply default format based on TTY detection
	if format == "" {
		if isTTY(os.Stdout) {
			format = FormatTable
		} else {
			format = FormatJSON
		}
	}

	return &Renderer{
		format:  format,
		noColor: c.Bool("no-color"),
		out:     os.Stdout,
	}, nil
}

// NewRendererWithWriter creates a renderer with a custom writer (for testing).
func NewRendererWithWriter(format Format, noColor bool, out io.Writer) *Renderer {
	return &Renderer{
		format:  format,
		noColor: noColor,
		out:     out,
	}
}

// Format returns the renderer's output format.
func (r *Renderer) Format() Format {
	return r.format
}

// Render outputs the data in the configured format.
func (r *Renderer) Render(data any) error {
	switch r.format {
	case FormatJSON:
		return r.renderJSON(data)
	case FormatTable:
		return r.renderTable(data)
	case FormatYAML:
		return r.renderYAML(data)
	default:
		return fmt.Errorf("unknown format: %s", r.format)
	}
}

// RenderTUI initiates TUI mode for the given view type.
// Per CONTRACT_CLI.md, TUI is opt-in only and read-only only.
func (r *Renderer) RenderTUI(viewType string, data any) error {
	// Validate TUI is supported for this view type
	if !tui.IsTUISupported(viewType) {
		return fmt.Errorf("--tui is not supported for %s", viewType)
	}

	// Run the TUI
	return tui.Run(viewType, data)
}

func (r *Renderer) renderJSON(data any) error {
	enc := json.NewEncoder(r.out)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}

func (r *Renderer) renderYAML(data any) error {
	enc := yaml.NewEncoder(r.out)
	enc.SetIndent(2)
	return enc.Encode(data)
}

func (r *Renderer) renderTable(data any) error {
	// Handle slice of items
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Slice {
		return r.renderSliceTable(v)
	}

	// Handle single struct/map
	return r.renderStructTable(data)
}

func (r *Renderer) renderSliceTable(v reflect.Value) error {
	if v.Len() == 0 {
		_, _ = fmt.Fprintln(r.out, "(no results)")
		return nil
	}

	w := tabwriter.NewWriter(r.out, 0, 0, 2, ' ', 0)
	defer iox.DiscardErr(w.Flush)

	// Get headers from first element
	first := v.Index(0)
	headers := r.getHeaders(first)

	// Print header row
	_, _ = fmt.Fprintln(w, strings.Join(headers, "\t"))

	// Print data rows
	for i := 0; i < v.Len(); i++ {
		row := r.getRowValues(v.Index(i), headers)
		_, _ = fmt.Fprintln(w, strings.Join(row, "\t"))
	}

	return nil
}

func (r *Renderer) renderStructTable(data any) error {
	w := tabwriter.NewWriter(r.out, 0, 0, 2, ' ', 0)
	defer iox.DiscardErr(w.Flush)

	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			name := r.getFieldName(field)
			val := r.formatValue(v.Field(i))
			_, _ = fmt.Fprintf(w, "%s:\t%s\n", name, val)
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			key := fmt.Sprintf("%v", iter.Key().Interface())
			val := r.formatValue(iter.Value())
			_, _ = fmt.Fprintf(w, "%s:\t%s\n", key, val)
		}
	default:
		_, _ = fmt.Fprintf(w, "%v\n", data)
	}

	return nil
}

func (r *Renderer) getHeaders(v reflect.Value) []string {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	var headers []string
	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			headers = append(headers, r.getFieldName(t.Field(i)))
		}
	case reflect.Map:
		// For maps, use keys as headers
		for _, key := range v.MapKeys() {
			headers = append(headers, fmt.Sprintf("%v", key.Interface()))
		}
	}
	return headers
}

func (r *Renderer) getRowValues(v reflect.Value, headers []string) []string {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	var values []string
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			values = append(values, r.formatValue(v.Field(i)))
		}
	case reflect.Map:
		for _, h := range headers {
			val := v.MapIndex(reflect.ValueOf(h))
			if val.IsValid() {
				values = append(values, r.formatValue(val))
			} else {
				values = append(values, "")
			}
		}
	}
	return values
}

func (r *Renderer) getFieldName(f reflect.StructField) string {
	// Prefer json tag name
	if tag := f.Tag.Get("json"); tag != "" {
		parts := strings.Split(tag, ",")
		if parts[0] != "" && parts[0] != "-" {
			return parts[0]
		}
	}
	return strings.ToLower(f.Name)
}

func (r *Renderer) formatValue(v reflect.Value) string {
	if !v.IsValid() {
		return ""
	}

	// Handle pointers
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}

	// Handle special types
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		if v.Len() == 0 {
			return "[]"
		}
		return fmt.Sprintf("[%d items]", v.Len())
	case reflect.Map:
		if v.Len() == 0 {
			return "{}"
		}
		return fmt.Sprintf("{%d keys}", v.Len())
	case reflect.Struct:
		// Check for time.Time
		if v.Type().String() == "time.Time" {
			return fmt.Sprintf("%v", v.Interface())
		}
		return "{...}"
	default:
		return fmt.Sprintf("%v", v.Interface())
	}
}

// isTTY returns true if the writer is a TTY.
func isTTY(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
