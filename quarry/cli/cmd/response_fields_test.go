package cmd

import (
	"reflect"
	"strings"
	"testing"

	"github.com/pithecene/quarry/cli/reader"
)

// A response has the same field names and omissions in YAML as in JSON.
func TestResponses_YAMLFieldsMatchJSON(t *testing.T) {
	t.Parallel()
	seen := make(map[reflect.Type]bool)
	var check func(typ reflect.Type)
	check = func(typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || seen[typ] || !strings.HasPrefix(typ.PkgPath(), "github.com/pithecene/quarry/") {
			return
		}
		seen[typ] = true
		for field := range typ.Fields() {
			if field.IsExported() && field.Tag.Get("yaml") != field.Tag.Get("json") {
				t.Errorf("%s.%s: yaml %q, json %q", typ, field.Name, field.Tag.Get("yaml"), field.Tag.Get("json"))
			}
			check(field.Type)
		}
	}
	for _, response := range []any{
		VersionResponse{}, InitResponse{}, reader.MetricsSnapshot{}, reader.ResolveProxyResponse{}, reader.InspectRunResponse{},
	} {
		check(reflect.TypeOf(response))
	}
}
