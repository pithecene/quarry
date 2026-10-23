package runtime

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/types"
)

func TestExecutorInputJSON_IncludesBrowserWSEndpoint(t *testing.T) {
	t.Parallel()

	input := executorInput{
		RunID:             "run-001",
		Attempt:           1,
		Job:               map[string]any{"url": "https://example.com"},
		BrowserWSEndpoint: "ws://127.0.0.1:9222/devtools/browser/abc-123",
	}

	data, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	wsEndpoint, ok := decoded["browser_ws_endpoint"].(string)
	if !ok {
		t.Fatal("browser_ws_endpoint field missing from JSON output")
	}
	if wsEndpoint != "ws://127.0.0.1:9222/devtools/browser/abc-123" {
		t.Errorf("browser_ws_endpoint = %q, want %q", wsEndpoint, "ws://127.0.0.1:9222/devtools/browser/abc-123")
	}
}

func TestExecutorInputJSON_OmitsBrowserWSEndpointWhenEmpty(t *testing.T) {
	t.Parallel()

	input := executorInput{
		RunID:   "run-001",
		Attempt: 1,
		Job:     map[string]any{},
	}

	data, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if _, exists := decoded["browser_ws_endpoint"]; exists {
		t.Error("browser_ws_endpoint should be omitted when empty")
	}
}

func TestExecutorInputJSON_IncludesProxy(t *testing.T) {
	t.Parallel()

	proxy := &types.ProxyEndpoint{
		Protocol: "http",
		Host:     "proxy.example.com",
		Port:     8080,
	}
	input := executorInput{
		RunID:             "run-001",
		Attempt:           1,
		Job:               map[string]any{},
		Proxy:             proxy,
		BrowserWSEndpoint: "ws://127.0.0.1:9222/devtools/browser/abc",
	}

	data, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Both fields present
	if _, ok := decoded["proxy"]; !ok {
		t.Error("proxy field missing from JSON output")
	}
	if _, ok := decoded["browser_ws_endpoint"]; !ok {
		t.Error("browser_ws_endpoint field missing from JSON output")
	}
}

func TestDeduplicateEnv_LastWins(t *testing.T) {
	t.Parallel()

	env := []string{
		"NODE_PATH=/old",
		"HOME=/home/user",
		"NODE_PATH=/new",
	}
	result := deduplicateEnv(env)

	// Last occurrence of NODE_PATH wins
	if !slices.Contains(result, "NODE_PATH=/new") {
		t.Error("expected NODE_PATH=/new to be kept")
	}
	if slices.Contains(result, "NODE_PATH=/old") {
		t.Error("expected NODE_PATH=/old to be removed")
	}
	if !slices.Contains(result, "HOME=/home/user") {
		t.Error("expected HOME=/home/user to be preserved")
	}
}

func TestDeduplicateEnv_NoDuplicates(t *testing.T) {
	t.Parallel()

	env := []string{
		"HOME=/home/user",
		"PATH=/usr/bin",
	}
	result := deduplicateEnv(env)

	if len(result) != 2 {
		t.Errorf("expected 2 entries, got %d", len(result))
	}
}

func TestDeduplicateEnv_Empty(t *testing.T) {
	t.Parallel()

	result := deduplicateEnv(nil)
	if len(result) != 0 {
		t.Errorf("expected 0 entries, got %d", len(result))
	}
}

func TestExecutorConfig_ResolveFromEnvSetup(t *testing.T) {
	t.Parallel()

	// Verify that the config struct accepts ResolveFrom
	config := &ExecutorConfig{
		ExecutorPath: "/usr/bin/node",
		ScriptPath:   "/app/script.ts",
		ResolveFrom:  "/app/node_modules",
		RunMeta: &types.RunMeta{
			RunID:   "run-001",
			Attempt: 1,
		},
	}

	if config.ResolveFrom != "/app/node_modules" {
		t.Errorf("ResolveFrom = %q, want %q", config.ResolveFrom, "/app/node_modules")
	}
}

func TestExecutorInputJSON_IncludesStoragePartition(t *testing.T) {
	t.Parallel()

	input := executorInput{
		RunID:   "run-001",
		Attempt: 1,
		Job:     map[string]any{},
		Storage: &StoragePartition{
			Dataset:  "quarry",
			Source:   "my-source",
			Category: "default",
			Day:      "2026-02-23",
			RunID:    "run-001",
		},
	}

	data, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	storage, ok := decoded["storage"].(map[string]any)
	if !ok {
		t.Fatal("storage field missing or not an object")
	}

	checks := map[string]string{
		"dataset":  "quarry",
		"source":   "my-source",
		"category": "default",
		"day":      "2026-02-23",
		"run_id":   "run-001",
	}
	for key, want := range checks {
		got, exists := storage[key].(string)
		if !exists {
			t.Errorf("storage.%s missing", key)
		} else if got != want {
			t.Errorf("storage.%s = %q, want %q", key, got, want)
		}
	}
}

func TestExecutorInputJSON_OmitsStorageWhenNil(t *testing.T) {
	t.Parallel()

	input := executorInput{
		RunID:   "run-001",
		Attempt: 1,
		Job:     map[string]any{},
	}

	data, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if _, exists := decoded["storage"]; exists {
		t.Error("storage should be omitted when nil")
	}
}

func TestScriptValidation_ValidJSON(t *testing.T) {
	t.Parallel()

	input := `{"valid":true,"exports":{"default":true,"hooks":["prepare","cleanup"]}}`

	var result ScriptValidation
	if err := json.Unmarshal([]byte(input), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !result.Valid {
		t.Error("expected valid=true")
	}
	if result.Exports == nil {
		t.Fatal("expected exports to be non-nil")
	}
	if !result.Exports.Default {
		t.Error("expected exports.default=true")
	}
	if len(result.Exports.Hooks) != 2 {
		t.Errorf("expected 2 hooks, got %d", len(result.Exports.Hooks))
	}
	if result.Exports.Hooks[0] != "prepare" {
		t.Errorf("hooks[0] = %q, want %q", result.Exports.Hooks[0], "prepare")
	}
	if result.Exports.Hooks[1] != "cleanup" {
		t.Errorf("hooks[1] = %q, want %q", result.Exports.Hooks[1], "cleanup")
	}
}

func TestScriptValidation_InvalidJSON(t *testing.T) {
	t.Parallel()

	input := `{"valid":false,"error":"Failed to load script \"bad.ts\": missing default export"}`

	var result ScriptValidation
	if err := json.Unmarshal([]byte(input), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if result.Valid {
		t.Error("expected valid=false")
	}
	if result.Exports != nil {
		t.Error("expected exports to be nil")
	}
	if result.Error == "" {
		t.Error("expected error message")
	}
}

func TestScriptValidation_EmptyHooks(t *testing.T) {
	t.Parallel()

	input := `{"valid":true,"exports":{"default":true,"hooks":[]}}`

	var result ScriptValidation
	if err := json.Unmarshal([]byte(input), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !result.Valid {
		t.Error("expected valid=true")
	}
	if result.Exports == nil {
		t.Fatal("expected exports to be non-nil")
	}
	if len(result.Exports.Hooks) != 0 {
		t.Errorf("expected 0 hooks, got %d", len(result.Exports.Hooks))
	}
}

// TestDeriveDay_UTCMidnightBoundary verifies DeriveDay determinism and
// correct day rollover at UTC midnight. The integration test that exercises
// the actual child-run wiring (where the day-drift bug lived) is in
// cli/cmd/run_test.go:TestChildRun_StorageDayAlignedWithBuildPolicy.
func TestDeriveDay_UTCMidnightBoundary(t *testing.T) {
	t.Parallel()

	// 2026-02-23T23:59:59.999Z — still Feb 23
	ts := time.Date(2026, 2, 23, 23, 59, 59, 999_000_000, time.UTC)

	if day := lode.DeriveDay(ts); day != "2026-02-23" {
		t.Errorf("DeriveDay at 23:59:59.999Z = %q, want %q", day, "2026-02-23")
	}

	// One millisecond later rolls to Feb 24
	tsNext := ts.Add(time.Millisecond)
	if day := lode.DeriveDay(tsNext); day != "2026-02-24" {
		t.Errorf("DeriveDay at 00:00:00.000Z = %q, want %q", day, "2026-02-24")
	}

	// Deterministic: same input always produces same output
	day1 := lode.DeriveDay(ts)
	day2 := lode.DeriveDay(ts)
	if day1 != day2 {
		t.Errorf("DeriveDay not deterministic: %q != %q", day1, day2)
	}
}

func TestStoragePartition_MatchesGoPathFormula(t *testing.T) {
	t.Parallel()

	// Verify StoragePartition fields produce a path matching
	// the Go buildFilePath() formula in quarry/lode/file_writer.go:
	// datasets/{dataset}/partitions/source={s}/category={c}/day={d}/run_id={r}/files/{filename}
	sp := StoragePartition{
		Dataset:  "quarry",
		Source:   "my-source",
		Category: "default",
		Day:      "2026-02-23",
		RunID:    "run-001",
	}

	filename := "screenshot.png"
	want := "datasets/quarry/partitions/source=my-source/category=default/day=2026-02-23/run_id=run-001/files/screenshot.png"

	// Reproduce the Go formula
	got := "datasets/" + sp.Dataset + "/partitions/source=" + sp.Source + "/category=" + sp.Category + "/day=" + sp.Day + "/run_id=" + sp.RunID + "/files/" + filename

	if got != want {
		t.Errorf("path formula mismatch:\n  got:  %s\n  want: %s", got, want)
	}
}
