package cmd

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/internal/testutil"
	"github.com/pithecene/quarry/types"
)

func runInit(t *testing.T, args ...string) (InitResponse, error) {
	t.Helper()
	out, err := runInitCLI(t, args...)
	var resp InitResponse
	if out != "" {
		if jerr := json.Unmarshal([]byte(out), &resp); jerr != nil {
			t.Fatalf("decode %q: %v", out, jerr)
		}
	}
	return resp, err
}

func runInitCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	app := cli.NewApp()
	app.Commands = []*cli.Command{InitCommand()}
	app.ExitErrHandler = func(*cli.Context, error) {}
	orig := os.Stdout
	rd, wr, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = wr
	runErr := app.Run(append([]string{"quarry", "init", "--format", "json"}, args...))
	_ = wr.Close()
	os.Stdout = orig
	out, _ := io.ReadAll(rd)
	return string(out), runErr
}

func TestInit_WritesProject(t *testing.T) {
	testutil.Serial(t, "runInitCLI swaps the process-wide os.Stdout")
	dir := filepath.Join(t.TempDir(), "My Scraper")

	resp, err := runInit(t, dir)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	want := []string{"package.json", ".npmrc", "mise.toml", ".gitignore", "links.ts", initStorageDir + "/"}
	if strings.Join(resp.Created, ",") != strings.Join(want, ",") {
		t.Fatalf("created = %v, want %v", resp.Created, want)
	}
	if len(resp.Next) != 3 || resp.Next[0] != "cd '"+dir+"'" || resp.Next[1] != "npm install" {
		t.Fatalf("next = %v", resp.Next)
	}
	if info, err := os.Stat(filepath.Join(dir, initStorageDir)); err != nil || !info.IsDir() {
		t.Fatalf("storage directory: %v", err)
	}

	var manifest struct {
		Name         string            `json:"name"`
		Private      bool              `json:"private"`
		Type         string            `json:"type"`
		Dependencies map[string]string `json:"dependencies"`
		AllowScripts map[string]bool   `json:"allowScripts"`
	}
	readJSON(t, filepath.Join(dir, "package.json"), &manifest)
	if manifest.Name != "my-scraper" || !manifest.Private || manifest.Type != "module" || !manifest.AllowScripts["puppeteer"] {
		t.Fatalf("manifest = %+v", manifest)
	}
	if got := manifest.Dependencies["@pithecene/quarry-sdk"]; got != "npm:@jsr/pithecene__quarry-sdk@^"+types.Version {
		t.Fatalf("sdk dependency = %q", got)
	}
	mise, err := os.ReadFile(filepath.Join(dir, "mise.toml"))
	if err != nil || !strings.Contains(string(mise), `"github:pithecene/quarry" = "`+types.Version+`"`) {
		t.Fatalf("mise.toml = %q, %v", mise, err)
	}
}

func TestInit_CurrentDirectoryHasNoCdStep(t *testing.T) {
	testutil.Serial(t, "runInitCLI swaps the process-wide os.Stdout")
	t.Chdir(t.TempDir())
	resp, err := runInit(t)
	if err != nil || resp.Directory != "." || resp.Next[0] != "npm install" {
		t.Fatalf("init . = %+v, %v", resp, err)
	}
}

func TestInit_RefusesExistingFilesAndWritesNothing(t *testing.T) {
	testutil.Serial(t, "runInitCLI swaps the process-wide os.Stdout")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := runInit(t, dir)
	if err == nil || !strings.Contains(err.Error(), "package.json") {
		t.Fatalf("err = %v, want one naming package.json", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("init wrote files despite the conflict: %v", entries)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "package.json"))
	if string(got) != "{}\n" {
		t.Fatalf("package.json was overwritten: %q", got)
	}
}

func TestInit_ReusesAnExistingStorageDirectory(t *testing.T) {
	testutil.Serial(t, "runInitCLI swaps the process-wide os.Stdout")
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, initStorageDir), 0o755); err != nil {
		t.Fatal(err)
	}
	resp, err := runInit(t, dir)
	if err != nil {
		t.Fatalf("init with an existing storage directory: %v", err)
	}
	if slices.Contains(resp.Created, initStorageDir+"/") {
		t.Fatalf("created = %v reports the reused storage directory", resp.Created)
	}
}

func TestInit_RejectsTwoDirectories(t *testing.T) {
	testutil.Serial(t, "runInitCLI swaps the process-wide os.Stdout")
	if _, err := runInit(t, t.TempDir(), t.TempDir()); err == nil {
		t.Fatal("init accepted two directories")
	}
	_, err := runInit(t, t.TempDir(), "--format", "yaml")
	if err == nil || !strings.Contains(err.Error(), "flags go before the directory") {
		t.Fatalf("flag after the directory: err = %v", err)
	}
}

func TestNpmPackageName(t *testing.T) {
	t.Parallel()
	for base, want := range map[string]string{
		"links":       "links",
		"My Scraper":  "my-scraper",
		"..":          "quarry-scripts",
		"_x_":         "x",
		"a/b":         "a-b",
		"scripts.v2":  "scripts.v2",
		"Ünïcode Dir": "n-code-dir",
	} {
		if got := npmPackageName(base); got != want {
			t.Errorf("npmPackageName(%q) = %q, want %q", base, got, want)
		}
	}
}

// TestInitPeerRanges_MatchExecutor keeps the ranges init writes equal to the
// executor's Puppeteer peer ranges.
func TestInitPeerRanges_MatchExecutor(t *testing.T) {
	t.Parallel()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate the test file")
	}
	var pkg struct {
		Peers map[string]string `json:"peerDependencies"`
	}
	readJSON(t, filepath.Join(filepath.Dir(file), "..", "..", "..", "executor-node", "package.json"), &pkg)
	for _, name := range []string{"puppeteer", "puppeteer-extra", "puppeteer-extra-plugin-stealth"} {
		if initPeerRanges[name] != pkg.Peers[name] {
			t.Errorf("init writes %s %q, executor peer range is %q", name, initPeerRanges[name], pkg.Peers[name])
		}
	}
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

func TestShellQuote(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"my-scraper":        "my-scraper",
		"./a/b_c.d":         "./a/b_c.d",
		"My Scraper":        "'My Scraper'",
		"it's":              `'it'\''s'`,
		"$(rm -rf x)":       "'$(rm -rf x)'",
		"~/scripts":         "'~/scripts'",
		"semi;colon&amp|pi": "'semi;colon&amp|pi'",
	} {
		if got := shellQuote(in); got != want {
			t.Errorf("shellQuote(%q) = %q, want %q", in, got, want)
		}
	}
}
