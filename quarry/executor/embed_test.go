package executor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/quarry/internal/testutil"
)

// embeddedProcess returns a process that runs source as the embedded bundle,
// in an empty directory, with a timeout.
func embeddedProcess(t *testing.T, source string, args ...string) *Process {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not available")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	p := Command(ctx, Embedded, args...)
	p.bundle = []byte(source)
	p.Dir = t.TempDir()
	return p
}

// output returns the trimmed stdout of p, and fails with its stderr.
func output(t *testing.T, p *Process) string {
	t.Helper()
	var stderr strings.Builder
	p.Stderr = &stderr
	out, err := p.Output()
	if err != nil {
		t.Fatalf("embedded executor: %v\n%s", err, stderr.String())
	}
	return strings.TrimSpace(string(out))
}

// writePackage writes a package that exports 1 under root/node_modules.
func writePackage(t *testing.T, root, name string) {
	t.Helper()
	dir := filepath.Join(root, "node_modules", name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("module.exports = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// The bundle arrives on its own pipe, larger than the pipe holds, so stdin
// carries only the caller's input, and the arguments stand where a script's
// would.
func TestCommand_RunsTheBundleFromItsOwnPipe(t *testing.T) {
	t.Parallel()
	source := "import { readFileSync } from 'node:fs'\n" +
		"console.log([readFileSync(0, 'utf8'), ...process.argv.slice(2)].join('|'))\n//" +
		strings.Repeat("x", 1<<20)
	p := embeddedProcess(t, source, "--flag", "value")
	p.Stdin = strings.NewReader("ipc")
	if got := output(t, p); got != "ipc|--flag|value" {
		t.Errorf("output %q, want the stdin and the arguments", got)
	}
}

// A bare import from the bundle never finds a package in the working
// directory, an ancestor of it, or NODE_PATH.
func TestCommand_IgnoresAmbientPackages(t *testing.T) {
	testutil.Serial(t, "calls t.Setenv, which changes the process environment")
	p := embeddedProcess(t, "import('planted').then(() => console.log('loaded'), () => console.log('refused'))")
	writePackage(t, p.Dir, "planted")
	t.Setenv("NODE_PATH", filepath.Join(p.Dir, "node_modules"))
	inner := filepath.Join(p.Dir, "inner")
	if err := os.Mkdir(inner, 0o700); err != nil {
		t.Fatal(err)
	}
	p.Dir = inner
	if got := output(t, p); got != "refused" {
		t.Errorf("import of a planted package: %q, want refused", got)
	}
}

// Cancelling the context ends the process.
func TestCommand_CancellationEndsTheProcess(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not available")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	p := Command(ctx, Embedded)
	p.bundle = []byte("console.log('ready'); setInterval(() => {}, 1000)")
	stdout, err := p.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := stdout.Read(make([]byte, 5)); err != nil {
		t.Fatal(err)
	}
	cancel()
	ended := make(chan error, 1)
	go func() { ended <- p.Wait() }()
	select {
	case err := <-ended:
		if err == nil {
			t.Error("Wait succeeded, want the cancellation")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the process outlived its context")
	}
}

// A child that never reads the bundle still reports its own exit.
func TestCommand_ReportsAChildThatNeverReads(t *testing.T) {
	t.Parallel()
	p := &Process{Cmd: exec.CommandContext(t.Context(), "/bin/sh", "-c", "exit 7"), bundle: make([]byte, 1<<20)}
	_, err := p.Output()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 7 {
		t.Errorf("Output = %v, want exit code 7", err)
	}
}

// The shipped bundle validates a script whose dependency only the module root
// provides, and refuses it without that root.
func TestCommand_HonorsTheModuleRoot(t *testing.T) {
	t.Parallel()
	for _, rooted := range []bool{true, false} {
		dir, root := t.TempDir(), t.TempDir()
		writePackage(t, root, "only-in-root")
		script := filepath.Join(dir, "script.mjs")
		if err := os.WriteFile(script, []byte("import 'only-in-root'\nexport default async function run() {}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		p := embeddedProcess(t, string(embeddedExecutor), "--validate", script)
		p.Env = append(os.Environ(), "QUARRY_RESOLVE_FROM=")
		if rooted {
			p.Env = append(os.Environ(), "QUARRY_RESOLVE_FROM="+root)
		}
		out, err := p.Output()
		var result struct {
			Valid bool
			Error string
		}
		if jsonErr := json.Unmarshal(out, &result); jsonErr != nil {
			t.Fatalf("module root %t: output %q: %v (%v)", rooted, out, jsonErr, err)
		}
		wantExit := 1 // a script that does not load
		if rooted {
			wantExit = 0
		}
		if code := p.ProcessState.ExitCode(); result.Valid != rooted || code != wantExit {
			t.Errorf("module root %t: valid %t, exit %d, want %d (%v)", rooted, result.Valid, code, wantExit, err)
		}
		if !rooted && !strings.Contains(result.Error, "only-in-root") {
			t.Errorf("error %q does not name the missing package", result.Error)
		}
	}
}

// Workers and forked processes start as they would from an executor file, and
// the globals that Node adds for -e are gone.
func TestCommand_LeavesChildLaunchesOrdinary(t *testing.T) {
	t.Parallel()
	p := embeddedProcess(t, `import { Worker } from 'node:worker_threads'
import { fork } from 'node:child_process'
import { once } from 'node:events'
import { pathToFileURL } from 'node:url'
const child = process.cwd() + '/child.mjs'
const [worker] = await once(new Worker(pathToFileURL(child)), 'exit')
const [forked] = await once(fork(child), 'exit')
console.log(worker, forked, typeof globalThis.require, typeof globalThis.module, typeof globalThis.fs)`)
	if err := os.WriteFile(filepath.Join(p.Dir, "child.mjs"), []byte("export {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, want := output(t, p), "0 0 undefined undefined undefined"; got != want {
		t.Errorf("output %q, want %q", got, want)
	}
}

// Every method that starts the process delivers the bundle, and Output keeps
// stderr in the exit error.
func TestProcess_StartMethodsDeliverTheBundle(t *testing.T) {
	t.Parallel()
	if err := embeddedProcess(t, "process.exit(0)").Run(); err != nil {
		t.Errorf("Run: %v", err)
	}
	out, err := embeddedProcess(t, "console.log('a'); console.error('b')").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "a") || !strings.Contains(string(out), "b") {
		t.Errorf("CombinedOutput = %q, %v", out, err)
	}
	_, err = embeddedProcess(t, "console.error('why'); process.exit(3)").Output()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || strings.TrimSpace(string(exit.Stderr)) != "why" {
		t.Errorf("Output error = %v, want an exit error with stderr", err)
	}
}

// A shared browser loads a peer whose own ESM import only the module root
// provides. The peer stands for Puppeteer.
func TestCommand_SharedBrowserHonorsTheModuleRoot(t *testing.T) {
	t.Parallel()
	dir, root := t.TempDir(), t.TempDir()
	writePackage(t, root, "only-in-root")
	writePackage(t, dir, "puppeteer")
	peer := filepath.Join(dir, "node_modules", "puppeteer")
	for name, body := range map[string]string{
		"package.json": `{"type":"module","main":"index.js"}`,
		"index.js":     "import 'only-in-root'\nthrow new Error('peer loaded')\n",
	} {
		if err := os.WriteFile(filepath.Join(peer, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	p := embeddedProcess(t, string(embeddedExecutor), "--launch-browser", filepath.Join(dir, "script.mjs"))
	p.Env = append(os.Environ(), "QUARRY_RESOLVE_FROM="+root)
	out, err := p.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "peer loaded") {
		t.Errorf("--launch-browser: %v, output %q, want the peer's own error", err, out)
	}
}

// The shipped bundle finds a peer through NODE_PATH, which the operator sets,
// and never in a folder that Node searches for every user.
func TestCommand_PeerLookupSkipsGlobalFolders(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		variable, value string
		loads           bool
	}{
		{"HOME", "", false},
		{"NODE_PATH", ".node_modules", true},
	} {
		home, dir := t.TempDir(), t.TempDir()
		// An incomplete local package does not admit a global one.
		if err := os.MkdirAll(filepath.Join(dir, "node_modules", "puppeteer"), 0o700); err != nil {
			t.Fatal(err)
		}
		peer := filepath.Join(home, ".node_modules", "puppeteer")
		if err := os.MkdirAll(peer, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(peer, "index.js"), []byte("throw new Error('peer loaded')\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		p := embeddedProcess(t, string(embeddedExecutor), "--launch-browser", filepath.Join(dir, "script.mjs"))
		p.Env = append(os.Environ(), "QUARRY_RESOLVE_FROM=", "NODE_PATH=", tc.variable+"="+filepath.Join(home, tc.value))
		out, err := p.CombinedOutput()
		if got := strings.Contains(string(out), "peer loaded"); err == nil || got != tc.loads {
			t.Errorf("%s: loaded %t, want %t (%v)\n%s", tc.variable, got, tc.loads, err, out)
		}
	}
}

// The executor's own fallback stays inside the module root: a package above
// the root does not load.
func TestCommand_PeerFallbackStaysInTheModuleRoot(t *testing.T) {
	t.Parallel()
	for _, inRoot := range []bool{true, false} {
		above, dir := t.TempDir(), t.TempDir()
		root := filepath.Join(above, "inner", "node_modules")
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
		holder := above
		if inRoot {
			holder = filepath.Join(above, "inner")
		}
		writePackage(t, holder, "puppeteer")
		body := []byte("throw new Error('peer loaded')\n")
		if err := os.WriteFile(filepath.Join(holder, "node_modules", "puppeteer", "index.js"), body, 0o600); err != nil {
			t.Fatal(err)
		}
		p := embeddedProcess(t, string(embeddedExecutor), "--launch-browser", filepath.Join(dir, "script.mjs"))
		p.Env = append(os.Environ(), "NODE_PATH=", "QUARRY_RESOLVE_FROM="+root)
		out, err := p.CombinedOutput()
		if got := strings.Contains(string(out), "peer loaded"); err == nil || got != inRoot {
			t.Errorf("package in the root %t: loaded %t (%v)\n%s", inRoot, got, err, out)
		}
	}
}

// A preload's globals survive the cleanup, and Node's default module type does
// not decide whether the bootstrap starts.
func TestCommand_HonorsNodeOptions(t *testing.T) {
	t.Parallel()
	p := embeddedProcess(t, "console.log(globalThis.fs, typeof globalThis.path)")
	preload := filepath.Join(p.Dir, "preload.cjs")
	if err := os.WriteFile(preload, []byte("globalThis.fs = 'kept'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p.Env = append(os.Environ(), "NODE_OPTIONS=--require="+preload)
	if got := output(t, p); got != "kept undefined" {
		t.Errorf("output %q, want the preload's fs and no path", got)
	}
	p = embeddedProcess(t, "console.log('ran')")
	p.Args = append([]string{p.Args[0], "--input-type=module"}, p.Args[1:]...)
	if got := output(t, p); got != "ran" {
		t.Errorf("as ESM: output %q, want ran", got)
	}
}
