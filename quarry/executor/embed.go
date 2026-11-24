// Package executor starts the Node executor.
//
// The executor bundle is embedded at build time and runs from memory: the
// launcher passes it to Node over a pipe, so no file exists for another user
// to replace, and no directory around it takes part in module resolution.
package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"os/exec"

	"github.com/pithecene/quarry/iox"
)

//go:embed bundle/executor.mjs
var embeddedExecutor []byte

// Embedded is the executor path that names the embedded bundle. It is not a
// file: Command runs it from memory.
const Embedded = "embedded:/executor.mjs"

// bootstrap reads the bundle from descriptor 3, closes it, and imports it as
// quarry:executor. That URL has no parent directory, so a bare import from the
// bundle resolves through --resolve-from alone, never a nearby node_modules.
//
// It runs as CommonJS or ESM, whichever Node defaults to, because workers
// inherit --input-type and then refuse a file. It removes the CommonJS
// bindings that Node makes global for -e, which module-format checks read. The
// lazy builtin globals stay, because a preload's look the same. It clears
// execArgv for forks, and inserts the argument that stands where Node puts a
// script's path. The import waits a turn because Node restores its module global
// when -e returns. Node before 22.15 has no registerHooks, and Node 26 warns
// about register.
const bootstrap = "const b=process.getBuiltinModule,{readFileSync,closeSync}=b('node:fs'),m=b('node:module');" +
	"const source=readFileSync(3,'utf8');closeSync(3);" +
	"if(m.registerHooks)m.registerHooks({" +
	"resolve:(s,c,n)=>s==='quarry:executor'?{url:s,format:'module',shortCircuit:true}:n(s,c)," +
	"load:(u,c,n)=>u==='quarry:executor'?{format:'module',source,shortCircuit:true}:n(u,c)});" +
	"else{const hook=`let source;export function initialize(d){source=d.source}" +
	"export function resolve(s,c,n){return s==='quarry:executor'?{url:s,format:'module',shortCircuit:true}:n(s,c)}" +
	"export function load(u,c,n){return u==='quarry:executor'?{format:'module',source,shortCircuit:true}:n(u,c)}`;" +
	"m.register('data:text/javascript,'+encodeURIComponent(hook),{data:{source}})}" +
	"process.execArgv=[];process.argv.splice(1,0,'quarry-executor');setImmediate(()=>{" +
	"const g=globalThis;" +
	"if(g.__filename==='[eval]')for(const k of['require','exports','__filename','__dirname'])delete g[k];" +
	"import('quarry:executor')})"

// IsEmbedded returns true if an executor is embedded in this binary.
func IsEmbedded() bool {
	return len(embeddedExecutor) > 0
}

// EmbeddedSHA256 returns the hex SHA-256 of the embedded bundle.
func EmbeddedSHA256() string {
	return fmt.Sprintf("%x", sha256.Sum256(embeddedExecutor))
}

// Process is an executor process. Its own methods replace every method of the
// Cmd within it that starts a process, so that each delivers the bundle.
type Process struct {
	*exec.Cmd
	bundle []byte // nil for an executor that is a file
}

// Command returns the process that runs the executor at path with args:
// the embedded bundle for Embedded, and otherwise the file at path. The
// embedded bundle uses descriptor 3, so stdin stays free for IPC. A caller that
// passes ExtraFiles leaves the first entry nil, whichever executor it runs.
func Command(ctx context.Context, path string, args ...string) *Process {
	if path != Embedded {
		return &Process{Cmd: exec.CommandContext(ctx, path, args...)}
	}
	nodeArgs := append([]string{"-e", bootstrap, "--"}, args...)
	return &Process{Cmd: exec.CommandContext(ctx, "node", nodeArgs...), bundle: embeddedExecutor}
}

// Start starts the process and, for the embedded bundle, delivers it.
func (p *Process) Start() error {
	if p.bundle == nil {
		return p.Cmd.Start()
	}
	if len(p.ExtraFiles) == 0 {
		p.ExtraFiles = make([]*os.File, 1)
	}
	if p.ExtraFiles[0] != nil {
		return errors.New("the embedded executor reserves descriptor 3")
	}
	r, w, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("executor bundle pipe: %w", err)
	}
	p.ExtraFiles[0] = r
	err = p.Cmd.Start()
	iox.DiscardClose(r) // the child holds its own descriptor
	if err != nil {
		iox.DiscardClose(w)
		return err
	}
	// The bundle can exceed the pipe's capacity, so it is written beside the
	// child. A child that ends first breaks the pipe, and its exit reports why.
	go func() {
		defer iox.DiscardClose(w)
		_, _ = w.Write(p.bundle)
	}()
	return nil
}

// Run starts the process and waits for it.
func (p *Process) Run() error {
	if err := p.Start(); err != nil {
		return err
	}
	return p.Wait()
}

// Output runs the process and returns its stdout. As exec.Cmd.Output does, it
// puts stderr in the ExitError when the caller set no Stderr.
func (p *Process) Output() ([]byte, error) {
	if p.Stdout != nil {
		return nil, errors.New("executor: Stdout already set")
	}
	var stdout bytes.Buffer
	var stderr tail
	p.Stdout = &stdout
	capture := p.Stderr == nil
	if capture {
		p.Stderr = &stderr
	}
	err := p.Run()
	var exit *exec.ExitError
	if capture && errors.As(err, &exit) {
		exit.Stderr = stderr
	}
	return stdout.Bytes(), err
}

// CombinedOutput runs the process and returns its stdout and stderr together.
func (p *Process) CombinedOutput() ([]byte, error) {
	if p.Stdout != nil || p.Stderr != nil {
		return nil, errors.New("executor: Stdout or Stderr already set")
	}
	var out bytes.Buffer
	p.Stdout, p.Stderr = &out, &out
	err := p.Run()
	return out.Bytes(), err
}

// tail keeps the last tailBytes written to it.
type tail []byte

const tailBytes = 64 << 10

func (t *tail) Write(b []byte) (int, error) {
	*t = append(*t, b...)
	if n := len(*t) - tailBytes; n > 0 {
		*t = append((*t)[:0], (*t)[n:]...)
	}
	return len(b), nil
}
