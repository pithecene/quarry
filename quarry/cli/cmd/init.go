package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/cli/render"
	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

// InitResponse is the response for the init command.
type InitResponse struct {
	Directory string   `json:"directory"`
	Created   []string `json:"created"`
	Next      []string `json:"next"`
}

// initNodeRange is the Node.js range of a generated project: native type
// stripping is on by default from 22.18 and 23.6, and Puppeteer 25 requires
// 22.12. The .npmrc sets engine-strict, so npm install fails at once on any
// other Node.js instead of installing a project that cannot run. init starts
// no process, so it cannot check the version itself.
const initNodeRange = ">=22.18 <23 || >=23.6"

// initStorageDir is the storage directory init creates and the example run
// command writes to.
const initStorageDir = "quarry-data"

// initPeerRanges are the executor's Puppeteer peer ranges
// (executor-node/package.json peerDependencies, which a test compares),
// plus proxy-agent, which Puppeteer needs to download its browser through a
// proxy. One npm install brings them in before Puppeteer's install script
// runs, so a proxied install works without a separate step.
var initPeerRanges = map[string]string{
	"proxy-agent":                    "^8",
	"puppeteer":                      "^24 || ^25",
	"puppeteer-extra":                "^3",
	"puppeteer-extra-plugin-stealth": "^2",
}

const initExampleScript = `import type { QuarryContext } from '@pithecene/quarry-sdk'

export default async function run(ctx: QuarryContext<{ url: string }>): Promise<void> {
  await ctx.page.goto(ctx.job.url)

  const links = await ctx.page.$$eval('a', (anchors) =>
    anchors.map((anchor) => ({
      text: anchor.textContent?.trim() ?? '',
      href: anchor.href
    }))
  )

  for (const link of links) {
    await ctx.emit.item({
      item_type: 'link',
      data: link
    })
  }

  await ctx.emit.runComplete()
}
`

// npmNameInvalid matches the characters an npm package name cannot hold.
var npmNameInvalid = regexp.MustCompile(`[^a-z0-9._-]+`)

// InitCommand returns the init command, which writes a script project
// (CONTRACT_CLI.md §init).
func InitCommand() *cli.Command {
	return &cli.Command{
		Name:      "init",
		Usage:     "Create a script project with the SDK, Puppeteer, and an example script (writes files)",
		ArgsUsage: "[directory]",
		Flags:     []cli.Flag{FormatFlag, NoColorFlag},
		Action:    initAction,
	}
}

func initAction(c *cli.Context) error {
	r, err := render.NewRenderer(c)
	if err != nil {
		return err
	}
	if c.NArg() > 1 {
		// Flags are parsed only before the first argument, so a flag after the
		// directory arrives as an argument.
		if strings.HasPrefix(c.Args().Get(1), "-") {
			return cli.Exit("flags go before the directory: quarry init [flags] [directory]", 1)
		}
		return cli.Exit("init takes at most one directory", 1)
	}
	dir := c.Args().First()
	if dir == "" {
		dir = "."
	}
	files, err := initFiles(dir)
	if err != nil {
		return cli.Exit(err.Error(), 1)
	}
	created, err := writeInitProject(dir, files)
	if err != nil {
		return cli.Exit(err.Error(), 1)
	}
	return renderInit(r, InitResponse{Directory: dir, Created: created, Next: initNextSteps(dir)})
}

// initStep is one next command, as a table row.
type initStep struct {
	Next string `json:"next"`
}

// renderInit renders the response. The table prints the files created to
// stderr and the next commands as rows, since a table cell shows a list only
// as its length.
func renderInit(r *render.Renderer, resp InitResponse) error {
	if r.Format() != render.FormatTable {
		return r.Render(resp)
	}
	fmt.Fprintf(os.Stderr, "Created in %s: %s\n", resp.Directory, strings.Join(resp.Created, ", "))
	steps := make([]initStep, len(resp.Next))
	for i, s := range resp.Next {
		steps[i] = initStep{Next: s}
	}
	return r.Render(steps)
}

// initFile is one file init writes, by path relative to the project.
type initFile struct {
	path     string
	contents string
}

// initFiles returns the files of a new project in dir, in the order written.
func initFiles(dir string) ([]initFile, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", dir, err)
	}
	manifest, err := initPackageJSON(npmPackageName(filepath.Base(abs)))
	if err != nil {
		return nil, err
	}
	return []initFile{
		{"package.json", manifest},
		{".npmrc", "@jsr:registry=https://npm.jsr.io\nengine-strict=true\n"},
		{"mise.toml", fmt.Sprintf("[tools]\n\"github:pithecene/quarry\" = %q\n", types.Version)},
		{".gitignore", "node_modules/\n" + initStorageDir + "/\n"},
		{"links.ts", initExampleScript},
	}, nil
}

// initPackageJSON renders the project manifest. The SDK comes from JSR at the
// CLI's own version, so the two stay in lockstep. allowScripts approves
// Puppeteer's install script, which downloads its browser, for npm releases
// that hold back unapproved install scripts.
func initPackageJSON(name string) (string, error) {
	deps := map[string]string{"@pithecene/quarry-sdk": "npm:@jsr/pithecene__quarry-sdk@^" + types.Version}
	for pkg, rng := range initPeerRanges {
		deps[pkg] = rng
	}
	// HTML escaping is off, so that ranges such as >=22.18 stay readable.
	var out strings.Builder
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	err := enc.Encode(struct {
		Name         string            `json:"name"`
		Private      bool              `json:"private"`
		Type         string            `json:"type"`
		Engines      map[string]string `json:"engines"`
		Dependencies map[string]string `json:"dependencies"`
		AllowScripts map[string]bool   `json:"allowScripts"`
	}{name, true, "module", map[string]string{"node": initNodeRange}, deps, map[string]bool{"puppeteer": true}})
	if err != nil {
		return "", fmt.Errorf("render package.json: %w", err)
	}
	return out.String(), nil
}

// npmPackageName derives a valid npm package name from a directory name.
func npmPackageName(base string) string {
	name := strings.Trim(npmNameInvalid.ReplaceAllString(strings.ToLower(base), "-"), "-._")
	if name == "" {
		return "quarry-scripts"
	}
	return name
}

// writeInitProject writes files into dir and creates the storage directory.
// It writes nothing when any file already exists, and it never replaces a
// file: each is created exclusively, so a file that appears meanwhile fails
// the write instead of being overwritten.
func writeInitProject(dir string, files []initFile) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create %s: %w", dir, err)
	}
	if existing := existingInitPaths(dir, files); len(existing) > 0 {
		return nil, fmt.Errorf("init writes no file that already exists, and %s already has: %s", dir, strings.Join(existing, ", "))
	}
	created := make([]string, 0, len(files)+1)
	for _, f := range files {
		if err := createExclusive(filepath.Join(dir, f.path), f.contents); err != nil {
			return created, fmt.Errorf("write %s (created so far: %v): %w", f.path, created, err)
		}
		created = append(created, f.path)
	}
	err := os.Mkdir(filepath.Join(dir, initStorageDir), 0o755)
	if errors.Is(err, os.ErrExist) {
		return created, nil // an existing storage directory is reused, not created
	}
	if err != nil {
		return created, fmt.Errorf("create %s: %w", initStorageDir, err)
	}
	return append(created, initStorageDir+"/"), nil
}

// existingInitPaths lists the files of the project that dir already holds,
// including the storage directory.
func existingInitPaths(dir string, files []initFile) []string {
	var existing []string
	for _, f := range files {
		if _, err := os.Lstat(filepath.Join(dir, f.path)); !errors.Is(err, os.ErrNotExist) {
			existing = append(existing, f.path)
		}
	}
	if info, err := os.Lstat(filepath.Join(dir, initStorageDir)); err == nil && !info.IsDir() {
		existing = append(existing, initStorageDir)
	}
	return existing
}

func createExclusive(path, contents string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(contents); err != nil {
		iox.DiscardClose(f)
		return err
	}
	return f.Close()
}

// initNextSteps returns the commands that install the project and run its
// example script.
func initNextSteps(dir string) []string {
	steps := []string{"npm install", "quarry run --script links.ts --run-id links-001 --source example " +
		`--job '{"url": "https://example.com"}' --storage-backend fs --storage-path ./` + initStorageDir}
	if dir == "." {
		return steps
	}
	if strings.HasPrefix(dir, "-") {
		dir = "./" + dir // cd would read a leading dash as an option
	}
	return append([]string{"cd " + shellQuote(dir)}, steps...)
}

// shellSafe matches a word that a POSIX shell reads literally.
var shellSafe = regexp.MustCompile(`^[A-Za-z0-9_./+:=@%-]+$`)

// shellQuote quotes s for a POSIX shell, so that a printed command runs as
// shown for any directory name.
func shellQuote(s string) string {
	if shellSafe.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
