package cmd

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/urfave/cli/v2"
)

// ParityArtifact represents the CLI parity artifact structure. FlagSets
// declares once each group of flags that several commands share, as the
// shared flag constructors do in code.
type ParityArtifact struct {
	Version     string                           `json:"version"`
	Description string                           `json:"description"`
	FlagSets    map[string]map[string]ParityFlag `json:"flagSets,omitempty"`
	Commands    map[string]ParityCommand         `json:"commands"`
}

// ParityCommand represents a command in the parity artifact. Its flags are
// its own flags and the flags of the sets it names. Requires names the flags
// that the command requires, which a set may declare optional.
type ParityCommand struct {
	Description string                      `json:"description"`
	FlagSets    []string                    `json:"flagSets,omitempty"`
	Requires    []string                    `json:"requires,omitempty"`
	Flags       map[string]ParityFlag       `json:"flags,omitempty"`
	Subcommands map[string]ParitySubcommand `json:"subcommands,omitempty"`
}

// ParitySubcommand represents a subcommand in the parity artifact, with flag
// sets and required flags as on ParityCommand.
type ParitySubcommand struct {
	FlagSets []string              `json:"flagSets,omitempty"`
	Requires []string              `json:"requires,omitempty"`
	Flags    map[string]ParityFlag `json:"flags"`
}

// ParityFlag represents a flag in the parity artifact.
type ParityFlag struct {
	Type          string   `json:"type"`
	Aliases       []string `json:"aliases,omitempty"`
	Required      bool     `json:"required"`
	Default       any      `json:"default,omitempty"`
	Description   string   `json:"description"`
	Validation    string   `json:"validation,omitempty"`
	ExclusiveWith []string `json:"exclusiveWith,omitempty"`
	DependsOn     []string `json:"dependsOn,omitempty"`
	Notes         string   `json:"notes,omitempty"`
}

// loadParityArtifact loads the CLI parity artifact from docs/maintainers/CLI_PARITY.json.
func loadParityArtifact(t *testing.T) *ParityArtifact {
	t.Helper()

	// Find the repo root by looking for docs/maintainers/CLI_PARITY.json
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine test file location")
	}

	// Walk up from quarry/cli/cmd to find repo root
	dir := filepath.Dir(filename)
	for range 5 {
		candidate := filepath.Join(dir, "docs", "maintainers", "CLI_PARITY.json")
		if _, err := os.Stat(candidate); err == nil {
			data, err := os.ReadFile(candidate)
			if err != nil {
				t.Fatalf("failed to read parity artifact: %v", err)
			}

			var artifact ParityArtifact
			if err := json.Unmarshal(data, &artifact); err != nil {
				t.Fatalf("failed to parse parity artifact: %v", err)
			}
			if err := artifact.expandFlagSets(); err != nil {
				t.Fatalf("invalid parity artifact: %v", err)
			}
			return &artifact
		}
		dir = filepath.Dir(dir)
	}

	t.Fatal("could not find docs/maintainers/CLI_PARITY.json - run from repo root")
	return nil
}

// expandFlagSets replaces each command's and subcommand's flags with its
// own flags plus the flags of the sets it names, marked required as its
// Requires says, so the checks compare complete flag lists.
func (a *ParityArtifact) expandFlagSets() error {
	for name, cmd := range a.Commands {
		flags, err := expandFlags(a.FlagSets, cmd.FlagSets, cmd.Requires, cmd.Flags)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		cmd.Flags = flags
		for subName, sub := range cmd.Subcommands {
			subFlags, err := expandFlags(a.FlagSets, sub.FlagSets, sub.Requires, sub.Flags)
			if err != nil {
				return fmt.Errorf("%s %s: %w", name, subName, err)
			}
			sub.Flags = subFlags
			cmd.Subcommands[subName] = sub
		}
		a.Commands[name] = cmd
	}
	return nil
}

// expandFlags merges the named sets into own and marks the required flags.
// A set the artifact does not declare, a flag declared twice, and a required
// flag that no declaration names are errors.
func expandFlags(sets map[string]map[string]ParityFlag, names, requires []string, own map[string]ParityFlag) (map[string]ParityFlag, error) {
	flags := maps.Clone(own)
	if flags == nil {
		flags = make(map[string]ParityFlag)
	}
	for _, name := range names {
		set, ok := sets[name]
		if !ok {
			return nil, fmt.Errorf("unknown flag set %q", name)
		}
		for flag, def := range set {
			if _, dup := flags[flag]; dup {
				return nil, fmt.Errorf("flag --%s is declared twice", flag)
			}
			flags[flag] = def
		}
	}
	for _, flag := range requires {
		def, ok := flags[flag]
		if !ok {
			return nil, fmt.Errorf("requires names undeclared flag --%s", flag)
		}
		def.Required = true
		flags[flag] = def
	}
	return flags, nil
}

func TestExpandFlags_RejectsInvalidDeclarations(t *testing.T) {
	t.Parallel()
	sets := map[string]map[string]ParityFlag{"storage-read": {"storage-path": {Type: "string"}}}
	cases := []struct {
		name     string
		names    []string
		requires []string
		own      map[string]ParityFlag
	}{
		{"unknown set", []string{"missing"}, nil, nil},
		{"flag declared twice", []string{"storage-read"}, nil, map[string]ParityFlag{"storage-path": {Type: "string"}}},
		{"required flag undeclared", []string{"storage-read"}, []string{"source"}, nil},
	}
	for _, tc := range cases {
		if _, err := expandFlags(sets, tc.names, tc.requires, tc.own); err == nil {
			t.Errorf("%s: expandFlags returned no error", tc.name)
		}
	}
	flags, err := expandFlags(sets, []string{"storage-read"}, []string{"storage-path"}, nil)
	if err != nil || !flags["storage-path"].Required || sets["storage-read"]["storage-path"].Required {
		t.Errorf("expandFlags = %v, %v; want --storage-path required without changing its set", flags, err)
	}
}

// extractFlags extracts flag names from a cli.Command.
func extractFlags(cmd *cli.Command) map[string]cli.Flag {
	flags := make(map[string]cli.Flag)
	for _, f := range cmd.Flags {
		names := f.Names()
		if len(names) > 0 {
			// Use the first (primary) name
			flags[names[0]] = f
		}
	}
	return flags
}

// TestCLIParityRunCommand validates the run command flags against the parity artifact.
func TestCLIParityRunCommand(t *testing.T) {
	t.Parallel()

	artifact := loadParityArtifact(t)
	runCmd := RunCommand()
	actualFlags := extractFlags(runCmd)

	parityRun, ok := artifact.Commands["run"]
	if !ok {
		t.Fatal("parity artifact missing 'run' command")
	}

	// Check all parity flags exist in actual CLI
	for flagName, parityFlag := range parityRun.Flags {
		actualFlag, exists := actualFlags[flagName]
		if !exists {
			t.Errorf("parity artifact declares flag --%s but it does not exist in CLI", flagName)
			continue
		}

		// Validate flag type matches
		actualType := getFlagType(actualFlag)
		if actualType != parityFlag.Type {
			t.Errorf("flag --%s: parity says type %q but actual is %q", flagName, parityFlag.Type, actualType)
		}

		// Validate required status
		actualRequired := isFlagRequired(actualFlag)
		if actualRequired != parityFlag.Required {
			t.Errorf("flag --%s: parity says required=%v but actual is %v", flagName, parityFlag.Required, actualRequired)
		}

		// Validate default value
		actualDefault := getFlagDefault(actualFlag)
		if parityFlag.Default != nil {
			if actualDefault != parityFlag.Default {
				t.Errorf("flag --%s: parity says default=%v but actual is %v", flagName, parityFlag.Default, actualDefault)
			}
		} else if actualDefault != nil {
			t.Errorf("flag --%s: parity has no default but actual default is %v", flagName, actualDefault)
		}
	}

	// Check all actual flags exist in parity artifact
	for flagName := range actualFlags {
		if _, exists := parityRun.Flags[flagName]; !exists {
			t.Errorf("CLI has flag --%s but it is not in parity artifact", flagName)
		}
	}
}

// TestCLIParityJobPayloadContract validates the job payload contract is correctly documented.
func TestCLIParityJobPayloadContract(t *testing.T) {
	t.Parallel()

	artifact := loadParityArtifact(t)

	parityRun, ok := artifact.Commands["run"]
	if !ok {
		t.Fatal("parity artifact missing 'run' command")
	}

	// Validate --job flag documentation
	jobFlag, ok := parityRun.Flags["job"]
	if !ok {
		t.Fatal("parity artifact missing 'job' flag")
	}

	if !strings.Contains(strings.ToLower(jobFlag.Validation), "object") {
		t.Error("--job flag validation should mention 'object' requirement")
	}

	if len(jobFlag.ExclusiveWith) == 0 || jobFlag.ExclusiveWith[0] != "job-json" {
		t.Error("--job flag should be exclusive with --job-json")
	}

	// Validate --job-json flag documentation
	jobJsonFlag, ok := parityRun.Flags["job-json"]
	if !ok {
		t.Fatal("parity artifact missing 'job-json' flag")
	}

	if !strings.Contains(strings.ToLower(jobJsonFlag.Validation), "object") {
		t.Error("--job-json flag validation should mention 'object' requirement")
	}

	if len(jobJsonFlag.ExclusiveWith) == 0 || jobJsonFlag.ExclusiveWith[0] != "job" {
		t.Error("--job-json flag should be exclusive with --job")
	}
}

// getFlagType returns the type string for a cli.Flag.
func getFlagType(f cli.Flag) string {
	switch f.(type) {
	case *cli.StringFlag:
		return "string"
	case *cli.IntFlag:
		return "int"
	case *cli.Int64Flag:
		return "int64"
	case *cli.BoolFlag:
		return "bool"
	case *cli.Float64Flag:
		return "float64"
	case *cli.DurationFlag:
		return "duration"
	case *cli.StringSliceFlag:
		return "string_slice"
	default:
		return "unknown"
	}
}

// isFlagRequired returns whether a cli.Flag is required.
func isFlagRequired(f cli.Flag) bool {
	switch tf := f.(type) {
	case *cli.StringFlag:
		return tf.Required
	case *cli.IntFlag:
		return tf.Required
	case *cli.Int64Flag:
		return tf.Required
	case *cli.BoolFlag:
		return tf.Required
	case *cli.DurationFlag:
		return tf.Required
	case *cli.StringSliceFlag:
		return tf.Required
	default:
		return false
	}
}

// getFlagDefault returns the default value for a cli.Flag.
// Returns nil if no default is set (empty string/zero value).
func getFlagDefault(f cli.Flag) any {
	switch tf := f.(type) {
	case *cli.StringFlag:
		if tf.Value == "" {
			return nil
		}
		return tf.Value
	case *cli.IntFlag:
		if tf.Value == 0 {
			return nil
		}
		return float64(tf.Value) // JSON numbers are float64
	case *cli.Int64Flag:
		if tf.Value == 0 {
			return nil
		}
		return float64(tf.Value)
	case *cli.BoolFlag:
		if !tf.Value {
			return nil
		}
		return tf.Value
	case *cli.DurationFlag:
		if tf.Value == 0 {
			return nil
		}
		return tf.Value.String()
	case *cli.StringSliceFlag:
		return nil
	default:
		return nil
	}
}

// checkFlagNames reports each flag that the parity artifact declares for
// command but the CLI lacks, and each flag that the CLI has but the artifact
// omits.
func checkFlagNames(t *testing.T, command string, declared map[string]ParityFlag, actual map[string]cli.Flag) {
	t.Helper()
	for name := range declared {
		if _, ok := actual[name]; !ok {
			t.Errorf("parity declares flag --%s for '%s' but it does not exist", name, command)
		}
	}
	for name := range actual {
		if _, ok := declared[name]; !ok {
			t.Errorf("CLI '%s' has flag --%s but it is not in parity artifact", command, name)
		}
	}
}

// checkSubcommandFlags checks the flags of each leaf subcommand of cmd, a nested
// one by its path such as "resolve proxy", against the parity artifact's entry
// for the command named name.
func checkSubcommandFlags(t *testing.T, name string, cmd *cli.Command) {
	t.Helper()
	parity, ok := loadParityArtifact(t).Commands[name]
	if !ok {
		t.Fatalf("parity artifact missing '%s' command", name)
	}
	for path, sub := range leafSubcommands(cmd, "") {
		paritySub, ok := parity.Subcommands[path]
		if !ok {
			t.Errorf("CLI has %s subcommand %q but it is not in parity artifact", name, path)
			continue
		}
		checkFlagNames(t, name+" "+path, paritySub.Flags, extractFlags(sub))
	}
}

// Every command and subcommand that the parity artifact declares exists in
// the CLI, so the artifact cannot keep a command that the CLI removed, and
// each command's description is the CLI's usage text. A
// nested debug subcommand is declared by its path, such as "resolve proxy".
func TestCLIParityArtifactDeclaresOnlyExistingCommands(t *testing.T) {
	t.Parallel()
	commands := map[string]*cli.Command{}
	for _, cmd := range []*cli.Command{
		RunCommand(), InitCommand(), InspectCommand(), StatsCommand(), ListCommand(),
		ReadCommand(), IndexCommand(), PruneCommand(), DebugCommand(), VersionCommand("", "test-commit"),
	} {
		commands[cmd.Name] = cmd
	}
	for name, parity := range loadParityArtifact(t).Commands {
		cmd, ok := commands[name]
		if !ok {
			t.Errorf("parity artifact declares command %q but the CLI has none", name)
			continue
		}
		if parity.Description != cmd.Usage {
			t.Errorf("parity description of %q = %q, want the CLI usage %q", name, parity.Description, cmd.Usage)
		}
		leaves := leafSubcommands(cmd, "")
		for sub := range parity.Subcommands {
			if _, ok := leaves[sub]; !ok {
				t.Errorf("parity artifact declares '%s %s' but the CLI has no such subcommand", name, sub)
			}
		}
	}
}

// leafSubcommands maps the path of each of cmd's leaf subcommands below prefix,
// such as "resolve proxy", to the subcommand.
func leafSubcommands(cmd *cli.Command, prefix string) map[string]*cli.Command {
	leaves := map[string]*cli.Command{}
	for _, sub := range cmd.Subcommands {
		path := strings.TrimSpace(prefix + " " + sub.Name)
		if len(sub.Subcommands) > 0 {
			maps.Copy(leaves, leafSubcommands(sub, path))
			continue
		}
		leaves[path] = sub
	}
	return leaves
}

// Each command's subcommands declare exactly the flags that the CLI defines.
func TestCLIParitySubcommandFlags(t *testing.T) {
	t.Parallel()
	for _, cmd := range []*cli.Command{ListCommand(), InspectCommand(), StatsCommand(), IndexCommand(), ReadCommand(), DebugCommand()} {
		t.Run(cmd.Name, func(t *testing.T) {
			checkSubcommandFlags(t, cmd.Name, cmd)
		})
	}
}

// Each command without subcommands declares exactly the flags that the CLI
// defines. run is checked on its own, because its check also compares types,
// required flags, and defaults.
func TestCLIParityCommandFlags(t *testing.T) {
	t.Parallel()
	artifact := loadParityArtifact(t)
	for _, cmd := range []*cli.Command{PruneCommand(), InitCommand(), VersionCommand("", "test-commit")} {
		t.Run(cmd.Name, func(t *testing.T) {
			parity, ok := artifact.Commands[cmd.Name]
			if !ok {
				t.Fatalf("parity artifact missing '%s' command", cmd.Name)
			}
			checkFlagNames(t, cmd.Name, parity.Flags, extractFlags(cmd))
		})
	}
}
