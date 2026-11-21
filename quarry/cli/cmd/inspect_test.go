package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	lodelib "github.com/pithecene/lode/lode"
	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/internal/testutil"
	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

// inspectLocator is the storage and selector of the run that writeFSRun
// writes under root.
func inspectLocator(root string) []string {
	return []string{"--storage-backend", "fs", "--storage-path", root,
		"--source", "demo", "--category", "default", "--day", "2026-10-22"}
}

// inspect run reads the run from its records, the policy from its metrics
// record, and its sidecar files.
func TestInspectRun_FromStorage(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	writeFSRun(t, root, "run-a")
	key := lode.Config{Dataset: "quarry", Source: "demo", Category: "default", Day: "2026-10-22", RunID: "run-a"}
	writeInspectDetails(t, root, key, "page.png", "strict")
	otherCategory, otherDay := key, key
	otherCategory.Category = "other"
	otherDay.Day = "2026-10-23"
	writeInspectDetails(t, root, otherCategory, "other-category.png", "buffered")
	writeInspectDetails(t, root, otherDay, "other-day.png", "streaming")

	out, err := runCLI(t, append(append([]string{"inspect", "run"}, inspectLocator(root)...), "run-a")...)
	if err != nil {
		t.Fatalf("inspect run: %v", err)
	}
	var resp struct {
		Run struct {
			RunID string `json:"run_id"`
			State string `json:"state"`
		} `json:"run"`
		Policy       *string `json:"policy"`
		SidecarFiles []struct {
			Filename string `json:"filename"`
		} `json:"sidecar_files"`
	}
	if err := json.Unmarshal([]byte(out), &resp); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if resp.Run.RunID != "run-a" || resp.Run.State != "succeeded" || resp.Policy == nil || *resp.Policy != "strict" ||
		len(resp.SidecarFiles) != 1 || resp.SidecarFiles[0].Filename != "page.png" {
		t.Fatalf("resp = %s", out)
	}

	cases := []struct {
		format string
		want   []string
	}{
		{"table", []string{"run_id: run-a", "state: succeeded", "policy: strict", "filename content_type size path", "page.png image/png 3 datasets/quarry/partitions/"}},
		{"yaml", []string{"run: run_id: run-a", "policy: strict", "sidecar_files: - filename: page.png content_type: image/png"}},
	}
	for _, tc := range cases {
		t.Run(tc.format, func(t *testing.T) {
			args := append([]string{"inspect", "run", "--format", tc.format}, inspectLocator(root)...)
			out, err := runCLI(t, append(args, "run-a")...)
			if err != nil {
				t.Fatal(err)
			}
			normalized := strings.Join(strings.Fields(out), " ")
			for _, want := range tc.want {
				if !strings.Contains(normalized, want) {
					t.Errorf("output %q is missing %q", out, want)
				}
			}
		})
	}
}

func writeInspectDetails(t *testing.T, root string, key lode.Config, filename, policy string) {
	t.Helper()
	ctx := t.Context()
	client, err := lode.NewLodeClient(ctx, key, root)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.PutFile(ctx, filename, "image/png", []byte("png")); err != nil {
		t.Fatal(err)
	}
	if err := client.WriteMetrics(ctx, metrics.Snapshot{RunID: key.RunID, Policy: policy}, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestInspectRun_MetricsPolicy(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	cases := []struct {
		name    string
		record  lodelib.D
		wantOut string
		wantErr string
	}{
		{name: "no metrics record", wantOut: `"policy": null`},
		{name: "buffered policy", record: lodelib.D{"policy": "buffered"}, wantOut: `"policy": "buffered"`},
		{name: "missing policy", record: lodelib.D{}, wantErr: "expected string policy"},
		{name: "non-string policy", record: lodelib.D{"policy": 42}, wantErr: "expected string policy"},
		{name: "empty policy", record: lodelib.D{"policy": ""}, wantErr: `unknown policy "" for run "run-a"`},
		{name: "unknown policy", record: lodelib.D{"policy": "custom"}, wantErr: `unknown policy "custom" for run "run-a"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFSRun(t, root, "run-a")
			if tc.record != nil {
				writeInspectMetricsRecord(t, root, tc.record, nil)
			}
			args := append([]string{"inspect", "run", "--format", "json"}, inspectLocator(root)...)
			out, err := runCLI(t, append(args, "run-a")...)
			if tc.wantErr == "" {
				if err != nil || !strings.Contains(out, tc.wantOut) {
					t.Fatalf("err = %v, out = %q; want output containing %q", err, out, tc.wantOut)
				}
				return
			}
			var exit cli.ExitCoder
			if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(exit.Error(), tc.wantErr) || out != "" {
				t.Fatalf("err = %v, out = %q; want exit 1 with %q and empty stdout", err, out, tc.wantErr)
			}
		})
	}
}

func TestInspectRun_RejectsMalformedSidecarInventory(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	prefix := "datasets/quarry/partitions/source=demo/category=default/day=2026-10-22/run_id=run-a/files/"
	valid := lode.SidecarFileRef{Filename: "page.png", ContentType: "image/png", Size: 3, Path: prefix + "page.png"}
	foreign := valid
	foreign.Path = strings.Replace(valid.Path, "run_id=run-a/", "run_id=run-b/", 1)
	mismatched := valid
	mismatched.Path = prefix + "other.png"
	cases := []struct {
		name      string
		inventory any
		want      string
	}{
		{"non-array inventory", "invalid", "sidecar_files: expected an array"},
		{"null inventory", nil, "sidecar_files: expected an array"},
		{"non-object entry after a valid file", []any{valid, "invalid"}, "sidecar_files[1]"},
		{"null entry", []any{nil}, "sidecar_files[0]"},
		{"missing size", []any{map[string]any{"filename": valid.Filename, "path": valid.Path, "content_type": valid.ContentType}}, "sidecar_files[0]"},
		{"wrong field type", []any{map[string]any{"filename": 42, "path": valid.Path, "content_type": valid.ContentType, "size": valid.Size}}, "sidecar_files[0]"},
		{"fractional size", []any{map[string]any{"filename": valid.Filename, "path": valid.Path, "content_type": valid.ContentType, "size": 1.5}}, "sidecar_files[0]"},
		{"negative size", []lode.SidecarFileRef{{Filename: valid.Filename, ContentType: valid.ContentType, Path: valid.Path, Size: -1}}, "sidecar_files[0]"},
		{"forward slash in filename", []lode.SidecarFileRef{{Filename: "nested/page.png", ContentType: valid.ContentType, Path: valid.Path, Size: valid.Size}}, "sidecar_files[0]: expected a flat filename"},
		{"backslash in filename", []lode.SidecarFileRef{{Filename: `nested\page.png`, ContentType: valid.ContentType, Path: valid.Path, Size: valid.Size}}, "sidecar_files[0]: expected a flat filename"},
		{"path belonging to another run", []lode.SidecarFileRef{foreign}, "does not match the selected run's file"},
		{"path naming another file", []lode.SidecarFileRef{mismatched}, "does not match the selected run's file"},
		{"parent directory as filename", []lode.SidecarFileRef{{Filename: "..", ContentType: valid.ContentType, Path: prefix + "..", Size: valid.Size}}, `filename that does not contain ".."`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFSRun(t, root, "run-a")
			writeInspectMetricsRecord(t, root, lodelib.D{"policy": "strict"}, map[string]any{lode.MetadataKeySidecarFiles: tc.inventory})

			args := append([]string{"inspect", "run", "--format", "json"}, inspectLocator(root)...)
			out, err := runCLI(t, append(args, "run-a")...)
			var exit cli.ExitCoder
			if !errors.As(err, &exit) || exit.ExitCode() != 1 || out != "" ||
				!strings.Contains(exit.Error(), "read sidecar files: read quarry/snapshot/") || !strings.Contains(exit.Error(), tc.want) {
				t.Fatalf("err = %v, out = %q; want exit 1 with a snapshot error naming %q and empty stdout", err, out, tc.want)
			}
		})
	}
}

func writeInspectMetricsRecord(t *testing.T, root string, record lodelib.D, metadata map[string]any) {
	t.Helper()
	ctx := t.Context()
	store, err := lodelib.NewFS(root)
	if err != nil {
		t.Fatal(err)
	}
	dataset, err := lode.OpenDataset(ctx, "quarry", store)
	if err != nil {
		t.Fatal(err)
	}
	record["record_kind"] = lode.RecordKindMetrics
	record["event_type"] = "metrics"
	record["source"] = "demo"
	record["category"] = "default"
	record["day"] = "2026-10-22"
	record["run_id"] = "run-a"
	if _, err := dataset.Write(ctx, lodelib.R(record), metadata); err != nil {
		t.Fatal(err)
	}
}

func TestInspectRun_RejectsStartRecordIdentityMismatch(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	cfg := lode.Config{Dataset: "quarry", Source: "demo", Category: "default", Day: "2026-10-22", RunID: "run-a"}
	key := lode.RunRecordDir(cfg) + lode.StartRecordName
	startedAt := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	cases := []struct {
		field  string
		change func(*lode.StartRecord)
	}{
		{"dataset", func(rec *lode.StartRecord) { rec.Dataset = "other" }},
		{"source", func(rec *lode.StartRecord) { rec.Source = "other" }},
		{"category", func(rec *lode.StartRecord) { rec.Category = "other" }},
		{"day", func(rec *lode.StartRecord) { rec.Day = "2026-10-23" }},
		{"run ID", func(rec *lode.StartRecord) { rec.RunID = "run-b" }},
		{"partition", func(rec *lode.StartRecord) {
			rec.Partition = "source=demo/category=default/day=2026-10-22/run_id=run-b/"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			root := t.TempDir()
			store, err := lodelib.NewFS(root)
			if err != nil {
				t.Fatal(err)
			}
			record := lode.NewStartRecord(cfg, &types.RunMeta{RunID: cfg.RunID, Attempt: 1}, startedAt)
			tc.change(&record)
			data, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Put(t.Context(), key, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}

			args := append([]string{"inspect", "run", "--format", "json"}, inspectLocator(root)...)
			out, err := runCLI(t, append(args, cfg.RunID)...)
			var exit cli.ExitCoder
			if !errors.As(err, &exit) || exit.ExitCode() != 1 ||
				!strings.Contains(exit.Error(), "start record identity does not match the selected storage key") || out != "" {
				t.Fatalf("err = %v, out = %q; want exit 1 with an identity error and empty stdout", err, out)
			}
		})
	}
}

// inspect run never reports a run that storage does not hold, and it reads
// only storage.
func TestInspectRun_RefusesWhatStorageDoesNotHold(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	writeFSRun(t, root, "run-a")
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"a run that does not exist", append(inspectLocator(root), "quarry-audit-nonexistent"), "run not found"},
		{"no selector", []string{"--storage-backend", "fs", "--storage-path", root, "run-a"}, "requires --source, --category, and --day"},
	}
	for _, tc := range cases {
		out, err := runCLI(t, append([]string{"inspect", "run"}, tc.args...)...)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, out = %q; want an error naming %q", tc.name, err, out, tc.want)
		}
	}
}

// inspect run builds its store factory once, so the fs backend's warning about
// ignored S3 flags appears once, as on the other storage commands, and the
// factory builds one store for the run record and details reads.
func TestInspectRun_BuildsTheStoreOnce(t *testing.T) {
	testutil.Serial(t, "runCLI and this test swap os.Stdout, os.Stderr, and newStoreFactory")
	builds := 0
	origFactory := newStoreFactory
	newStoreFactory = func(backend, path string, s3cfg lode.S3Config) (lodelib.Factory[lodelib.RangeStore], error) {
		f, err := origFactory(backend, path, s3cfg)
		return func() (lodelib.RangeStore, error) {
			builds++
			return f()
		}, err
	}
	t.Cleanup(func() { newStoreFactory = origFactory })
	root := t.TempDir()
	writeFSRun(t, root, "run-a")

	orig := os.Stderr
	rd, wr, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = wr
	t.Cleanup(func() {
		os.Stderr = orig
		iox.DiscardClose(wr)
		iox.DiscardClose(rd)
	})
	args := append([]string{"inspect", "run", "--format", "json", "--storage-endpoint", "http://ignored"}, inspectLocator(root)...)
	_, runErr := runCLI(t, append(args, "run-a")...)
	iox.DiscardClose(wr) // ReadAll needs EOF; the cleanup closes it again harmlessly.
	os.Stderr = orig
	stderr, _ := io.ReadAll(rd)
	if runErr != nil {
		t.Fatalf("inspect run: %v", runErr)
	}
	if n := strings.Count(string(stderr), "are ignored for fs backend"); n != 1 {
		t.Fatalf("warning appears %d times, want once:\n%s", n, stderr)
	}
	if builds != 1 {
		t.Errorf("inspect run built %d stores, want 1", builds)
	}
}
