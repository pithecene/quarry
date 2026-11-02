package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/quarry/internal/testutil"
	"github.com/pithecene/quarry/lode"
	runreader "github.com/pithecene/quarry/reader"
	"github.com/pithecene/quarry/types"
)

// artifactRunID is the run writeFSArtifactRun writes.
const artifactRunID = "run-art"

// writeFSArtifactRun writes one finished run whose one artifact, a1, holds
// data in a Volume, through the runtime's own write path. digest overrides
// the SHA-256 the commit record states when it is not empty.
func writeFSArtifactRun(t *testing.T, root string, data []byte, digest string) {
	t.Helper()
	ctx := t.Context()
	cfg := lode.Config{Dataset: "quarry", Source: "demo", Category: "default", Day: "2026-10-22", RunID: artifactRunID}
	c, err := lode.NewLodeClient(ctx, cfg, root)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if err := c.WriteStartRecord(ctx, &types.RunMeta{RunID: artifactRunID, Attempt: 1}, time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("start: %v", err)
	}
	w, err := c.OpenArtifact(ctx, "a1", int64(len(data)), "")
	if err != nil {
		t.Fatalf("open artifact: %v", err)
	}
	// Two ranges written out of order, as a script may write them.
	half := len(data) / 2
	for _, r := range [][2]int{{half, len(data)}, {0, half}} {
		if err := w.WriteRange(ctx, int64(r[0]), data[r[0]:r[1]]); err != nil {
			t.Fatalf("write range: %v", err)
		}
	}
	storage, err := w.Finish(ctx)
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if digest != "" {
		storage.SHA256 = digest
	}
	commit := &types.EventEnvelope{
		ContractVersion: types.ContractVersion, EventID: "a-a1", RunID: artifactRunID, Seq: 1, Type: types.EventTypeArtifact,
		Ts: "2026-10-22T00:00:00Z", Attempt: 1, Storage: &storage,
		Payload: map[string]any{"artifact_id": "a1", "name": "a1.bin", "content_type": "application/octet-stream", "size_bytes": float64(len(data))},
	}
	if err := c.WriteEvents(ctx, "quarry", artifactRunID, []*types.EventEnvelope{commit}); err != nil {
		t.Fatalf("events: %v", err)
	}
	if err := c.WriteEndRecord(ctx, lode.RunEnd{EndedAt: time.Now(), Outcome: types.OutcomeSuccess, Terminal: types.EventTypeRunComplete, EventCount: 1}); err != nil {
		t.Fatalf("end: %v", err)
	}
}

// artifactLocator is the storage and run flags that locate the run.
func artifactLocator(root string) []string {
	return []string{"--storage-backend", "fs", "--storage-path", root, "--source", "demo", "--category", "default", "--day", "2026-10-22", "--run-id", artifactRunID}
}

func TestListArtifacts_FromStorage(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	data := []byte("twenty bytes of data")
	writeFSArtifactRun(t, root, data, "")

	out, err := runListCLI(t, append([]string{"artifacts", "--format", "json"}, artifactLocator(root)...)...)
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	var got struct {
		Run struct {
			RunID string `json:"run_id"`
		} `json:"run"`
		Artifacts []struct {
			ArtifactID       string `json:"artifact_id"`
			SizeBytes        int64  `json:"size_bytes"`
			Storage          string `json:"storage"`
			VolumeSnapshotID string `json:"volume_snapshot_id"`
			SHA256           string `json:"sha256"`
		} `json:"artifacts"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	sum := sha256.Sum256(data)
	if got.Run.RunID != artifactRunID || len(got.Artifacts) != 1 {
		t.Fatalf("response = %+v", got)
	}
	a := got.Artifacts[0]
	if a.ArtifactID != "a1" || a.SizeBytes != int64(len(data)) || a.Storage != "volume" || a.VolumeSnapshotID == "" || a.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("artifact = %+v", a)
	}
}

func TestReadArtifact(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	data := []byte("twenty bytes of data")
	writeFSArtifactRun(t, root, data, "")
	read := func(extra ...string) (string, error) {
		args := append([]string{"read", "artifact"}, artifactLocator(root)...)
		return runCLI(t, append(args, append([]string{"--artifact-id", "a1"}, extra...)...)...)
	}

	t.Run("the whole artifact to stdout", func(t *testing.T) {
		out, err := read()
		if err != nil || out != string(data) {
			t.Fatalf("read = %q, %v; want %q", out, err, data)
		}
	})
	t.Run("the whole artifact to a file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "a1.bin")
		if out, err := read("--out", path); err != nil || out != "" {
			t.Fatalf("read = %q, %v; want nothing on stdout", out, err)
		}
		if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("file = %q, %v; want %q", got, err, data)
		}
	})
	ranges := []struct {
		name  string
		flags []string
		want  string
	}{
		{"a range", []string{"--offset", "7", "--length", "5"}, string(data[7:12])},
		{"from an offset to the end", []string{"--offset", "13"}, string(data[13:])},
		{"an empty range at the end", []string{"--offset", "20", "--length", "0"}, ""},
	}
	for _, tc := range ranges {
		t.Run(tc.name, func(t *testing.T) {
			out, err := read(tc.flags...)
			if err != nil || out != tc.want {
				t.Fatalf("read = %q, %v; want %q", out, err, tc.want)
			}
		})
	}
	refusals := []struct {
		name  string
		flags []string
	}{
		{"a negative offset", []string{"--offset", "-1"}},
		{"an offset past the end", []string{"--offset", "21"}},
		{"a length past the end", []string{"--offset", "15", "--length", "6"}},
		{"a negative length", []string{"--length", "-1"}},
	}
	for _, tc := range refusals {
		t.Run(tc.name+" is refused", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "a1.bin")
			if out, err := read(append(tc.flags, "--out", path)...); err == nil || out != "" {
				t.Fatalf("read = %q, %v; want an error and no output", out, err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("--out after a refused range: %v; want no file", err)
			}
		})
	}
	t.Run("an existing --out is refused and kept", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "a1.bin")
		if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := read("--out", path); err == nil {
			t.Fatal("read over an existing file succeeded")
		}
		if got, _ := os.ReadFile(path); string(got) != "keep" {
			t.Fatalf("existing file = %q; want it untouched", got)
		}
	})
	t.Run("an unknown artifact is an error", func(t *testing.T) {
		args := append([]string{"read", "artifact"}, artifactLocator(root)...)
		if _, err := runCLI(t, append(args, "--artifact-id", "a9")...); err == nil {
			t.Fatal("read of an unknown artifact succeeded")
		}
	})
}

// A commit record whose digest differs from the stored bytes fails a whole
// read and leaves no file behind. A range read is not compared, because the
// record states the digest of the whole artifact only.
func TestReadArtifact_DigestMismatch(t *testing.T) {
	testutil.Serial(t, "runCLI swaps the process-wide os.Stdout")
	root := t.TempDir()
	data := []byte("twenty bytes of data")
	writeFSArtifactRun(t, root, data, hex.EncodeToString(make([]byte, sha256.Size)))
	args := append([]string{"read", "artifact"}, artifactLocator(root)...)
	args = append(args, "--artifact-id", "a1")

	path := filepath.Join(t.TempDir(), "a1.bin")
	if _, err := runCLI(t, append(args, "--out", path)...); err == nil {
		t.Fatal("whole read with a wrong digest succeeded")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("--out after a digest mismatch: %v; want the file removed", err)
	}
	if out, err := runCLI(t, append(args, "--offset", "2", "--length", "3")...); err != nil || out != string(data[2:5]) {
		t.Fatalf("range read = %q, %v; want %q", out, err, data[2:5])
	}
}

// A character device that is not a terminal, such as /dev/null, is a valid
// stdout: only a terminal is refused.
func TestReadArtifact_DevNullStdout(t *testing.T) {
	testutil.Serial(t, "swaps the process-wide os.Stdout")
	null, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = null.Close() }()
	orig := os.Stdout
	os.Stdout = null
	defer func() { os.Stdout = orig }()
	if isStdoutTTY() {
		t.Fatal("isStdoutTTY = true for /dev/null")
	}
	if err := checkDestination(""); err != nil {
		t.Fatalf("checkDestination with stdout on /dev/null: %v", err)
	}
}

// checkDestination refuses an existing --out, as it refuses a terminal
// stdout. The action calls it before it reads any storage, since an
// artifact of a run before 0.15 is assembled in memory when it is opened.
func TestReadArtifact_DestinationCheckedFirst(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "a1.bin")
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkDestination(path); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Fatalf("checkDestination(existing) = %v; want an exists error", err)
	}
	if err := checkDestination(filepath.Join(t.TempDir(), "new.bin")); err != nil {
		t.Fatalf("checkDestination(new) = %v", err)
	}
}

// countingReaderAt serves size zero bytes and records the length of each
// read, as each ReadAt of a Volume is a request to the backend.
type countingReaderAt struct {
	size  int64
	reads []int
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.reads = append(c.reads, len(p))
	if off >= c.size {
		return 0, io.EOF
	}
	n := min(int64(len(p)), c.size-off)
	clear(p[:n])
	if n < int64(len(p)) {
		return int(n), io.EOF
	}
	return int(n), nil
}

// A range read into a file reads the artifact in pieces of the copy
// buffer, not in the 32 KiB pieces of the file's own ReadFrom: each piece
// is one request to the backend.
func TestCopySpan_RangeReadsUseTheBuffer(t *testing.T) {
	t.Parallel()
	f, err := os.Create(filepath.Join(t.TempDir(), "out.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	size := int64(2*artifactCopyBuffer + 5)
	rd := &countingReaderAt{size: size}
	s := span{offset: 1, length: size - 1}
	if err := copySpan(f, rd, runreader.Artifact{ArtifactID: "a1", SizeBytes: size}, s); err != nil {
		t.Fatalf("copySpan: %v", err)
	}
	if len(rd.reads) > 4 {
		t.Fatalf("%d reads of %v; want a few of %d bytes", len(rd.reads), rd.reads[:min(len(rd.reads), 5)], artifactCopyBuffer)
	}
	if info, err := f.Stat(); err != nil || info.Size() != s.length {
		t.Fatalf("wrote %v, %v; want %d bytes", info.Size(), err, s.length)
	}
}
