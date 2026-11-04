package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/urfave/cli/v2"

	"github.com/pithecene/quarry/ipc"
	runreader "github.com/pithecene/quarry/reader"
)

// artifactReadTimeout bounds `read artifact`. An artifact holds up to
// 1 GiB, which a slow backend may take minutes to deliver, so the bound
// is wider than the one on listings.
const artifactReadTimeout = 30 * time.Minute

// artifactCopyBuffer is the size of one read from storage. It is the
// largest range a writer commits (ipc.MaxChunkSize), so each read spans at
// most two of a Volume's blocks and a whole read makes few requests.
const artifactCopyBuffer = ipc.MaxChunkSize

// ReadCommand returns the read command, whose subcommands write stored
// bytes rather than a rendered listing (CONTRACT_CLI.md §read).
func ReadCommand() *cli.Command {
	return &cli.Command{
		Name:        "read",
		Usage:       "Write stored bytes (artifacts)",
		Subcommands: []*cli.Command{readArtifactCommand()},
	}
}

func readArtifactCommand() *cli.Command {
	flags := runLocatorFlags("Run that holds the artifact (required)")
	flags = append(flags,
		&cli.StringFlag{Name: "artifact-id", Usage: "Artifact to read, as list artifacts names it (required)", Required: true},
		&cli.StringFlag{Name: "out", Usage: "File to create and write; it must not exist (default: stdout, which must not be a terminal)"},
		&cli.Int64Flag{Name: "offset", Usage: "First byte of the range to write (default 0)"},
		&cli.Int64Flag{Name: "length", Usage: "Bytes to write from --offset (default: to the end of the artifact)"},
	)
	return &cli.Command{
		Name:   "artifact",
		Usage:  "Write one artifact's bytes, checked against its commit record (reads storage)",
		Flags:  flags,
		Action: readArtifactAction,
	}
}

func readArtifactAction(c *cli.Context) error {
	// The destination is checked before any storage is read: opening an
	// artifact of a run before 0.15 assembles it in memory, up to 1 GiB.
	if err := checkDestination(c.String("out")); err != nil {
		return cli.Exit(fmt.Sprintf("read artifact: %v", err), 1)
	}
	ctx, cancel := context.WithTimeout(c.Context, artifactReadTimeout)
	defer cancel()
	// A read that falls back to the run's artifact partition costs requests
	// that grow with the run, so the reason is reported.
	fallback := runreader.WithIndexFallback(func(id string, why runreader.IndexFallback) {
		fmt.Fprintf(os.Stderr, "read artifact: %s: %s; reading the run's artifact partition\n", id, why)
	})
	rr, run, err := locatedRun(ctx, c, "read artifact", fallback)
	if err != nil {
		return err
	}
	artifact, rd, err := rr.OpenArtifact(ctx, run, c.String("artifact-id"))
	if err != nil {
		return cli.Exit(fmt.Sprintf("read artifact: %v", err), 1)
	}
	span, err := spanOf(c, artifact.SizeBytes)
	if err != nil {
		return cli.Exit(fmt.Sprintf("read artifact: %v", err), 1)
	}
	sink, err := openArtifactSink(c.String("out"))
	if err != nil {
		return cli.Exit(fmt.Sprintf("read artifact: %v", err), 1)
	}
	if err := sink.finish(copySpan(sink.w, rd, artifact, span)); err != nil {
		return cli.Exit(fmt.Sprintf("read artifact: %v", err), 1)
	}
	return nil
}

// span is the part of an artifact a read writes. whole marks the entire
// artifact, the only span whose digest the commit record states.
type span struct {
	offset, length int64
	whole          bool
}

// spanOf is the span that --offset and --length request, checked to lie
// inside an artifact of size bytes. Without --length it runs to the end.
func spanOf(c *cli.Context, size int64) (span, error) {
	offset := c.Int64("offset")
	if offset < 0 || offset > size {
		return span{}, fmt.Errorf("--offset %d is outside the artifact's %d bytes", offset, size)
	}
	length := size - offset
	if c.IsSet("length") {
		length = c.Int64("length")
	}
	if length < 0 || length > size-offset {
		return span{}, fmt.Errorf("--length %d from offset %d is outside the artifact's %d bytes", length, offset, size)
	}
	return span{offset: offset, length: length, whole: offset == 0 && length == size}, nil
}

// copySpan writes the span's bytes to w. For the whole artifact it also
// checks them against the digest of the commit record, which a run that a
// release before 0.15 wrote does not record.
func copySpan(w io.Writer, rd io.ReaderAt, a runreader.Artifact, s span) error {
	var digest hash.Hash
	if s.whole && a.SHA256 != "" {
		digest = sha256.New()
		w = io.MultiWriter(w, digest)
	}
	// writerOnly hides a destination's ReadFrom: io.CopyBuffer would hand the
	// copy to it, and *os.File reads in pieces of 32 KiB, one request each.
	n, err := io.CopyBuffer(writerOnly{w}, io.NewSectionReader(rd, s.offset, s.length), make([]byte, artifactCopyBuffer))
	if err != nil {
		return fmt.Errorf("artifact %q: %w", a.ArtifactID, err)
	}
	if n != s.length {
		return fmt.Errorf("artifact %q: wrote %d of %d bytes", a.ArtifactID, n, s.length)
	}
	if digest == nil {
		return nil
	}
	if got := hex.EncodeToString(digest.Sum(nil)); !strings.EqualFold(got, a.SHA256) {
		return fmt.Errorf("artifact %q: sha256 %s, its commit record states %s", a.ArtifactID, got, a.SHA256)
	}
	return nil
}

// writerOnly is a writer with no other method, so a copy into it uses the
// copy's own buffer.
type writerOnly struct{ w io.Writer }

func (o writerOnly) Write(p []byte) (int, error) { return o.w.Write(p) }

// artifactSink is where a read writes: a file the read created, or stdout.
type artifactSink struct {
	w    io.Writer
	file *os.File // nil for stdout
}

// checkDestination refuses a destination a read must not write: a stdout
// that is a terminal, when path is empty, or a path that exists.
// openArtifactSink creates the file exclusively all the same, since the
// path may appear in between.
func checkDestination(path string) error {
	if path == "" {
		if isStdoutTTY() {
			return errors.New("stdout is a terminal: pass --out or redirect stdout")
		}
		return nil
	}
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("--out %s exists", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("--out %s: %w", path, err)
	}
	return nil
}

// openArtifactSink creates the file at path, refusing one that exists, or
// selects stdout when path is empty.
func openArtifactSink(path string) (*artifactSink, error) {
	if path == "" {
		return &artifactSink{w: os.Stdout}, nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create --out: %w", err)
	}
	return &artifactSink{w: f, file: f}, nil
}

// finish closes the file the read created and removes it when the read or
// the close failed, so a file left behind always holds verified bytes.
// Stdout is left as it is: the caller learns of a failure from the exit
// code.
func (s *artifactSink) finish(readErr error) error {
	if s.file == nil {
		return readErr
	}
	err := errors.Join(readErr, s.file.Close())
	if err != nil {
		if rmErr := os.Remove(s.file.Name()); rmErr != nil {
			err = errors.Join(err, fmt.Errorf("remove %s: %w", s.file.Name(), rmErr))
		}
	}
	return err
}

// isStdoutTTY reports whether stdout is a terminal. A character device is
// not enough: /dev/null is one, and a valid place to discard bytes.
func isStdoutTTY() bool {
	fd := os.Stdout.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
}
