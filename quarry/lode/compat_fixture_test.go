package lode_test

import (
	"archive/zip"
	"io/fs"
	"os"
	"testing"

	"github.com/pithecene/quarry/iox"
)

func unpackFixture(t *testing.T, path string) string {
	t.Helper()
	fixture, err := zip.OpenReader(path)
	if err != nil {
		t.Fatalf("open fixture %s: %v", path, err)
	}
	defer iox.DiscardClose(fixture)
	return copyFixture(t, fixture)
}

// Copy the fixture so the test can write to it.
func copyFixture(t *testing.T, src fs.FS) string {
	t.Helper()
	root := t.TempDir()
	if err := os.CopyFS(root, src); err != nil {
		t.Fatalf("copy fixture: %v", err)
	}
	return root
}
