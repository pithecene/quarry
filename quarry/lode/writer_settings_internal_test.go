package lode

import (
	"errors"
	"testing"

	"github.com/pithecene/lode/lode"
)

// A head that records a compressor or checksum that Quarry has no bundled
// component for cannot be extended, so the writer is refused before it opens.
func TestRecordedComponents_UnknownNameIsASettingsMismatch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		manifest lode.Manifest
	}{
		{"unknown compressor", lode.Manifest{Compressor: "brotli"}},
		{"unknown checksum", lode.Manifest{Compressor: "zstd", ChecksumAlgorithm: "blake3"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := recordedComponents(&tc.manifest); !errors.Is(err, lode.ErrSettingsMismatch) {
				t.Errorf("recordedComponents = %v, want ErrSettingsMismatch", err)
			}
		})
	}
}
