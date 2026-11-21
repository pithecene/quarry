package lode

import (
	"fmt"
	"slices"

	"github.com/pithecene/lode/lode"
)

// writerComponents are the layout, compressor, and checksum of every Quarry
// writer (CONTRACT_LODE.md §Writer Settings). Lode applies zstd and SHA-256
// only to a dataset without a head. A dataset with a head, published or
// staged by an interrupted first commit, keeps the compressor and checksum
// that the head records.
func writerComponents() []lode.DatasetOption {
	return []lode.DatasetOption{
		layout(),
		lode.WithCompressor(lode.Zstd()),
		lode.WithChecksum(lode.SHA256()),
		lode.WithRecordedComponents(),
	}
}

// writerOptions combines the caller options with Quarry's writer settings. A
// caller cannot set a component: every Quarry writer of a dataset uses the same
// layout, compressor, and checksum (CONTRACT_LODE.md §Writer Settings).
func writerOptions(opts []lode.DatasetOption) ([]lode.DatasetOption, error) {
	if slices.ContainsFunc(opts, isComponentOption) {
		return nil, fmt.Errorf("%w: Quarry sets the layout, compressor, and checksum of its datasets", lode.ErrInvalidArgument)
	}
	return slices.Concat([]lode.DatasetOption{lode.WithRetry(datasetRetryCount)}, opts, writerComponents()), nil
}

func isComponentOption(opt lode.DatasetOption) bool {
	_, ok := opt.(lode.ComponentOption)
	return ok
}
