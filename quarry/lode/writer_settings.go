package lode

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/pithecene/lode/lode"
)

// compressors and checksums map the names that Lode records in a manifest to
// the bundled components that wrote it. A writer must open with the head's
// components, or Lode refuses it with lode.ErrSettingsMismatch.
var (
	compressors = map[string]func() lode.Compressor{
		"noop": lode.Uncompressed,
		"gzip": lode.Gzip,
		"zstd": lode.Zstd,
	}
	checksums = map[string]func() lode.Checksum{
		"sha256": lode.SHA256,
		"crc32c": lode.CRC32C,
		"md5":    lode.MD5,
	}
)

// newDatasetComponents are the layout, compressor, and checksum of a
// dataset that has no snapshot yet (CONTRACT_LODE.md §Writer Settings).
func newDatasetComponents() []lode.DatasetOption {
	return []lode.DatasetOption{layout(), lode.WithCompressor(lode.Zstd()), lode.WithChecksum(lode.SHA256())}
}

// writerOptions combines Quarry's writer settings with the caller options.
// Reading the head requires opening a view and calling its Latest.
func writerOptions(ctx context.Context, dataset string, store lode.Store, opts []lode.DatasetOption) ([]lode.DatasetOption, error) {
	if err := validateWriterOptions(ctx, lode.DatasetID(dataset), opts); err != nil {
		return nil, err
	}
	settings := []lode.DatasetOption{lode.WithRetry(datasetRetryCount)}
	view, err := OpenView(ctx, dataset, store)
	if errors.Is(err, lode.ErrNotFound) {
		// Lode applies options in order and has no head to compare against yet.
		settings = append(settings, opts...)
		return append(settings, newDatasetComponents()...), nil
	}
	if err != nil {
		return nil, err
	}
	head, err := view.Latest(ctx)
	if err != nil {
		return nil, err
	}
	components, err := recordedComponents(head.Manifest)
	if err != nil {
		return nil, err
	}
	recorded := append([]lode.DatasetOption{layout()}, components...)
	if slices.ContainsFunc(opts, isComponentOption) {
		// Preserve Lode's mismatch errors for caller settings. Lode compares
		// component names, so the returned writer still needs the bundled
		// implementations that readers reconstruct from those names.
		requested := slices.Concat(settings, recorded, opts)
		if _, err := lode.Open(lode.Use(store)).Records(ctx, lode.DatasetID(dataset), recordCodec(), requested...); err != nil {
			return nil, err
		}
	}
	return slices.Concat(settings, opts, recorded), nil
}

func isComponentOption(opt lode.DatasetOption) bool {
	_, ok := opt.(lode.ComponentOption)
	return ok
}

func validateWriterOptions(ctx context.Context, dataset lode.DatasetID, opts []lode.DatasetOption) error {
	if len(opts) == 0 {
		return nil
	}
	// Lode validates its opaque options when opening a writer. Use an empty
	// store so a head read cannot mask an invalid argument.
	_, err := lode.Open(lode.Memory()).Records(ctx, dataset, recordCodec(), opts...)
	return err
}

// recordedComponents returns the bundled components whose names m records. A
// name that no bundled component has is a dataset that Quarry cannot extend.
func recordedComponents(m *lode.Manifest) ([]lode.DatasetOption, error) {
	compressor, ok := compressors[m.Compressor]
	if !ok {
		return nil, fmt.Errorf("%w: the dataset records compressor %q, which Quarry does not write", lode.ErrSettingsMismatch, m.Compressor)
	}
	opts := []lode.DatasetOption{lode.WithCompressor(compressor())}
	if m.ChecksumAlgorithm == "" {
		return append(opts, lode.WithChecksum(nil)), nil
	}
	checksum, ok := checksums[m.ChecksumAlgorithm]
	if !ok {
		return nil, fmt.Errorf("%w: the dataset records checksum %q, which Quarry does not write", lode.ErrSettingsMismatch, m.ChecksumAlgorithm)
	}
	return append(opts, lode.WithChecksum(checksum())), nil
}
