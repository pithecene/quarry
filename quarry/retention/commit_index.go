package retention

import (
	"context"
	"errors"
	"strings"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
)

// indexEntries lists the keys of the run's commit index entries
// (CONTRACT_LODE.md §Commit Index): the direct children of its commits/
// prefix that are artifact IDs. Quarry writes nothing else there, and a
// key in another shape is left in place.
func (p *pruner) indexEntries(ctx context.Context, cfg qlode.Config) ([]string, error) {
	prefix := qlode.CommitIndexPrefix(cfg)
	keys, err := p.store.List(ctx, prefix)
	if errors.Is(err, lode.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, qlode.WrapReadError(err, prefix)
	}
	entries := keys[:0]
	for _, key := range keys {
		if qlode.ValidateArtifactID(strings.TrimPrefix(key, prefix)) == nil {
			entries = append(entries, key)
		}
	}
	return entries, nil
}
