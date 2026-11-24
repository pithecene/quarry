package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/runtime"
	"github.com/pithecene/quarry/types"
)

// childFactory holds the shared configuration needed to construct and execute
// child runs during fan-out. Each invocation of Run builds an independent
// policy, sink, and metrics collector for the child.
type childFactory struct {
	policyChoice      policyChoice
	executorPath      string
	storage           storageChoice
	storageDataset    string
	source            string
	category          string
	proxy             *types.ProxyEndpoint
	browserWSEndpoint string
	noBrowser         bool
	resolveFrom       string
	eventSinks        []eventSinkChoice
	// The max duration bounds each child run from its own start, as
	// --max-duration bounds the root run.
	maxDuration time.Duration
}

// Run constructs and executes a single child run for the fan-out operator.
func (cf *childFactory) Run(ctx context.Context, item runtime.WorkItem, observer runtime.EnqueueObserver) (*runtime.RunResult, error) {
	childMeta := &types.RunMeta{
		RunID:   item.RunID,
		Attempt: 1,
	}

	childSource := cf.source
	if item.Source != "" {
		childSource = item.Source
	}
	childCategory := cf.category
	if item.Category != "" {
		childCategory = item.Category
	}

	childCollector := metrics.NewCollector(
		cf.policyChoice.name,
		filepath.Base(cf.executorPath),
		cf.storage.backend,
		item.RunID,
		"",
	)

	if err := lode.ValidateRunKey(cf.storageDataset, childSource, childCategory, item.RunID); err != nil {
		return nil, fmt.Errorf("invalid child run key: %w", err)
	}

	childStartTime := time.Now()
	childPol, childLodeClient, err := buildPolicy(ctx,
		cf.policyChoice, cf.storage, cf.storageDataset,
		childSource, childCategory, childMeta,
		childStartTime, childCollector, cf.eventSinks,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create child policy: %w", err)
	}
	defer iox.DiscardClose(childPol)

	config := &runtime.RunConfig{
		ExecutorPath:      cf.executorPath,
		ScriptPath:        item.Target,
		Job:               item.Params,
		RunMeta:           childMeta,
		Policy:            childPol,
		Proxy:             cf.proxy,
		FileWriter:        childLodeClient,
		ArtifactStore:     childLodeClient,
		EnqueueObserver:   observer,
		BrowserWSEndpoint: cf.browserWSEndpoint,
		NoBrowser:         cf.noBrowser,
		ResolveFrom:       cf.resolveFrom,
		Source:            childSource,
		Category:          childCategory,
		StorageDataset:    cf.storageDataset,
		StorageDay:        lode.DeriveDay(childStartTime),
		Collector:         childCollector,
		Storage:           childLodeClient,
		StartedAt:         childStartTime,
		MaxDuration:       cf.maxDuration,
	}

	orchestrator, err := runtime.NewRunOrchestrator(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create child orchestrator: %w", err)
	}

	// Execute persists the child's metrics and run records itself
	// (CONTRACT_RUN.md §Run Finalization).
	result, err := orchestrator.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("child execution failed: %w", err)
	}
	return result, nil
}
