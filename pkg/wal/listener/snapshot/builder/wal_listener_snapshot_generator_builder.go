// SPDX-License-Identifier: Apache-2.0

package builder

import (
	"atvenupgstream/pkg/otel"
	"atvenupgstream/pkg/schemalog"
	"atvenupgstream/pkg/snapshot/generator"
	"atvenupgstream/pkg/wal/listener"
	"atvenupgstream/pkg/wal/listener/snapshot/adapter"
	"context"
	"errors"
	"fmt"

	loglib "atvenupgstream/pkg/log"

	schemaloginstrumentation "atvenupgstream/pkg/schemalog/instrumentation"
	schemalogpg "atvenupgstream/pkg/schemalog/postgres"

	generatorinstrumentation "atvenupgstream/pkg/snapshot/generator/instrumentation"
	pgsnapshotgenerator "atvenupgstream/pkg/snapshot/generator/postgres/data"
	pgdumprestoregenerator "atvenupgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	schemalogsnapshotgenerator "atvenupgstream/pkg/snapshot/generator/postgres/schema/schemalog"
	pgtablefinder "atvenupgstream/pkg/snapshot/generator/postgres/tablefinder"
	snapshotstore "atvenupgstream/pkg/snapshot/store"
	snapshotstoreinstrumentation "atvenupgstream/pkg/snapshot/store/instrumentation"
	pgsnapshotstore "atvenupgstream/pkg/snapshot/store/postgres"

	listenersnapshot "atvenupgstream/pkg/wal/listener/snapshot"
)

var errSchemaSnapshotNotConfigured = errors.New("no schema snapshot has been configured")

// NewSnapshotGenerator builds a snapshot generator based on the
// configuration on input, adapting the process wal event to fit with the
// snapshot package implementation. It will add an activity recorder layer if
// configured which will keep track of snapshot requests and their status.
//
// ┌───────────────────────────────────────────────────┐
// │               Snapshot Generator                  │
// │ ┌─────────┐  ┌─────────┐  ┌────────┐  ┌─────────┐ │
// │ │         │  │         │  │        │  │         │ │
// │ │         │  │         │  │        │  │         │ │
// │ │         │  │         │  │        │  │         │ │
// │ │Snapshot │  │ Schema  │  │ Table  │  │  Data   │ │
// │ │Activity │─▶│Snapshot ├─▶│ Finder ├─▶│Snapshot │ │
// │ │Recorder │  │         │  │        │  │         │ │
// │ │         │  │         │  │        │  │         │ │
// │ │         │  │         │  │        │  │         │ │
// │ └─────────┘  └─────────┘  └────────┘  └─────────┘ │
// └───────────────────────────────────────────────────┘

func NewSnapshotGenerator(ctx context.Context, cfg *SnapshotListenerConfig, p listener.Processor, logger loglib.Logger, instrumentation *otel.Instrumentation) (listenersnapshot.Generator, error) {
	var g generator.SnapshotGenerator
	var err error

	// postgres data snapshot generator layer
	if cfg.Data != nil {
		opts := []pgsnapshotgenerator.Option{
			pgsnapshotgenerator.WithLogger(logger),
		}
		if !cfg.DisableProgressTracking {
			opts = append(opts, pgsnapshotgenerator.WithProgressTracking())
		}
		if instrumentation.IsEnabled() {
			opts = append(opts, pgsnapshotgenerator.WithInstrumentation(instrumentation))
		}
		g, err = pgsnapshotgenerator.NewSnapshotGenerator(ctx, cfg.Data, p, opts...)
		if err != nil {
			return nil, err
		}

		// snapshot table finder layer
		finderOpts := []pgtablefinder.Option{}
		if instrumentation.IsEnabled() {
			finderOpts = append(finderOpts, pgtablefinder.WithInstrumentation(instrumentation))
		}

		g, err = pgtablefinder.NewSnapshotSchemaTableFinder(ctx, cfg.Data.URL, g, finderOpts...)
		if err != nil {
			return nil, err
		}
	}

	if cfg.Schema != nil {
		// postgres schema snapshot generator layer
		g, err = newSchemaSnapshotGenerator(ctx, cfg.Schema, g, p, logger, instrumentation, !cfg.DisableProgressTracking)
		if err != nil {
			return nil, err
		}
	}

	if cfg.Recorder != nil && cfg.Recorder.SnapshotStoreURL != "" {
		// snapshot activity recorder layer
		var snapshotStore snapshotstore.Store
		snapshotStore, err := pgsnapshotstore.New(ctx, cfg.Recorder.SnapshotStoreURL)
		if err != nil {
			return nil, fmt.Errorf("create postgres snapshot store: %w", err)
		}
		if instrumentation.IsEnabled() {
			snapshotStore = snapshotstoreinstrumentation.NewStore(snapshotStore, instrumentation)
		}
		g = generator.NewSnapshotRecorder(snapshotStore, g, cfg.Recorder.RepeatableSnapshots)
	}

	if instrumentation.IsEnabled() {
		g, err = generatorinstrumentation.NewSnapshotGenerator(g, instrumentation)
		if err != nil {
			return nil, err
		}
	}

	return adapter.NewSnapshotGeneratorAdapter(&cfg.Adapter, g, adapter.WithLogger(logger)), nil
}

func newSchemaSnapshotGenerator(ctx context.Context, cfg *SchemaSnapshotConfig, g generator.SnapshotGenerator, processor listener.Processor, logger loglib.Logger, instrumentation *otel.Instrumentation, progressTracking bool) (generator.SnapshotGenerator, error) {
	switch {
	case cfg.SchemaLogStore != nil:
		// postgres schemalog schema snapshot generator
		var schemaLogStore schemalog.Store
		var err error
		schemaLogStore, err = schemalogpg.NewStore(ctx, *cfg.SchemaLogStore)
		if err != nil {
			return nil, fmt.Errorf("create schema log postgres store: %w", err)
		}
		schemaLogStore = schemalog.NewStoreCache(schemaLogStore)
		if instrumentation.IsEnabled() {
			schemaLogStore = schemaloginstrumentation.NewStore(schemaLogStore, instrumentation)
		}
		return schemalogsnapshotgenerator.NewSnapshotGenerator(
			schemaLogStore,
			processor,
			schemalogsnapshotgenerator.WithSnapshotGenerator(g),
			schemalogsnapshotgenerator.WithLogger(logger)), nil
	case cfg.DumpRestore != nil:
		// postgres pgdump/pgrestore schema snapshot generator
		opts := []pgdumprestoregenerator.Option{
			pgdumprestoregenerator.WithLogger(logger),
			pgdumprestoregenerator.WithSnapshotGenerator(g),
		}
		if progressTracking {
			opts = append(opts, pgdumprestoregenerator.WithProgressTracking(ctx))
		}
		if instrumentation.IsEnabled() {
			opts = append(opts, pgdumprestoregenerator.WithInstrumentation(instrumentation))
		}
		return pgdumprestoregenerator.NewSnapshotGenerator(ctx, cfg.DumpRestore, opts...)
	default:
		return nil, errSchemaSnapshotNotConfigured
	}
}
