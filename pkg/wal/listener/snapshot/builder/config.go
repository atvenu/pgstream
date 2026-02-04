// SPDX-License-Identifier: Apache-2.0

package builder

import (
	schemalogpg "atvenupgstream/pkg/schemalog/postgres"
	pgsnapshotgenerator "atvenupgstream/pkg/snapshot/generator/postgres/data"
	"atvenupgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"atvenupgstream/pkg/wal/listener/snapshot/adapter"
)

type SnapshotListenerConfig struct {
	Data                    *pgsnapshotgenerator.Config
	Adapter                 adapter.SnapshotConfig
	Recorder                *SnapshotRecorderConfig
	Schema                  *SchemaSnapshotConfig
	DisableProgressTracking bool
}

type SchemaSnapshotConfig struct {
	SchemaLogStore *schemalogpg.Config
	DumpRestore    *pgdumprestore.Config
}

type SnapshotRecorderConfig struct {
	RepeatableSnapshots bool
	SnapshotStoreURL    string
}
