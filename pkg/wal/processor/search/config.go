// SPDX-License-Identifier: Apache-2.0

package search

import (
	"atvenupgstream/pkg/wal/processor/batch"
)

type IndexerConfig struct {
	Batch batch.Config
}
