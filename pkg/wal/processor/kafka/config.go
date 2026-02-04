// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"atvenupgstream/pkg/kafka"
	"atvenupgstream/pkg/wal/processor/batch"
)

type Config struct {
	Kafka kafka.ConnConfig
	Batch batch.Config
}
