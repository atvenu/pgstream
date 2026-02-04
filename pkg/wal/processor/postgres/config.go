// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"atvenupgstream/pkg/backoff"
	"atvenupgstream/pkg/wal/processor/batch"
	"time"

	schemalogpg "atvenupgstream/pkg/schemalog/postgres"
)

type Config struct {
	URL               string
	BatchConfig       batch.Config
	SchemaLogStore    schemalogpg.Config
	DisableTriggers   bool
	OnConflictAction  string
	BulkIngestEnabled bool
	RetryPolicy       backoff.Config
	IgnoreDDL         bool
}

const (
	defaultInitialInterval = 500 * time.Millisecond
	defaultMaxInterval     = 30 * time.Second
)

func (c *Config) retryPolicy() backoff.Config {
	if c.RetryPolicy.IsSet() {
		return c.RetryPolicy
	}
	return backoff.Config{
		Exponential: &backoff.ExponentialConfig{
			InitialInterval: defaultInitialInterval,
			MaxInterval:     defaultMaxInterval,
		},
	}
}
