// SPDX-License-Identifier: Apache-2.0

package config

import (
	"atvenupgstream/pkg/backoff"
	"atvenupgstream/pkg/kafka"
	"atvenupgstream/pkg/otel"
	"atvenupgstream/pkg/snapshot/generator/postgres/schema/pgdumprestore"
	"atvenupgstream/pkg/stream"
	"atvenupgstream/pkg/tls"
	"atvenupgstream/pkg/wal/listener/snapshot/adapter"
	"atvenupgstream/pkg/wal/listener/snapshot/builder"
	"atvenupgstream/pkg/wal/processor/batch"
	"atvenupgstream/pkg/wal/processor/filter"
	"atvenupgstream/pkg/wal/processor/injector"
	"atvenupgstream/pkg/wal/processor/postgres"
	"atvenupgstream/pkg/wal/processor/search"
	"atvenupgstream/pkg/wal/processor/search/store"
	"atvenupgstream/pkg/wal/processor/transformer"
	"atvenupgstream/pkg/wal/processor/webhook/notifier"
	"atvenupgstream/pkg/wal/processor/webhook/subscription/server"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	schemalogpg "atvenupgstream/pkg/schemalog/postgres"
	pgsnapshotgenerator "atvenupgstream/pkg/snapshot/generator/postgres/data"

	kafkacheckpoint "atvenupgstream/pkg/wal/checkpointer/kafka"

	kafkaprocessor "atvenupgstream/pkg/wal/processor/kafka"

	pgreplication "atvenupgstream/pkg/wal/replication/postgres"
)

// this function validates the stream configuration produced from the test
// configuration in the test directory.
func validateTestStreamConfig(t *testing.T, streamConfig *stream.Config) {
	expectedConfig := &stream.Config{
		Listener: stream.ListenerConfig{
			Postgres: &stream.PostgresListenerConfig{
				URL: "postgresql://user:password@localhost:5432/mydatabase",
				Replication: pgreplication.Config{
					PostgresURL:         "postgresql://user:password@localhost:5432/mydatabase",
					ReplicationSlotName: "pgstream_mydatabase_slot",
					IncludeTables:       []string{"test", "test_schema.test", "another_schema.*"},
					ExcludeTables:       []string{"excluded_test", "excluded_schema.test", "another_excluded_schema.*"},
				},
				RetryPolicy: backoff.Config{
					DisableRetries: true,
					Exponential: &backoff.ExponentialConfig{
						MaxRetries:      5,
						InitialInterval: time.Second,
						MaxInterval:     60 * time.Second,
					},
				},
				Snapshot: &builder.SnapshotListenerConfig{
					Adapter: adapter.SnapshotConfig{
						Tables:         []string{"test", "test_schema.Test", "another_schema.*"},
						ExcludedTables: []string{"test_schema.Test"},
					},
					Data: &pgsnapshotgenerator.Config{
						URL:             "postgresql://user:password@localhost:5432/mydatabase",
						SnapshotWorkers: 4,
						SchemaWorkers:   4,
						TableWorkers:    4,
						BatchBytes:      83886080,
						MaxConnections:  20,
					},
					Schema: &builder.SchemaSnapshotConfig{
						DumpRestore: &pgdumprestore.Config{
							SourcePGURL:            "postgresql://user:password@localhost:5432/mydatabase",
							TargetPGURL:            "postgresql://user:password@localhost:5432/mytargetdatabase",
							CleanTargetDB:          true,
							CreateTargetDB:         true,
							IncludeGlobalDBObjects: true,
							RolesSnapshotMode:      "disabled",
							Role:                   "test-role",
							NoOwner:                true,
							NoPrivileges:           true,
							DumpDebugFile:          "pg_dump.sql",
							ExcludedSecurityLabels: []string{"anon"},
						},
					},
					Recorder: &builder.SnapshotRecorderConfig{
						SnapshotStoreURL:    "postgresql://user:password@localhost:5432/mytargetdatabase",
						RepeatableSnapshots: true,
					},
					DisableProgressTracking: true,
				},
			},
			Kafka: &stream.KafkaListenerConfig{
				Reader: kafka.ReaderConfig{
					Conn: kafka.ConnConfig{
						Servers: []string{"localhost:9092"},
						Topic: kafka.TopicConfig{
							Name: "mytopic",
						},
						TLS: tls.Config{
							Enabled:        true,
							CaCertFile:     "/path/to/ca.crt",
							ClientCertFile: "/path/to/client.crt",
							ClientKeyFile:  "/path/to/client.key",
						},
					},
					ConsumerGroupID:          "mygroup",
					ConsumerGroupStartOffset: "earliest",
				},
				Checkpointer: kafkacheckpoint.Config{
					CommitBackoff: backoff.Config{
						DisableRetries: true,
						Exponential: &backoff.ExponentialConfig{
							MaxRetries:      5,
							InitialInterval: time.Second,
							MaxInterval:     60 * time.Second,
						},
					},
				},
			},
		},
		Processor: stream.ProcessorConfig{
			Postgres: &stream.PostgresProcessorConfig{
				BatchWriter: postgres.Config{
					URL: "postgresql://user:password@localhost:5432/mytargetdatabase",
					BatchConfig: batch.Config{
						MaxBatchSize:     100,
						BatchTimeout:     time.Second,
						MaxBatchBytes:    1572864,
						MaxQueueBytes:    204800,
						IgnoreSendErrors: true,
						AutoTune: batch.AutoTuneConfig{
							Enabled:              true,
							MinBatchBytes:        10,
							MaxBatchBytes:        1000,
							ConvergenceThreshold: 0.05,
						},
					},
					DisableTriggers:   false,
					OnConflictAction:  "nothing",
					BulkIngestEnabled: true,
					SchemaLogStore: schemalogpg.Config{
						URL: "postgresql://user:password@localhost:5432/mydatabase",
					},
					RetryPolicy: backoff.Config{
						DisableRetries: true,
						Exponential: &backoff.ExponentialConfig{
							MaxRetries:      5,
							InitialInterval: time.Second,
							MaxInterval:     60 * time.Second,
						},
					},
					IgnoreDDL: true,
				},
			},
			Kafka: &stream.KafkaProcessorConfig{
				Writer: &kafkaprocessor.Config{
					Kafka: kafka.ConnConfig{
						Topic: kafka.TopicConfig{
							Name:              "mytopic",
							NumPartitions:     1,
							ReplicationFactor: 1,
							AutoCreate:        true,
						},
						Servers: []string{"localhost:9092"},
						TLS: tls.Config{
							Enabled:        true,
							CaCertFile:     "/path/to/ca.crt",
							ClientCertFile: "/path/to/client.crt",
							ClientKeyFile:  "/path/to/client.key",
						},
					},
					Batch: batch.Config{
						MaxBatchSize:     100,
						BatchTimeout:     time.Second,
						MaxBatchBytes:    1572864,
						MaxQueueBytes:    204800,
						IgnoreSendErrors: true,
					},
				},
			},
			Search: &stream.SearchProcessorConfig{
				Store: store.Config{
					ElasticsearchURL: "http://localhost:9200",
				},
				Indexer: search.IndexerConfig{
					Batch: batch.Config{
						MaxBatchSize:     100,
						BatchTimeout:     time.Second,
						MaxBatchBytes:    1572864,
						MaxQueueBytes:    204800,
						IgnoreSendErrors: true,
					},
				},
				Retrier: search.StoreRetryConfig{
					Backoff: backoff.Config{
						DisableRetries: true,
						Exponential: &backoff.ExponentialConfig{
							MaxRetries:      5,
							InitialInterval: time.Second,
							MaxInterval:     60 * time.Second,
						},
					},
				},
			},
			Webhook: &stream.WebhookProcessorConfig{
				SubscriptionServer: server.Config{
					Address:      "localhost:9090",
					ReadTimeout:  time.Minute,
					WriteTimeout: time.Minute,
				},
				Notifier: notifier.Config{
					URLWorkerCount: 4,
					ClientTimeout:  time.Second,
				},
				SubscriptionStore: stream.WebhookSubscriptionStoreConfig{
					URL:                  "postgresql://user:password@localhost:5432/mydatabase",
					CacheEnabled:         true,
					CacheRefreshInterval: 60 * time.Second,
				},
			},
			Injector: &injector.Config{
				Store: schemalogpg.Config{
					URL: "postgresql://user:password@localhost:5432/mydatabase",
				},
			},
			Transformer: &transformer.Config{
				InferFromSecurityLabels: false,
				DumpInferredRules:       false,
				ValidationMode:          "relaxed",
				TransformerRules: []transformer.TableRules{
					{
						Schema:         "public",
						Table:          "test",
						ValidationMode: "relaxed",
						ColumnRules: map[string]transformer.TransformerRules{
							"name": {
								Name: "greenmask_firstname",
								DynamicParameters: map[string]any{
									"gender": map[string]any{
										"column": "sex",
									},
								},
							},
						},
					},
				},
			},
			Filter: &filter.Config{
				IncludeTables: []string{"test", "test_schema.test", "another_schema.*"},
				ExcludeTables: []string{"excluded_test", "excluded_schema.test", "another_excluded_schema.*"},
			},
		},
	}

	assert.Equal(t, expectedConfig, streamConfig)
}

// this function validates the otel configuration produced from the test
// configuration in the test directory.
func validateTestOtelConfig(t *testing.T, otelConfig *otel.Config) {
	assert.Equal(t, "http://localhost:4317", otelConfig.Metrics.Endpoint)
	assert.Equal(t, 60*time.Second, otelConfig.Metrics.CollectionInterval)

	assert.Equal(t, "http://localhost:4317", otelConfig.Traces.Endpoint)
	assert.Equal(t, 0.5, otelConfig.Traces.SampleRatio)
}
