// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"atvenupgstream/pkg/log"
	"atvenupgstream/pkg/snapshot"
	"atvenupgstream/pkg/snapshot/generator"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	generatormocks "atvenupgstream/pkg/snapshot/generator/mocks"
)

func TestSnapshotGeneratorAdapter_CreateSnapshot(t *testing.T) {
	t.Parallel()

	errTest := errors.New("oh noes")

	tests := []struct {
		name         string
		generator    generator.SnapshotGenerator
		schemaTables map[string][]string

		wantErr error
	}{
		{
			name: "ok",
			generator: &generatormocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					require.Equal(t, &snapshot.Snapshot{
						SchemaTables: map[string][]string{
							publicSchema: {"*"},
						},
					}, ss)
					return nil
				},
			},
			schemaTables: map[string][]string{
				publicSchema: {"*"},
			},

			wantErr: nil,
		},
		{
			name: "error",
			generator: &generatormocks.Generator{
				CreateSnapshotFn: func(ctx context.Context, ss *snapshot.Snapshot) error {
					return errTest
				},
			},
			schemaTables: map[string][]string{
				publicSchema: {"*"},
			},

			wantErr: errTest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ga := SnapshotGeneratorAdapter{
				logger:       log.NewNoopLogger(),
				generator:    tc.generator,
				schemaTables: tc.schemaTables,
			}
			defer ga.Close()

			err := ga.CreateSnapshot(context.Background())
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
