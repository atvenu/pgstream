// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"atvenupgstream/pkg/snapshot"
	"context"
)

type mockGenerator struct {
	createSnapshotFn func(ctx context.Context, snapshot *snapshot.Snapshot) error
	closeFn          func() error
}

func (m *mockGenerator) CreateSnapshot(ctx context.Context, snapshot *snapshot.Snapshot) error {
	return m.createSnapshotFn(ctx, snapshot)
}

func (m *mockGenerator) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}
