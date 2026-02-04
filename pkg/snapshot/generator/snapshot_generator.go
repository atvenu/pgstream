// SPDX-License-Identifier: Apache-2.0

package generator

import (
	"atvenupgstream/pkg/snapshot"
	"context"
)

type SnapshotGenerator interface {
	CreateSnapshot(ctx context.Context, snapshot *snapshot.Snapshot) error
	Close() error
}
