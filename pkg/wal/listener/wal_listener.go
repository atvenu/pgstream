// SPDX-License-Identifier: Apache-2.0

package listener

import (
	"atvenupgstream/pkg/wal"
	"context"
)

// Listener represents a process that listens to WAL events.
type Listener interface {
	Listen(ctx context.Context) error
	Close() error
}

type ProcessWalEvent func(context.Context, *wal.Event) error

type Processor interface {
	ProcessWALEvent(context.Context, *wal.Event) error
	Name() string
	Close() error
}
