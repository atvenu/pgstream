// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"atvenupgstream/pkg/wal"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	pglib "atvenupgstream/internal/postgres"
)

type mockProcessor struct {
	eventChan chan *wal.Event
	once      sync.Once
}

func (mp *mockProcessor) ProcessWALEvent(ctx context.Context, event *wal.Event) error {
	mp.eventChan <- event
	return nil
}

func (mp *mockProcessor) Close() error {
	mp.once.Do(func() { close(mp.eventChan) })
	return nil
}

func (mp *mockProcessor) Name() string {
	return "mockProcessor"
}

func execQuery(t *testing.T, ctx context.Context, pgurl, query string) {
	conn, err := pglib.NewConn(ctx, pgurl)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, query)
	require.NoError(t, err)
}
