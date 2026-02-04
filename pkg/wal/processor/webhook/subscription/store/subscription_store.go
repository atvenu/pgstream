// SPDX-License-Identifier: Apache-2.0

package store

import (
	"atvenupgstream/pkg/wal/processor/webhook/subscription"
	"context"
)

type Store interface {
	CreateSubscription(ctx context.Context, s *subscription.Subscription) error
	DeleteSubscription(ctx context.Context, s *subscription.Subscription) error
	GetSubscriptions(ctx context.Context, action, schema, table string) ([]*subscription.Subscription, error)
}
