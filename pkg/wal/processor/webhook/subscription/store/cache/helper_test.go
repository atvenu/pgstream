// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"atvenupgstream/pkg/wal/processor/webhook/subscription"
	"errors"
)

var errTest = errors.New("oh noes")

func newTestSubscription(url, schema, table string, eventTypes []string) *subscription.Subscription {
	return &subscription.Subscription{
		URL:        url,
		Schema:     schema,
		Table:      table,
		EventTypes: eventTypes,
	}
}
