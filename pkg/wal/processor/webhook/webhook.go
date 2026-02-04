// SPDX-License-Identifier: Apache-2.0

package webhook

import "atvenupgstream/pkg/wal"

type Payload struct {
	Data *wal.Data
}
