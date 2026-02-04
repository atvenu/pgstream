// SPDX-License-Identifier: Apache-2.0

package main

import (
	"atvenupgstream/cmd"
	"os"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
