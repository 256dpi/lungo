package lungo

import "fmt"

// TODO: Add reflect based test to catch also added options.

func assertUnsupported(features map[string]bool) {
	for name, unsupported := range features {
		if unsupported {
			panic(fmt.Sprintf("lungo: unsupported feature: %s", name))
		}
	}
}
