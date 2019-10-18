package lungo

import "fmt"

// TODO: Add reflect based test to catch also added options.

func assertUnsupported(features map[string]bool) error {
	for name, unsupported := range features {
		if unsupported {
			return fmt.Errorf("unsupported feature: %s", name)
		}
	}

	return nil
}
