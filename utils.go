package lungo

import "fmt"

func assertUnsupported(features map[string]bool) error {
	for name, unsupported := range features {
		if unsupported {
			return fmt.Errorf("unsupported feature: %s", name)
		}
	}

	return nil
}
