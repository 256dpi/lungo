package lungo

import (
	"strings"
)

// IsUniquenessError returns true if the provided error is generated due to a
// document failing a unique index constraint.
func IsUniquenessError(err error) bool {
	// check error
	if err == nil {
		return false
	}

	// ge string
	str := err.Error()

	// check if duplicate key error
	if strings.Contains(str, "duplicate key error") {
		return true
	} else if strings.Contains(str, "duplicate document for index") {
		return true
	}

	return false
}
