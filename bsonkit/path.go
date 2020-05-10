package bsonkit

import (
	"strconv"
	"strings"
)

// PathEnd is returned by X if the end of the path has been reached.
var PathEnd = "\x00"

// ReducePath will reduce the path by one segment. It will return PathEnd if
// there are no more segments.
func ReducePath(path string) string {
	// get next dot
	i := strings.IndexByte(path, '.')
	if i >= 0 {
		return path[i+1:]
	}

	return PathEnd
}

// PathSegment will return the first segment of the path.
func PathSegment(path string) string {
	// get first dot
	i := strings.IndexByte(path, '.')
	if i >= 0 {
		return path[:i]
	}

	return path
}

// ParseIndex will attempt to parse the provided string as an index.
func ParseIndex(str string) (int, bool) {
	// check if strings begins with a number
	if len(str) == 0 || str[0] < '0' || str[0] > '9' {
		return 0, false
	}

	// parse number
	index, err := strconv.Atoi(str)
	if err != nil {
		return 0, false
	}

	return index, true
}
