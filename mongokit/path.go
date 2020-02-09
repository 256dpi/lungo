package mongokit

import (
	"strings"

	"github.com/256dpi/lungo/bsonkit"
)

const numbers = "1234567890"

// IndexedPath returns true if the specified path contains array indices.
func IndexedPath(path string) bool {
	// fast check
	if !strings.ContainsAny(path, numbers) {
		return false
	}

	// check all segments
	for path != bsonkit.PathEnd {
		// check segment
		_, ok := bsonkit.ParseIndex(bsonkit.PathSegment(path))
		if ok {
			return true
		}

		// reduce path
		path = bsonkit.PathReduce(path)
	}

	return false
}

// SplitDynamicPath will split the provided path on the first positional
// operator. It will return the leading path, the operator and the trailing path.
// The segments may be set to bsonkit.PathEnd if there are not available in the
// path.
func SplitDynamicPath(path string) (string, string, string) {
	// find first "$" operator
	index := strings.Index(path, "$")

	// return full path if no operator has been found
	if index < 0 {
		return path, bsonkit.PathEnd, bsonkit.PathEnd
	}

	// handle root operator
	if index == 0 {
		return bsonkit.PathEnd, bsonkit.PathSegment(path), bsonkit.PathReduce(path)
	}

	// get leading part
	lead := path[:index-1]

	// reduce path
	path = path[index:]

	return lead, bsonkit.PathSegment(path), bsonkit.PathReduce(path)
}
