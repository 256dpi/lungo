package mongokit

import (
	"strconv"
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

// PathBuilder is a memory efficient builder for paths.
type PathBuilder struct {
	buf []byte
	len int
}

// NewPathBuilder creates a new path builder with the provided allocated memory.
func NewPathBuilder(buffer int) *PathBuilder {
	return &PathBuilder{
		buf: make([]byte, buffer),
	}
}

// AddSegment will add the specified segment.
func (b *PathBuilder) AddSegment(seg string) int {
	// add separator
	if b.len > 0 {
		b.buf[b.len] = '.'
		b.len++
	}

	// copy segment
	b.len += copy(b.buf[b.len:], seg)

	return b.len
}

// AddIndex will add the specified index.
func (b *PathBuilder) AddIndex(idx int) int {
	// add separator
	if b.len > 0 {
		b.buf[b.len] = '.'
		b.len++
	}

	// append number
	ret := strconv.AppendInt(b.buf[b.len:b.len], int64(idx), 10)
	b.len += len(ret)

	return b.len
}

// Truncate will shrink the buffer to the provided length.
func (b *PathBuilder) Truncate(len int) {
	// set length
	if len < b.len {
		b.len = len
	}
}

// String will return the built path.
func (b *PathBuilder) String() string {
	return string(b.buf[:b.len])
}
