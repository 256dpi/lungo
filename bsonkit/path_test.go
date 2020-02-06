package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPathReduce(t *testing.T) {
	assert.Equal(t, "bar.baz", PathReduce("foo.bar.baz"))
	assert.Equal(t, "baz", PathReduce("bar.baz"))
	assert.Equal(t, PathEnd, PathReduce("baz"))
	assert.Equal(t, PathEnd, PathReduce(""))
}

func TestPathSegment(t *testing.T) {
	assert.Equal(t, "foo", PathSegment("foo.bar.baz"))
	assert.Equal(t, "bar", PathSegment("bar.baz"))
	assert.Equal(t, "baz", PathSegment("baz"))
	assert.Equal(t, "", PathSegment(""))
}

func TestParseIndex(t *testing.T) {
	index, ok := ParseIndex("123")
	assert.True(t, ok)
	assert.Equal(t, 123, index)

	index, ok = ParseIndex("abc")
	assert.False(t, ok)
	assert.Equal(t, 0, index)

	index, ok = ParseIndex("+123")
	assert.False(t, ok)
	assert.Equal(t, 0, index)

	index, ok = ParseIndex("12.3")
	assert.False(t, ok)
	assert.Equal(t, 0, index)

	index, ok = ParseIndex("123.0")
	assert.False(t, ok)
	assert.Equal(t, 0, index)
}
