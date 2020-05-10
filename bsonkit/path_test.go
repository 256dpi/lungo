package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReducePath(t *testing.T) {
	assert.Equal(t, "bar.baz", ReducePath("foo.bar.baz"))
	assert.Equal(t, "baz", ReducePath("bar.baz"))
	assert.Equal(t, PathEnd, ReducePath("baz"))
	assert.Equal(t, PathEnd, ReducePath(""))
}

func TestReducePathReverse(t *testing.T) {
	assert.Equal(t, "foo.bar", ReducePathReverse("foo.bar.baz"))
	assert.Equal(t, "bar", ReducePathReverse("bar.baz"))
	assert.Equal(t, PathEnd, ReducePathReverse("baz"))
	assert.Equal(t, PathEnd, ReducePathReverse(""))
}

func TestPathSegment(t *testing.T) {
	assert.Equal(t, "foo", PathSegment("foo.bar.baz"))
	assert.Equal(t, "bar", PathSegment("bar.baz"))
	assert.Equal(t, "baz", PathSegment("baz"))
	assert.Equal(t, "", PathSegment(""))
}

func TestPathSegmentReverse(t *testing.T) {
	assert.Equal(t, "foo", PathSegmentReverse("baz.bar.foo"))
	assert.Equal(t, "bar", PathSegmentReverse("baz.bar"))
	assert.Equal(t, "baz", PathSegmentReverse("baz"))
	assert.Equal(t, "", PathSegmentReverse(""))
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
