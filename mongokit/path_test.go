package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/lungo/bsonkit"
)

func TestIndexedPath(t *testing.T) {
	assert.False(t, IndexedPath(""))
	assert.False(t, IndexedPath(bsonkit.PathEnd))
	assert.False(t, IndexedPath("foo.bar"))
	assert.False(t, IndexedPath("foo1.2bar"))
	assert.True(t, IndexedPath("0.foo.bar"))
	assert.True(t, IndexedPath("foo.1.bar"))
	assert.True(t, IndexedPath("foo.bar.2"))
}

func TestSplitDynamicPath(t *testing.T) {
	head, operator, tail := SplitDynamicPath("foo.bar.$[baz].quz")
	assert.Equal(t, "foo.bar", head)
	assert.Equal(t, "$[baz]", operator)
	assert.Equal(t, "quz", tail)

	head, operator, tail = SplitDynamicPath("foo.bar.baz")
	assert.Equal(t, "foo.bar.baz", head)
	assert.Equal(t, bsonkit.PathEnd, operator)
	assert.Equal(t, bsonkit.PathEnd, tail)

	head, operator, tail = SplitDynamicPath("foo.bar.baz.$")
	assert.Equal(t, head, "foo.bar.baz")
	assert.Equal(t, operator, "$")
	assert.Equal(t, bsonkit.PathEnd, tail)

	head, operator, tail = SplitDynamicPath("$[].foo.bar.baz")
	assert.Equal(t, bsonkit.PathEnd, head)
	assert.Equal(t, "$[]", operator)
	assert.Equal(t, "foo.bar.baz", tail)

	head, operator, tail = SplitDynamicPath("$")
	assert.Equal(t, bsonkit.PathEnd, head)
	assert.Equal(t, "$", operator)
	assert.Equal(t, bsonkit.PathEnd, tail)

	head, operator, tail = SplitDynamicPath("")
	assert.Equal(t, "", head)
	assert.Equal(t, bsonkit.PathEnd, operator)
	assert.Equal(t, bsonkit.PathEnd, tail)

	head, operator, tail = SplitDynamicPath(bsonkit.PathEnd)
	assert.Equal(t, bsonkit.PathEnd, head)
	assert.Equal(t, bsonkit.PathEnd, operator)
	assert.Equal(t, bsonkit.PathEnd, tail)
}

func TestPathBuilder(t *testing.T) {
	pb := NewPathBuilder(200)
	assert.Equal(t, "", pb.String())

	pb.AddSegment("foo")
	assert.Equal(t, "foo", pb.String())

	pb.AddIndex(42)
	assert.Equal(t, "foo.42", pb.String())

	pb.AddSegment("bar")
	assert.Equal(t, "foo.42.bar", pb.String())
}

func BenchmarkPathBuilder(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pb := NewPathBuilder(6)
		pb.AddSegment("foo")
		pb.AddIndex(42)
		_ = pb.String()
	}
}
