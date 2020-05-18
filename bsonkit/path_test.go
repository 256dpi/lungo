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

func TestPathNode(t *testing.T) {
	root := PathNode{}

	/* append */

	node := root.Append("foo.bar.baz")
	assert.Equal(t, root["foo"].(PathNode)["bar"].(PathNode)["baz"], node)
	assert.Equal(t, PathNode{
		"foo": PathNode{
			"bar": PathNode{
				"baz": PathNode{},
			},
		},
	}, root)

	node = root.Append("foo.bar.quz")
	assert.Equal(t, root["foo"].(PathNode)["bar"].(PathNode)["quz"], node)
	assert.Equal(t, PathNode{
		"foo": PathNode{
			"bar": PathNode{
				"baz": PathNode{},
				"quz": PathNode{},
			},
		},
	}, root)

	node = root.Append("foo.bar.baz")
	assert.Equal(t, root["foo"].(PathNode)["bar"].(PathNode)["baz"], node)
	assert.Equal(t, PathNode{
		"foo": PathNode{
			"bar": PathNode{
				"baz": PathNode{},
				"quz": PathNode{},
			},
		},
	}, root)

	/* lookup */

	node, path := root.Lookup(PathEnd)
	assert.Equal(t, PathEnd, path)
	assert.Equal(t, root, node)

	node, path = root.Lookup("foo")
	assert.Equal(t, PathEnd, path)
	assert.Equal(t, root["foo"], node)

	node, path = root.Lookup("foo.bar")
	assert.Equal(t, PathEnd, path)
	assert.Equal(t, root["foo"].(PathNode)["bar"], node)

	node, path = root.Lookup("foo.bar.baz")
	assert.Equal(t, PathEnd, path)
	assert.Equal(t, root["foo"].(PathNode)["bar"].(PathNode)["baz"], node)

	node, path = root.Lookup("foo.bar.quz")
	assert.Equal(t, PathEnd, path)
	assert.Equal(t, root["foo"].(PathNode)["bar"].(PathNode)["quz"], node)

	node, path = root.Lookup("foo.bar.baz.quz")
	assert.Equal(t, "quz", path)
	assert.Equal(t, root["foo"].(PathNode)["bar"].(PathNode)["baz"], node)

	node, path = root.Lookup("foo.bar.qux")
	assert.Equal(t, "qux", path)
	assert.Equal(t, root["foo"].(PathNode)["bar"], node)

	node, path = root.Lookup("bar.baz")
	assert.Equal(t, "bar.baz", path)
	assert.Equal(t, root, node)
}

func BenchmarkPathNode(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		node := NewPathNode()
		node.Append("foo.bar.baz").Store(1)
		ret, _ := node.Lookup("foo.bar.baz")
		if ret.Load() != 1 {
			panic("error")
		}
		node.Recycle()
	}
}
