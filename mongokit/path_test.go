package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/lungo/bsonkit"
)

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
