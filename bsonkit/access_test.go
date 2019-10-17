package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestGet(t *testing.T) {
	doc := Convert(bson.M{
		"foo": "bar",
	})

	res := Get(doc, "foo")
	assert.Equal(t, "bar", res)

	res = Get(doc, "bar")
	assert.Equal(t, Missing, res)
}

func TestGetNested(t *testing.T) {
	doc := Convert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 42,
			},
		},
	})

	res := Get(doc, "foo")
	assert.Equal(t, bson.D{
		bson.E{Key: "bar", Value: bson.D{
			bson.E{Key: "baz", Value: 42},
		}},
	}, res)

	res = Get(doc, "bar")
	assert.Equal(t, Missing, res)

	res = Get(doc, "foo.bar")
	assert.Equal(t, bson.D{
		bson.E{Key: "baz", Value: 42},
	}, res)

	res = Get(doc, "bar.foo")
	assert.Equal(t, Missing, res)

	res = Get(doc, "foo.bar.baz")
	assert.Equal(t, 42, res)
}

func TestSet(t *testing.T) {
	doc := Convert(bson.M{
		"foo": "bar",
	})

	doc = Set(doc, "bar", "baz", false)
	assert.Equal(t, bson.D{
		bson.E{Key: "foo", Value: "bar"},
		bson.E{Key: "bar", Value: "baz"},
	}, doc)

	doc = Set(doc, "foo", "baz", false)
	assert.Equal(t, bson.D{
		bson.E{Key: "foo", Value: "baz"},
		bson.E{Key: "bar", Value: "baz"},
	}, doc)

	doc = Set(doc, "baz", "quz", true)
	assert.Equal(t, bson.D{
		bson.E{Key: "baz", Value: "quz"},
		bson.E{Key: "foo", Value: "baz"},
		bson.E{Key: "bar", Value: "baz"},
	}, doc)
}
