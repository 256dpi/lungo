package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestGet(t *testing.T) {
	doc := bson.D{
		bson.E{Key: "foo", Value: "bar"},
	}

	res := Get(doc, "foo")
	assert.Equal(t, "bar", res)

	res = Get(doc, "bar")
	assert.Equal(t, Missing, res)
}

func TestSet(t *testing.T) {
	doc := bson.D{
		bson.E{Key: "foo", Value: "bar"},
	}

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
