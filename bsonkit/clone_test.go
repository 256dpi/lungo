package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestClone(t *testing.T) {
	doc1 := Convert(bson.M{
		"foo": bson.M{
			"bar": "baz",
		},
		"bar": bson.A{"foo", "bar"},
	})

	doc2 := Clone(doc1)
	assert.Equal(t, doc1, doc2)

	err := Put(doc2, "foo.bar", "quz", false)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": "baz",
		},
		"bar": bson.A{"foo", "bar"},
	}), doc1)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": "quz",
		},
		"bar": bson.A{"foo", "bar"},
	}), doc2)

	a := Get(doc2, "bar", false).(bson.A)
	a = append(a, "baz")
	err = Put(doc2, "bar", a, false)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": "baz",
		},
		"bar": bson.A{"foo", "bar"},
	}), doc1)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": "quz",
		},
		"bar": bson.A{"foo", "bar", "baz"},
	}), doc2)
}
