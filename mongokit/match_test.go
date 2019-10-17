package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

func matchTest(t *testing.T, doc, query bson.M, result bool) {
	t.Run("Mongo", func(t *testing.T) {
		coll := testCollection()
		_, err := coll.InsertOne(nil, doc)
		assert.NoError(t, err)
		n, err := coll.CountDocuments(nil, query)
		assert.NoError(t, err)
		assert.Equal(t, result, n == 1)
	})

	t.Run("Lungo", func(t *testing.T) {
		res, err := Match(bsonkit.Convert(doc), bsonkit.Convert(query))
		assert.NoError(t, err)
		assert.Equal(t, result, res)
	})
}

func TestMatchEq(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
	}, bson.M{
		"foo": "bar",
	}, true)

	matchTest(t, bson.M{
		"foo": "bar",
	}, bson.M{
		"foo": bson.M{
			"$eq": "bar",
		},
	}, true)

	matchTest(t, bson.M{
		"foo": "bar",
	}, bson.M{
		"foo": "baz",
	}, false)

	matchTest(t, bson.M{
		"foo": "bar",
	}, bson.M{
		"foo": bson.M{
			"$eq": "baz",
		},
	}, false)
}
