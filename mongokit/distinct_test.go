package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

func distinctTest(t *testing.T, list bsonkit.List, fn func(fn func(string, bson.A))) {
	t.Run("Mongo", func(t *testing.T) {
		coll := testCollection()

		_, err := coll.DeleteMany(nil, bson.M{})
		assert.NoError(t, err)

		docs := make(bson.A, 0, len(list))
		for _, item := range list {
			docs = append(docs, *item)
		}

		res, err := coll.InsertMany(nil, docs)
		assert.NoError(t, err)
		assert.Equal(t, len(list), len(res.InsertedIDs))

		fn(func(path string, result bson.A) {
			values, err := coll.Distinct(nil, path, bson.M{})
			assert.NoError(t, err)
			assert.Equal(t, result, convertArray(values))
		})
	})

	t.Run("Lungo", func(t *testing.T) {
		fn(func(path string, result bson.A) {
			values := Distinct(list, path)
			assert.Equal(t, result, values)
		})
	})
}

func TestDistinct(t *testing.T) {
	// basic fields
	distinctTest(t, bsonkit.List{
		bsonkit.Convert(bson.M{"a": "1"}),
		bsonkit.Convert(bson.M{"a": "2"}),
		bsonkit.Convert(bson.M{"a": "2"}),
		bsonkit.Convert(bson.M{"b": "3"}),
	}, func(fn func(string, bson.A)) {
		fn("a", bson.A{"1", "2"})
	})

	// array fields
	distinctTest(t, bsonkit.List{
		bsonkit.Convert(bson.M{"a": "1"}),
		bsonkit.Convert(bson.M{"a": bson.A{"1", "2"}}),
		bsonkit.Convert(bson.M{"a": "2"}),
	}, func(fn func(string, bson.A)) {
		fn("a", bson.A{"1", "2"})
	})

	// embedded fields (with array fields)
	distinctTest(t, bsonkit.List{
		bsonkit.Convert(bson.M{"a": bson.A{bson.M{"b": "1"}}}),
		bsonkit.Convert(bson.M{"a": bson.A{bson.M{"b": bson.A{"1", "2"}}}}),
		bsonkit.Convert(bson.M{"a": bson.A{bson.M{"b": "1"}, bson.M{"b": "2"}}}),
	}, func(fn func(string, bson.A)) {
		fn("a.b", bson.A{"1", "2"})
	})

	// numbers
	distinctTest(t, bsonkit.List{
		bsonkit.Convert(bson.M{"a": int32(1)}),
		bsonkit.Convert(bson.M{"a": int64(1)}),
		bsonkit.Convert(bson.M{"a": float64(1)}),
		bsonkit.Convert(bson.M{"a": "1"}),
	}, func(fn func(string, bson.A)) {
		fn("a", bson.A{int32(1), "1"})
	})
}
