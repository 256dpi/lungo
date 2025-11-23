package mongokit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"

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
			values := coll.Distinct(context.TODO(), path, bson.M{})
			var dec bson.A
			err = values.Decode(&dec)
			assert.NoError(t, err)
			assert.Equal(t, result, dec)
		})
	})

	t.Run("Lungo", func(t *testing.T) {
		fn(func(path string, result bson.A) {
			raw := Distinct(list, path)

			var dec bson.A
			err := bson.RawValue{Type: bson.TypeArray, Value: raw}.Unmarshal(&dec)
			assert.NoError(t, err)
			assert.Equal(t, result, dec)
		})
	})
}

func TestDistinct(t *testing.T) {
	// basic fields
	distinctTest(t, bsonkit.List{
		bsonkit.MustConvert(bson.M{"a": "1"}),
		bsonkit.MustConvert(bson.M{"a": "2"}),
		bsonkit.MustConvert(bson.M{"a": "2"}),
		bsonkit.MustConvert(bson.M{"b": "3"}),
	}, func(fn func(string, bson.A)) {
		fn("a", bson.A{"1", "2"})
	})

	// array fields
	distinctTest(t, bsonkit.List{
		bsonkit.MustConvert(bson.M{"a": "1"}),
		bsonkit.MustConvert(bson.M{"a": bson.A{"1", "2"}}),
		bsonkit.MustConvert(bson.M{"a": "2"}),
	}, func(fn func(string, bson.A)) {
		fn("a", bson.A{"1", "2"})
	})

	// embedded fields (with array fields)
	distinctTest(t, bsonkit.List{
		bsonkit.MustConvert(bson.M{"a": bson.A{bson.M{"b": "1"}}}),
		bsonkit.MustConvert(bson.M{"a": bson.A{bson.M{"b": bson.A{"1", "2"}}}}),
		bsonkit.MustConvert(bson.M{"a": bson.A{bson.M{"b": "1"}, bson.M{"b": "2"}}}),
	}, func(fn func(string, bson.A)) {
		fn("a.b", bson.A{"1", "2"})
	})

	//numbers
	distinctTest(t, bsonkit.List{
		bsonkit.MustConvert(bson.M{"a": int32(1)}),
		bsonkit.MustConvert(bson.M{"a": 1}),
		bsonkit.MustConvert(bson.M{"a": float64(1)}),
		bsonkit.MustConvert(bson.M{"a": "1"}),
	}, func(fn func(string, bson.A)) {
		fn("a", bson.A{int32(1), "1"})
	})
}
