package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
)

func extractTest(t *testing.T, fn func(fn func(bson.M, interface{}))) {
	t.Run("Mongo", func(t *testing.T) {
		coll := testCollection()

		fn(func(query bson.M, result interface{}) {
			_, err := coll.DeleteMany(nil, bson.M{})
			assert.NoError(t, err)

			var out bson.M
			err = coll.FindOneAndUpdate(nil, query, bson.M{
				"$set": bson.M{"_x": "x"},
			}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&out)
			if _, ok := result.(string); ok {
				assert.Error(t, err, query)
				assert.Nil(t, out, query)
			} else {
				delete(out, "_id")
				delete(out, "_x")
				assert.NoError(t, err, query)
				assert.Equal(t, result, out, query)
			}
		})
	})

	t.Run("Lungo", func(t *testing.T) {
		fn(func(query bson.M, result interface{}) {
			doc, err := Extract(bsonkit.Convert(query))
			if str, ok := result.(string); ok {
				assert.Error(t, err)
				assert.Equal(t, str, err.Error())
				assert.Nil(t, doc, query)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, result, doc.Map(), query)
			}
		})
	})
}

func TestExtract(t *testing.T) {
	extractTest(t, func(fn func(bson.M, interface{})) {
		// simple equality condition
		fn(bson.M{
			"foo": "bar",
		}, bson.M{
			"foo": "bar",
		})

		// equality operator expression
		fn(bson.M{
			"foo": bson.M{
				"$eq": "bar",
			},
		}, bson.M{
			"foo": "bar",
		})

		// top level and expression
		fn(bson.M{
			"$and": bson.A{
				bson.M{
					"foo": "bar",
				},
				bson.M{
					"bar": bson.M{
						"$eq": "baz",
					},
				},
			},
		}, bson.M{
			"foo": "bar",
			"bar": "baz",
		})

		// top level or expression (multiple)
		fn(bson.M{
			"$or": bson.A{
				bson.M{
					"foo": "bar",
				},
				bson.M{
					"bar": bson.M{
						"$eq": "baz",
					},
				},
			},
		}, bson.M{})

		// top level or expression (single)
		fn(bson.M{
			"$or": bson.A{
				bson.M{
					"foo": "bar",
				},
			},
		}, bson.M{
			"foo": "bar",
		})

		// in operator expression (multiple)
		fn(bson.M{
			"foo": bson.M{
				"$in": bson.A{"foo", "bar"},
			},
		}, bson.M{})

		// in operator expression (single)
		fn(bson.M{
			"foo": bson.M{
				"$in": bson.A{"bar"},
			},
		}, bson.M{
			"foo": "bar",
		})
	})
}
