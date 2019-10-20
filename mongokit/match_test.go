package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// TODO: Properly test all operators.

func matchTest(t *testing.T, doc bson.M, fn func(fn func(bson.M, interface{}))) {
	t.Run("Mongo", func(t *testing.T) {
		coll := testCollection()
		_, err := coll.InsertOne(nil, doc)
		assert.NoError(t, err)

		fn(func(query bson.M, result interface{}) {
			n, err := coll.CountDocuments(nil, query)
			if _, ok := result.(string); ok {
				assert.Error(t, err, query)
				assert.Zero(t, n, query)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, result, n == 1, query)
			}
		})
	})

	t.Run("Lungo", func(t *testing.T) {
		fn(func(query bson.M, result interface{}) {
			res, err := Match(bsonkit.Convert(doc), bsonkit.Convert(query))
			if str, ok := result.(string); ok {
				assert.Error(t, err)
				assert.Equal(t, str, err.Error())
				assert.False(t, res, query)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, result, res, query)
			}
		})
	})
}

func TestMatchQueryComposition(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": "baz",
	}, func(fn func(bson.M, interface{})) {
		// empty query filter
		fn(bson.M{}, true)

		// field condition
		fn(bson.M{
			"foo": "bar",
		}, true)

		// field expression
		fn(bson.M{
			"foo": bson.M{"$eq": "bar"},
		}, true)

		// unknown top level operator
		// top level operator with field condition
		fn(bson.M{
			"$cool": bson.A{
				bson.M{"foo": "bar"},
			},
		}, `match: unknown top level operator "$cool"`)

		// top level operator with field condition
		fn(bson.M{
			"$and": bson.A{
				bson.M{"foo": "bar"},
			},
		}, true)

		// top level operator with field expression
		fn(bson.M{
			"$and": bson.A{
				bson.M{"foo": bson.M{"$eq": "bar"}},
			},
		}, true)

		// top level operator and field condition
		fn(bson.M{
			"foo": "bar",
			"$and": bson.A{
				bson.M{"foo": "bar"},
			},
		}, true)

		// top level operator and field expresion
		fn(bson.M{
			"foo": bson.M{"$eq": "bar"},
			"$and": bson.A{
				bson.M{"foo": "bar"},
			},
		}, true)

		// field expression and field condition
		fn(bson.M{
			"foo": bson.M{"$eq": "bar"},
			"bar": "baz",
		}, true)

		// mixed field expression with initial field condition
		fn(bson.M{
			"foo": bson.D{
				bson.E{Key: "bar", Value: "baz"},
				bson.E{Key: "$eq", Value: "bar"},
			},
		}, false)

		// mixed field expression with initial field expression
		fn(bson.M{
			"foo": bson.D{
				bson.E{Key: "$eq", Value: "bar"},
				bson.E{Key: "bar", Value: "baz"},
			},
		}, `match: expected operator, got "bar"`)

		// unknown expression operator
		fn(bson.M{
			"foo": bson.D{
				bson.E{Key: "$cool", Value: "bar"},
			},
		}, `match: unknown operator "$cool"`)
	})
}

func TestMatchEq(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, interface{})) {
		// field condition
		fn(bson.M{
			"foo": "bar",
		}, true)
		fn(bson.M{
			"foo": "baz",
		}, false)

		// field expression
		fn(bson.M{
			"foo": bson.M{
				"$eq": "bar",
			},
		}, true)
		fn(bson.M{
			"foo": bson.M{
				"$eq": "baz",
			},
		}, false)
	})

	// nested document
	matchTest(t, bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"$eq": "baz",
			},
		},
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": bson.M{
				"bar": bson.M{
					"$eq": "baz",
				},
			},
		}, true)
	})

	// nested document
	matchTest(t, bson.M{
		"foo": bson.M{
			"bar": "baz",
		},
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": bson.M{
				"$eq": bson.M{
					"bar": "baz",
				},
			},
		}, true)
	})

	// array field
	matchTest(t, bson.M{
		"foo": bson.A{
			"bar", "baz",
		},
		"bar": bson.A{
			bson.A{"foo", "bar"}, bson.A{"bar", "baz"},
		},
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": bson.M{
				"$eq": bson.A{
					"bar", "baz",
				},
			},
		}, true)

		fn(bson.M{
			"bar": bson.M{
				"$eq": bson.A{
					"bar", "baz",
				},
			},
		}, true)
	})
}

func TestMatchIn(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": bson.A{"foo", "bar"},
	}, func(fn func(bson.M, interface{})) {
		// missing list
		fn(bson.M{
			"foo": bson.M{"$in": ""},
		}, "match: $in: expected list")

		// empty list
		fn(bson.M{
			"foo": bson.M{"$in": bson.A{}},
		}, false)

		// matching list
		fn(bson.M{
			"foo": bson.M{"$in": bson.A{"bar"}},
		}, true)

		// array item
		fn(bson.M{
			"bar": bson.M{"$in": bson.A{"bar"}},
		}, true)
	})
}

func TestMatchNin(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": bson.A{"foo", "bar"},
	}, func(fn func(bson.M, interface{})) {
		// missing list
		fn(bson.M{
			"foo": bson.M{"$nin": ""},
		}, "match: $nin: expected list")

		// empty list
		fn(bson.M{
			"foo": bson.M{"$nin": bson.A{}},
		}, true)

		// matching list
		fn(bson.M{
			"foo": bson.M{"$nin": bson.A{"bar"}},
		}, false)

		// missing field
		fn(bson.M{
			"baz": bson.M{"$nin": bson.A{"bar"}},
		}, true)

		// array item
		fn(bson.M{
			"bar": bson.M{"$nin": bson.A{"bar"}},
		}, false)
	})
}
