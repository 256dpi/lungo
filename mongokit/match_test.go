package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

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
		fn(bson.M{
			"$cool": bson.A{
				bson.M{"foo": "bar"},
			},
		}, `unknown top level operator "$cool"`)

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
		}, `expected operator, got "bar"`)

		// unknown expression operator
		fn(bson.M{
			"foo": bson.D{
				bson.E{Key: "$cool", Value: "bar"},
			},
		}, `unknown expression operator "$cool"`)

		// nested top level operator
		fn(bson.M{
			"foo": bson.M{
				"$and": bson.A{
					bson.M{},
				},
			},
		}, `unknown expression operator "$and"`)
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

	// nested document (condition)
	matchTest(t, bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"$foo": "baz",
			},
		},
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": bson.M{
				"bar": bson.M{
					"$foo": "baz",
				},
			},
		}, true)
	})

	// nested document (operator)
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

	// array fields (unwind)
	matchTest(t, bson.M{
		"foo": bson.A{
			"bar", "baz",
		},
		"bar": bson.A{
			bson.A{"foo", "bar"}, bson.A{"bar", "baz"},
		},
		"baz": "quz",
	}, func(fn func(bson.M, interface{})) {
		// match element
		fn(bson.M{
			"foo": "bar",
		}, true)
		fn(bson.M{
			"foo": bson.M{
				"$eq": "bar",
			},
		}, true)
		fn(bson.M{
			"foo": bson.M{
				"$eq": "quz",
			},
		}, false)

		// match array
		fn(bson.M{
			"baz": bson.A{"bar", "quz"},
		}, false)
		fn(bson.M{
			"baz": bson.M{
				"$eq": bson.A{"bar", "quz"},
			},
		}, false)

		// match sub array
		fn(bson.M{
			"bar": bson.M{
				"$eq": bson.A{
					"bar", "baz",
				},
			},
		}, true)
		fn(bson.M{
			"bar": bson.M{
				"$eq": bson.A{
					"baz", "bar",
				},
			},
		}, false)
	})
}

func TestMatchComp(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": bson.A{7.0, 42.0},
	}, func(fn func(bson.M, interface{})) {
		// greater than (wrong type)
		fn(bson.M{
			"foo": bson.M{"$gt": int64(0)},
		}, false)

		// lesser field (wrong type)
		fn(bson.M{
			"foo": bson.M{"$lt": int64(0)},
		}, false)

		// greater than
		fn(bson.M{
			"foo": bson.M{"$gt": "a"},
		}, true)

		// lesser field
		fn(bson.M{
			"foo": bson.M{"$lt": "z"},
		}, true)

		// array field (unwind)
		fn(bson.M{
			"bar": bson.M{"$gt": 13.0},
		}, true)
	})
}

func TestMatchNot(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, interface{})) {
		// no document
		fn(bson.M{
			"foo": bson.M{"$not": ""},
		}, "$not: expected document")

		// empty document
		fn(bson.M{
			"foo": bson.M{"$not": bson.M{}},
		}, "$not: empty document")

		// empty list
		fn(bson.M{
			"foo": bson.M{
				"$not": bson.M{
					"$eq": "baz",
				},
			},
		}, true)

		// empty list
		fn(bson.M{
			"foo": bson.M{
				"$not": bson.M{
					"$eq": "bar",
					"$ne": "foo",
				},
			},
		}, false)
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
		}, "$in: expected list")

		// empty list
		fn(bson.M{
			"foo": bson.M{"$in": bson.A{}},
		}, false)

		// matching list
		fn(bson.M{
			"foo": bson.M{"$in": bson.A{"bar"}},
		}, true)

		// array field (unwind)
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
		}, "$nin: expected list")

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

		// array field (unwind)
		fn(bson.M{
			"bar": bson.M{"$nin": bson.A{"bar"}},
		}, false)
	})
}

func TestMatchExists(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, interface{})) {
		// present field
		fn(bson.M{
			"foo": bson.M{"$exists": true},
		}, true)

		// present field
		fn(bson.M{
			"foo": bson.M{"$exists": false},
		}, false)

		// missing field
		fn(bson.M{
			"bar": bson.M{"$exists": true},
		}, false)

		// missing field
		fn(bson.M{
			"bar": bson.M{"$exists": false},
		}, true)
	})
}
