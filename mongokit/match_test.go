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
			res, err := Match(bsonkit.MustConvert(doc), bsonkit.MustConvert(query))
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

func TestMatch(t *testing.T) {
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

		// nested top level operators
		fn(bson.M{
			"$and": bson.A{
				bson.M{
					"$or": bson.A{
						bson.M{"foo": bson.M{"$eq": "bar"}},
					},
				},
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

func TestMatchAnd(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": true,
	}, func(fn func(bson.M, interface{})) {
		// no array
		fn(bson.M{
			"$and": nil,
		}, "$and: expected array")

		// empty array
		fn(bson.M{
			"$and": bson.A{},
		}, "$and: empty array")

		// match single
		fn(bson.M{
			"$and": bson.A{
				bson.M{
					"foo": "bar",
				},
			},
		}, true)
		fn(bson.M{
			"$and": bson.A{
				bson.M{
					"foo": "baz",
				},
			},
		}, false)

		// match multiple
		fn(bson.M{
			"$and": bson.A{
				bson.M{
					"foo": "bar",
				},
				bson.M{
					"bar": bson.M{
						"$eq": true,
					},
				},
			},
		}, true)
		fn(bson.M{
			"$and": bson.A{
				bson.M{
					"foo": "bar",
				},
				bson.M{
					"bar": bson.M{
						"$eq": false,
					},
				},
			},
		}, false)
		fn(bson.M{
			"$and": bson.A{
				bson.M{
					"foo": "foo",
				},
				bson.M{
					"bar": bson.M{
						"$eq": true,
					},
				},
			},
		}, false)
		fn(bson.M{
			"$and": bson.A{
				bson.M{
					"foo": "foo",
				},
				bson.M{
					"bar": bson.M{
						"$eq": false,
					},
				},
			},
		}, false)

		// as expression
		fn(bson.M{
			"foo": bson.M{
				"$and": bson.A{
					bson.M{
						"$eq": "bar",
					},
				},
			},
		}, `unknown expression operator "$and"`)

		// nesting
		fn(bson.M{
			"$and": bson.A{
				bson.M{
					"$and": bson.A{
						bson.M{"foo": bson.M{"$eq": "bar"}},
					},
				},
			},
		}, true)
	})
}

func TestMatchOr(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": true,
	}, func(fn func(bson.M, interface{})) {
		// no array
		fn(bson.M{
			"$or": nil,
		}, "$or: expected array")

		// empty array
		fn(bson.M{
			"$or": bson.A{},
		}, "$or: empty array")

		// match single
		fn(bson.M{
			"$or": bson.A{
				bson.M{
					"foo": "bar",
				},
			},
		}, true)
		fn(bson.M{
			"$or": bson.A{
				bson.M{
					"foo": "baz",
				},
			},
		}, false)

		// match multiple
		fn(bson.M{
			"$or": bson.A{
				bson.M{
					"foo": "bar",
				},
				bson.M{
					"bar": bson.M{
						"$eq": true,
					},
				},
			},
		}, true)
		fn(bson.M{
			"$or": bson.A{
				bson.M{
					"foo": "bar",
				},
				bson.M{
					"bar": bson.M{
						"$eq": false,
					},
				},
			},
		}, true)
		fn(bson.M{
			"$or": bson.A{
				bson.M{
					"foo": "foo",
				},
				bson.M{
					"bar": bson.M{
						"$eq": true,
					},
				},
			},
		}, true)
		fn(bson.M{
			"$or": bson.A{
				bson.M{
					"foo": "foo",
				},
				bson.M{
					"bar": bson.M{
						"$eq": false,
					},
				},
			},
		}, false)

		// as expression
		fn(bson.M{
			"foo": bson.M{
				"$or": bson.A{
					bson.M{
						"$eq": "bar",
					},
				},
			},
		}, `unknown expression operator "$or"`)

		// nesting
		fn(bson.M{
			"$or": bson.A{
				bson.M{
					"$or": bson.A{
						bson.M{"foo": bson.M{"$eq": "bar"}},
					},
				},
			},
		}, true)
	})
}

func TestMatchNor(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": true,
	}, func(fn func(bson.M, interface{})) {
		// no array
		fn(bson.M{
			"$nor": nil,
		}, "$nor: expected array")

		// empty array
		fn(bson.M{
			"$nor": bson.A{},
		}, "$nor: empty array")

		// match single
		fn(bson.M{
			"$nor": bson.A{
				bson.M{
					"foo": "bar",
				},
			},
		}, false)
		fn(bson.M{
			"$nor": bson.A{
				bson.M{
					"foo": "baz",
				},
			},
		}, true)

		// match multiple
		fn(bson.M{
			"$nor": bson.A{
				bson.M{
					"foo": "bar",
				},
				bson.M{
					"bar": bson.M{
						"$eq": true,
					},
				},
			},
		}, false)
		fn(bson.M{
			"$nor": bson.A{
				bson.M{
					"foo": "bar",
				},
				bson.M{
					"bar": bson.M{
						"$eq": false,
					},
				},
			},
		}, false)
		fn(bson.M{
			"$nor": bson.A{
				bson.M{
					"foo": "foo",
				},
				bson.M{
					"bar": bson.M{
						"$eq": true,
					},
				},
			},
		}, false)
		fn(bson.M{
			"$nor": bson.A{
				bson.M{
					"foo": "foo",
				},
				bson.M{
					"bar": bson.M{
						"$eq": false,
					},
				},
			},
		}, true)

		// as expression
		fn(bson.M{
			"foo": bson.M{
				"$nor": bson.A{
					bson.M{
						"$eq": "bar",
					},
				},
			},
		}, `unknown expression operator "$nor"`)

		// nesting
		fn(bson.M{
			"$nor": bson.A{
				bson.M{
					"$nor": bson.A{
						bson.M{"foo": bson.M{"$eq": "bar"}},
					},
				},
			},
		}, true)
	})
}

func TestMatchComp(t *testing.T) {
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

	// array fields
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
			"foo": bson.A{"bar", "baz"},
		}, true)
		fn(bson.M{
			"foo": bson.M{
				"$eq": bson.A{"bar", "baz"},
			},
		}, true)
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

		// do not match flattened sub array
		fn(bson.M{
			"bar": bson.M{
				"$eq": bson.A{
					"foo", "bar", "bar", "baz",
				},
			},
		}, false)
	})

	// embedded documents
	matchTest(t, bson.M{
		"foo": bson.A{
			"bar",
			bson.M{
				"baz": 7.0,
				"quz": bson.A{
					bson.M{
						"qux": 13.0,
					},
				},
			},
			bson.M{
				"baz": 42.0,
				"quz": bson.A{
					bson.M{
						"qux": bson.A{13.0, 26.0},
					},
				},
			},
		},
	}, func(fn func(bson.M, interface{})) {
		// one level
		fn(bson.M{
			"foo.baz": 7.0,
		}, true)

		// two levels
		fn(bson.M{
			"foo.quz.qux": 13.0,
		}, true)

		// two levels array field
		fn(bson.M{
			"foo.quz.qux": 26.0,
		}, true)

		// do not match merged array
		fn(bson.M{
			"foo.quz.qux": bson.A{13.0, 13.0},
		}, false)
	})

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

		// no match => match
		fn(bson.M{
			"foo": bson.M{
				"$not": bson.M{
					"$eq": "baz",
				},
			},
		}, true)

		// match => no match
		fn(bson.M{
			"foo": bson.M{
				"$not": bson.M{
					"$eq": "bar",
					"$ne": "foo",
				},
			},
		}, false)

		// top level
		fn(bson.M{
			"$not": bson.M{"$eq": "foo"},
		}, `unknown top level operator "$not"`)
	})
}

func TestMatchIn(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": bson.A{"foo", "bar", bson.A{"baz"}},
		"baz": bson.A{
			bson.M{"foo": "baz"},
			bson.M{"foo": bson.A{"bar"}},
		},
	}, func(fn func(bson.M, interface{})) {
		// missing array
		fn(bson.M{
			"foo": bson.M{"$in": ""},
		}, "$in: expected array")

		// empty array
		fn(bson.M{
			"foo": bson.M{"$in": bson.A{}},
		}, false)

		// matching array
		fn(bson.M{
			"foo": bson.M{"$in": bson.A{"bar"}},
		}, true)

		// array field
		fn(bson.M{
			"bar": bson.M{"$in": bson.A{"bar"}},
		}, true)

		// array field (flattened)
		fn(bson.M{
			"bar": bson.M{"$in": bson.A{"baz"}},
		}, false)

		// embedded document
		fn(bson.M{
			"baz.foo": bson.M{"$in": bson.A{"baz"}},
		}, true)

		// embedded document (flattened)
		fn(bson.M{
			"baz.foo": bson.M{"$in": bson.A{"bar"}},
		}, true)
	})
}

func TestMatchNin(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": bson.A{"foo", "bar"},
		"baz": bson.A{
			bson.M{"foo": "baz"},
			bson.M{"foo": "bar"},
		},
	}, func(fn func(bson.M, interface{})) {
		// missing array
		fn(bson.M{
			"foo": bson.M{"$nin": ""},
		}, "$nin: expected array")

		// empty array
		fn(bson.M{
			"foo": bson.M{"$nin": bson.A{}},
		}, true)

		// matching array
		fn(bson.M{
			"foo": bson.M{"$nin": bson.A{"bar"}},
		}, false)

		// missing field
		fn(bson.M{
			"baz": bson.M{"$nin": bson.A{"bar"}},
		}, true)

		// array field
		fn(bson.M{
			"bar": bson.M{"$nin": bson.A{"bar"}},
		}, false)

		// embedded document
		fn(bson.M{
			"baz.foo": bson.M{"$nin": bson.A{"bar"}},
		}, false)
	})
}

func TestMatchExists(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, interface{})) {
		// non boolean
		fn(bson.M{
			"foo": bson.M{"$exists": "bar"},
		}, true)

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

func TestMatchType(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": 7.0,
	}, func(fn func(bson.M, interface{})) {
		// invalid argument
		fn(bson.M{
			"foo": bson.M{"$type": true},
		}, "$type: expected string or number")
		fn(bson.M{
			"foo": bson.M{"$type": "foo"},
		}, "$type: unknown type string")
		fn(bson.M{
			"foo": bson.M{"$type": int32(300)},
		}, "$type: unknown type number")

		// string
		fn(bson.M{
			"foo": bson.M{"$type": "string"},
		}, true)
		fn(bson.M{
			"foo": bson.M{"$type": "double"},
		}, false)
		fn(bson.M{
			"bar": bson.M{"$type": "string"},
		}, false)
		fn(bson.M{
			"bar": bson.M{"$type": "double"},
		}, true)
		fn(bson.M{
			"bar": bson.M{"$type": "number"},
		}, true)

		// number
		fn(bson.M{
			"foo": bson.M{"$type": int32(2)},
		}, true)
		fn(bson.M{
			"foo": bson.M{"$type": int64(2)},
		}, true)
		fn(bson.M{
			"foo": bson.M{"$type": float64(2)},
		}, true)
		fn(bson.M{
			"foo": bson.M{"$type": int64(1)},
		}, false)

		// missing field
		fn(bson.M{
			"baz": bson.M{"type": "string"},
		}, false)
		fn(bson.M{
			"baz": bson.M{"type": "null"},
		}, false)
	})
}

func TestMatchAll(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": bson.A{"foo", "bar"},
		"baz": bson.A{},
		"quz": bson.A{
			bson.M{"foo": "baz"},
			bson.M{"foo": "bar"},
		},
	}, func(fn func(bson.M, interface{})) {
		// missing array
		fn(bson.M{
			"foo": bson.M{"$all": ""},
		}, "$all: expected array")

		// empty array
		fn(bson.M{
			"foo": bson.M{"$all": bson.A{}},
		}, false)
		fn(bson.M{
			"baz": bson.M{"$all": bson.A{}},
		}, false)

		// matching array
		fn(bson.M{
			"foo": bson.M{"$all": bson.A{"bar"}},
		}, true)
		fn(bson.M{
			"foo": bson.M{"$all": bson.A{"bar", "baz"}},
		}, false)

		// array field
		fn(bson.M{
			"bar": bson.M{"$all": bson.A{"foo"}},
		}, true)
		fn(bson.M{
			"bar": bson.M{"$all": bson.A{"foo", "bar"}},
		}, true)
		fn(bson.M{
			"bar": bson.M{"$all": bson.A{"foo", "bar", "baz"}},
		}, false)

		// embedded document
		fn(bson.M{
			"quz.foo": bson.M{"$all": bson.A{"bar", "baz"}},
		}, true)
	})
}

func TestMatchSize(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": bson.A{"foo", "bar"},
		"baz": bson.A{
			bson.M{"foo": "baz"},
			bson.M{"foo": "bar"},
		},
	}, func(fn func(bson.M, interface{})) {
		// invalid value
		fn(bson.M{
			"foo": bson.M{"$size": false},
		}, "$size: expected number")

		// non array
		fn(bson.M{
			"foo": bson.M{"$size": int32(0)},
		}, false)
		fn(bson.M{
			"foo": bson.M{"$size": int32(1)},
		}, false)

		// matching array
		fn(bson.M{
			"bar": bson.M{"$size": int32(0)},
		}, false)
		fn(bson.M{
			"bar": bson.M{"$size": int32(1)},
		}, false)
		fn(bson.M{
			"bar": bson.M{"$size": int32(2)},
		}, true)
		fn(bson.M{
			"bar": bson.M{"$size": int32(3)},
		}, false)

		// embedded fields (no support)
		fn(bson.M{
			"baz.foo": bson.M{"$size": int32(2)},
		}, false)
	})
}

func TestMatchElem(t *testing.T) {
	matchTest(t, bson.M{
		"foo": "bar",
		"bar": bson.A{"foo", "bar"},
		"baz": bson.A{
			bson.M{
				"foo": 7.0,
				"bar": bson.A{
					bson.M{
						"baz": 13.0,
					},
				},
			},
			bson.M{
				"foo": 42.0,
				"bar": bson.A{
					bson.M{
						"baz": bson.A{13.0, 26.0},
					},
				},
			},
		},
	}, func(fn func(bson.M, interface{})) {
		// invalid value
		fn(bson.M{
			"foo": bson.M{"$elemMatch": false},
		}, "$elemMatch: expected document")

		// no array
		fn(bson.M{
			"foo": bson.M{"$elemMatch": bson.M{}},
		}, false)

		// no query
		fn(bson.M{
			"bar": bson.M{"$elemMatch": bson.M{}},
		}, false)

		// basic query
		fn(bson.M{
			"bar": bson.M{"$elemMatch": bson.M{
				"$eq": "foo",
			}},
		}, true)

		// embedded field
		fn(bson.M{
			"baz": bson.M{"$elemMatch": bson.M{
				"foo": bson.M{
					"$gt": 9.0,
				},
			}},
		}, true)

		// nested field
		fn(bson.M{
			"baz.bar": bson.M{"$elemMatch": bson.M{
				"baz": bson.M{
					"$gt": 15.0,
				},
			}},
		}, true)
	})
}
