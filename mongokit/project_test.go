package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
)

func projectTest(t *testing.T, doc bson.M, fn func(fn func(bson.M, interface{}))) {
	t.Run("Mongo", func(t *testing.T) {
		coll := testCollection()
		res, err := coll.InsertOne(nil, doc)
		assert.NoError(t, err)

		fn(func(projection bson.M, result interface{}) {
			var out bson.M
			err := coll.FindOne(nil, bson.M{
				"_id": res.InsertedID,
			}, options.FindOne().SetProjection(projection)).Decode(&out)
			if _, ok := result.(string); ok {
				assert.Error(t, err)
				assert.Zero(t, out)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, result, out)
			}
		})
	})

	t.Run("Lungo", func(t *testing.T) {
		fn(func(projection bson.M, result interface{}) {
			res, err := Project(bsonkit.MustConvert(doc), bsonkit.MustConvert(projection))
			if str, ok := result.(string); ok {
				assert.Error(t, err)
				assert.Equal(t, str, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, bsonkit.MustConvert(result.(bson.M)), res)
			}
		})
	})
}

func TestProject(t *testing.T) {
	id := bson.NewObjectID()

	// hide id
	projectTest(t, bson.M{
		"_id": id,
		"foo": "bar",
		"bar": "baz",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"_id": 0,
		}, bson.M{
			"foo": "bar",
			"bar": "baz",
		})
	})

	// include
	projectTest(t, bson.M{
		"_id": id,
		"foo": "bar",
		"bar": "baz",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": 1,
		}, bson.M{
			"_id": id,
			"foo": "bar",
		})
	})

	// include, hide id
	projectTest(t, bson.M{
		"_id": id,
		"foo": "bar",
		"bar": "baz",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"_id": 0,
			"foo": 1,
		}, bson.M{
			"foo": "bar",
		})
	})

	// exclude
	projectTest(t, bson.M{
		"_id": id,
		"foo": "bar",
		"bar": "baz",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": 0,
		}, bson.M{
			"_id": id,
			"bar": "baz",
		})
	})

	// exclude, hide id
	projectTest(t, bson.M{
		"_id": id,
		"foo": "bar",
		"bar": "baz",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"_id": 0,
			"foo": 0,
		}, bson.M{
			"bar": "baz",
		})
	})

	// exclude multiple fields
	projectTest(t, bson.M{
		"_id": id,
		"foo": "bar",
		"bar": "baz",
		"qux": "quux",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": 0,
			"bar": 0,
		}, bson.M{
			"_id": id,
			"qux": "quux",
		})
	})

	// boolean true/false are equivalent to numeric 1/0
	projectTest(t, bson.M{
		"_id": id,
		"foo": "bar",
		"bar": "baz",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": true,
		}, bson.M{
			"_id": id,
			"foo": "bar",
		})
		fn(bson.M{
			"foo": false,
		}, bson.M{
			"_id": id,
			"bar": "baz",
		})
	})

	// TODO: Test allowed mixing with _id.

	// mixed projection
	projectTest(t, bson.M{
		"_id": id,
		"foo": "bar",
		"bar": "baz",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": 0,
			"bar": 1,
		}, "cannot have a mix of inclusion and exclusion")
	})
}

func TestProjectSlice(t *testing.T) {
	id := bson.NewObjectID()

	projectTest(t, bson.M{
		"_id": id,
		"foo": bson.A{
			bson.M{
				"a": 1.0,
			},
			bson.M{
				"a": 2.0,
			},
			bson.M{
				"a": 3.0,
			},
		},
	}, func(fn func(bson.M, interface{})) {
		// zero
		fn(bson.M{
			"foo": bson.M{
				"$slice": 0,
			},
		}, bson.M{
			"_id": id,
			"foo": bson.A{},
		})

		// first one
		fn(bson.M{
			"foo": bson.M{
				"$slice": 1,
			},
		}, bson.M{
			"_id": id,
			"foo": bson.A{
				bson.M{
					"a": 1.0,
				},
			},
		})

		// first two
		fn(bson.M{
			"foo": bson.M{
				"$slice": 2,
			},
		}, bson.M{
			"_id": id,
			"foo": bson.A{
				bson.M{
					"a": 1.0,
				},
				bson.M{
					"a": 2.0,
				},
			},
		})

		// last one
		fn(bson.M{
			"foo": bson.M{
				"$slice": -1,
			},
		}, bson.M{
			"_id": id,
			"foo": bson.A{
				bson.M{
					"a": 3.0,
				},
			},
		})

		// last two
		fn(bson.M{
			"foo": bson.M{
				"$slice": -2,
			},
		}, bson.M{
			"_id": id,
			"foo": bson.A{
				bson.M{
					"a": 2.0,
				},
				bson.M{
					"a": 3.0,
				},
			},
		})

		// overload positive
		fn(bson.M{
			"foo": bson.M{
				"$slice": 5,
			},
		}, bson.M{
			"_id": id,
			"foo": bson.A{
				bson.M{
					"a": 1.0,
				},
				bson.M{
					"a": 2.0,
				},
				bson.M{
					"a": 3.0,
				},
			},
		})

		// overload negative
		fn(bson.M{
			"foo": bson.M{
				"$slice": -5,
			},
		}, bson.M{
			"_id": id,
			"foo": bson.A{
				bson.M{
					"a": 1.0,
				},
				bson.M{
					"a": 2.0,
				},
				bson.M{
					"a": 3.0,
				},
			},
		})
	})

	// $slice alone preserves all other fields (regression: previously
	// dropped every field except _id and the sliced one)
	projectTest(t, bson.M{
		"_id": id,
		"foo": bson.A{int32(1), int32(2), int32(3), int32(4)},
		"bar": "baz",
		"qux": int32(7),
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": bson.M{"$slice": 2},
		}, bson.M{
			"_id": id,
			"foo": bson.A{int32(1), int32(2)},
			"bar": "baz",
			"qux": int32(7),
		})
	})
}

func TestProjectSliceArray(t *testing.T) {
	id := bson.NewObjectID()

	projectTest(t, bson.M{
		"_id": id,
		"foo": bson.A{int32(1), int32(2), int32(3), int32(4), int32(5)},
		"bar": "keep",
	}, func(fn func(bson.M, interface{})) {
		// [skip:1, limit:2]
		fn(bson.M{
			"foo": bson.M{"$slice": bson.A{1, 2}},
		}, bson.M{
			"_id": id,
			"foo": bson.A{int32(2), int32(3)},
			"bar": "keep",
		})

		// [skip:0, limit:3]
		fn(bson.M{
			"foo": bson.M{"$slice": bson.A{0, 3}},
		}, bson.M{
			"_id": id,
			"foo": bson.A{int32(1), int32(2), int32(3)},
			"bar": "keep",
		})

		// [skip:-2, limit:5] - negative skip with overflowing limit
		fn(bson.M{
			"foo": bson.M{"$slice": bson.A{-2, 5}},
		}, bson.M{
			"_id": id,
			"foo": bson.A{int32(4), int32(5)},
			"bar": "keep",
		})

		// [skip:-10, limit:2] - skip past start, clamps to start
		fn(bson.M{
			"foo": bson.M{"$slice": bson.A{-10, 2}},
		}, bson.M{
			"_id": id,
			"foo": bson.A{int32(1), int32(2)},
			"bar": "keep",
		})

		// [skip:99, limit:2] - skip past end, returns empty
		fn(bson.M{
			"foo": bson.M{"$slice": bson.A{99, 2}},
		}, bson.M{
			"_id": id,
			"foo": bson.A{},
			"bar": "keep",
		})
	})
}

func TestProjectElemMatch(t *testing.T) {
	id := bson.NewObjectID()

	// match found: returns single-element array, only _id and field
	projectTest(t, bson.M{
		"_id": id,
		"foo": bson.A{
			bson.M{"a": int32(1)},
			bson.M{"a": int32(2)},
			bson.M{"a": int32(3)},
		},
		"bar": "ignored",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": bson.M{"$elemMatch": bson.M{"a": int32(2)}},
		}, bson.M{
			"_id": id,
			"foo": bson.A{bson.M{"a": int32(2)}},
		})
	})

	// no match: field omitted, _id still returned
	projectTest(t, bson.M{
		"_id": id,
		"foo": bson.A{
			bson.M{"a": int32(1)},
			bson.M{"a": int32(2)},
		},
		"bar": "ignored",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": bson.M{"$elemMatch": bson.M{"a": int32(99)}},
		}, bson.M{
			"_id": id,
		})
	})

	// $elemMatch combined with explicit field inclusion
	projectTest(t, bson.M{
		"_id": id,
		"foo": bson.A{
			bson.M{"a": int32(1)},
			bson.M{"a": int32(2)},
		},
		"bar": "kept",
		"qux": "dropped",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": bson.M{"$elemMatch": bson.M{"a": int32(1)}},
			"bar": int32(1),
		}, bson.M{
			"_id": id,
			"foo": bson.A{bson.M{"a": int32(1)}},
			"bar": "kept",
		})
	})

	// match using comparison operator
	projectTest(t, bson.M{
		"_id": id,
		"foo": bson.A{
			bson.M{"a": int32(1)},
			bson.M{"a": int32(5)},
			bson.M{"a": int32(10)},
		},
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": bson.M{"$elemMatch": bson.M{"a": bson.M{"$gte": int32(5)}}},
		}, bson.M{
			"_id": id,
			"foo": bson.A{bson.M{"a": int32(5)}},
		})
	})

	// non-array field: omitted from result
	projectTest(t, bson.M{
		"_id": id,
		"foo": "not an array",
		"bar": "ignored",
	}, func(fn func(bson.M, interface{})) {
		fn(bson.M{
			"foo": bson.M{"$elemMatch": bson.M{"a": int32(1)}},
		}, bson.M{
			"_id": id,
		})
	})
}
