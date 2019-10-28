package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
)

func applyTest(t *testing.T, upsert bool, doc bson.M, fn func(fn func(bson.M, interface{}))) {
	t.Run("Mongo", func(t *testing.T) {
		coll := testCollection()

		fn(func(update bson.M, result interface{}) {
			var query bson.M
			if !upsert {
				res, err := coll.InsertOne(nil, doc)
				assert.NoError(t, err)
				query = bson.M{
					"_id": res.InsertedID,
				}
			}

			n, err := coll.UpdateOne(nil, query, update, options.Update().SetUpsert(upsert))
			if _, ok := result.(string); ok {
				assert.Error(t, err, update)
				assert.Nil(t, n, update)
				return
			}

			var d bson.D
			err = coll.FindOne(nil, query).Decode(&d)
			assert.NoError(t, err)
			bsonkit.Unset(&d, "_id")

			if cb, ok := result.(func(*testing.T, bson.D)); ok {
				cb(t, d)
				return
			}

			assert.Equal(t, result, &d, update)
		})
	})

	t.Run("Lungo", func(t *testing.T) {
		fn(func(query bson.M, result interface{}) {
			d := bsonkit.Convert(doc)
			err := Apply(d, bsonkit.Convert(query), upsert)
			if str, ok := result.(string); ok {
				assert.Error(t, err)
				assert.Equal(t, str, err.Error())
				return
			}

			assert.NoError(t, err)

			if cb, ok := result.(func(*testing.T, bson.D)); ok {
				cb(t, *d)
				return
			}

			assert.Equal(t, result, d, query)
		})
	})
}

func TestApply(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, interface{})) {
		// empty update
		fn(bson.M{}, bsonkit.Convert(bson.M{
			"foo": "bar",
		}))

		// not an operator
		fn(bson.M{
			"foo": "bar",
		}, `unknown top level operator "foo"`)

		// unknown operator
		fn(bson.M{
			"$foo": "bar",
		}, `unknown top level operator "$foo"`)

		// missing document
		fn(bson.M{
			"$set": "bar",
		}, "$set: expected document")

		// empty document
		fn(bson.M{
			"$set": bson.M{},
		}, bsonkit.Convert(bson.M{
			"foo": "bar",
		}))

		// valid update
		fn(bson.M{
			"$set": bson.M{
				"foo": "baz",
			},
		}, bsonkit.Convert(bson.M{
			"foo": "baz",
		}))
	})
}

func TestApplySet(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, interface{})) {
		// replace value
		fn(bson.M{
			"$set": bson.M{
				"foo": "baz",
			},
		}, bsonkit.Convert(bson.M{
			"foo": "baz",
		}))

		// add value
		fn(bson.M{
			"$set": bson.M{
				"quz": bson.M{
					"qux": int32(42),
				},
			},
		}, bsonkit.Convert(bson.M{
			"foo": "bar",
			"quz": bson.M{
				"qux": int32(42),
			},
		}))
	})
}

func TestApplySetOnInsert(t *testing.T) {
	// update
	applyTest(t, false, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, interface{})) {
		// replace value
		fn(bson.M{
			"$setOnInsert": bson.M{
				"foo": "baz",
			},
		}, bsonkit.Convert(bson.M{
			"foo": "bar",
		}))

		// add value
		fn(bson.M{
			"$setOnInsert": bson.M{
				"quz": bson.M{
					"qux": int32(42),
				},
			},
		}, bsonkit.Convert(bson.M{
			"foo": "bar",
		}))
	})

	// upsert
	applyTest(t, true, nil, func(fn func(bson.M, interface{})) {
		// replace value
		fn(bson.M{
			"$setOnInsert": bson.M{
				"foo": "baz",
			},
		}, bsonkit.Convert(bson.M{
			"foo": "baz",
		}))
	})

	// upsert
	applyTest(t, true, nil, func(fn func(bson.M, interface{})) {
		// add value
		fn(bson.M{
			"$setOnInsert": bson.M{
				"quz": bson.M{
					"qux": int32(42),
				},
			},
		}, bsonkit.Convert(bson.M{
			"quz": bson.M{
				"qux": int32(42),
			},
		}))
	})
}

func TestApplyUnset(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": "baz",
		},
	}, func(fn func(bson.M, interface{})) {
		// unset nested value
		fn(bson.M{
			"$unset": bson.M{
				"foo.bar": nil,
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{},
		}))

		// unset value
		fn(bson.M{
			"$unset": bson.M{
				"foo": nil,
			},
		}, bsonkit.Convert(bson.M{}))
	})
}

func TestApplyRename(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": "baz",
		},
	}, func(fn func(bson.M, interface{})) {
		// rename nested value
		fn(bson.M{
			"$rename": bson.M{
				"foo.bar": "foo.baz",
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"baz": "baz",
			},
		}))

		// rename value
		fn(bson.M{
			"$rename": bson.M{
				"foo": "bar",
			},
		}, bsonkit.Convert(bson.M{
			"bar": bson.M{
				"bar": "baz",
			},
		}))

		// rename missing
		fn(bson.M{
			"$rename": bson.M{
				"baz": "quz",
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": "baz",
			},
		}))
	})
}

func TestApplyInc(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": int32(42),
		},
	}, func(fn func(bson.M, interface{})) {
		// increment nested value
		fn(bson.M{
			"$inc": bson.M{
				"foo.bar": int64(2),
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": int64(44),
			},
		}))

		// increment missing value
		fn(bson.M{
			"$inc": bson.M{
				"foo.baz": int32(2),
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": int32(42),
				"baz": int32(2),
			},
		}))

		// increment with different type
		fn(bson.M{
			"$inc": bson.M{
				"foo.bar": 1.5,
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": 43.5,
			},
		}))
	})
}

func TestApplyMul(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": int32(42),
		},
	}, func(fn func(bson.M, interface{})) {
		// multiply nested value
		fn(bson.M{
			"$mul": bson.M{
				"foo.bar": int64(2),
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": int64(84),
			},
		}))

		// multiply missing value
		fn(bson.M{
			"$mul": bson.M{
				"foo.baz": int32(2),
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": int32(42),
				"baz": int32(0),
			},
		}))

		// multiply with different type
		fn(bson.M{
			"$mul": bson.M{
				"foo.bar": 1.5,
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": 63.0,
			},
		}))
	})
}

func TestApplyMax(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": int64(42),
		},
	}, func(fn func(bson.M, interface{})) {
		// keep value
		fn(bson.M{
			"$max": bson.M{
				"foo.bar": int32(2),
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": int64(42),
			},
		}))

		// set value
		fn(bson.M{
			"$max": bson.M{
				"foo.bar": int32(44),
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": int32(44),
			},
		}))

		// add value
		fn(bson.M{
			"$max": bson.M{
				"foo.baz": int32(2),
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": int64(42),
				"baz": int32(2),
			},
		}))
	})
}

func TestApplyMin(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": int64(42),
		},
	}, func(fn func(bson.M, interface{})) {
		// keep value
		fn(bson.M{
			"$min": bson.M{
				"foo.bar": int32(44),
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": int64(42),
			},
		}))

		// set value
		fn(bson.M{
			"$min": bson.M{
				"foo.bar": int32(21),
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": int32(21),
			},
		}))

		// add value
		fn(bson.M{
			"$min": bson.M{
				"foo.baz": int32(2),
			},
		}, bsonkit.Convert(bson.M{
			"foo": bson.M{
				"bar": int64(42),
				"baz": int32(2),
			},
		}))
	})
}

func TestApplyCurrentDate(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, interface{})) {
		// missing document
		fn(bson.M{
			"$currentDate": bson.M{
				"foo": "baz",
			},
		}, "$currentDate: expected boolean or document")

		// invalid document
		fn(bson.M{
			"$currentDate": bson.M{
				"foo": bson.M{
					"foo": "bar",
				},
			},
		}, "$currentDate: expected document with a single $type field")

		// invalid type
		fn(bson.M{
			"$currentDate": bson.M{
				"foo": bson.M{
					"$type": "foo",
				},
			},
		}, "$currentDate: expected $type 'date' or 'timestamp'")

		// set date
		fn(bson.M{
			"$currentDate": bson.M{
				"foo": true,
			},
		}, func(t *testing.T, d bson.D) {
			assert.Len(t, d, 1)
			assert.Equal(t, "foo", d[0].Key)
			assert.IsType(t, primitive.DateTime(0), d[0].Value)
		})

		// set date using type
		fn(bson.M{
			"$currentDate": bson.M{
				"foo": bson.M{
					"$type": "date",
				},
			},
		}, func(t *testing.T, d bson.D) {
			assert.Len(t, d, 1)
			assert.Equal(t, "foo", d[0].Key)
			assert.IsType(t, primitive.DateTime(0), d[0].Value)
		})

		// set timestamp using type
		fn(bson.M{
			"$currentDate": bson.M{
				"foo": bson.M{
					"$type": "timestamp",
				},
			},
		}, func(t *testing.T, d bson.D) {
			assert.Len(t, d, 1)
			assert.Equal(t, "foo", d[0].Key)
			assert.IsType(t, primitive.Timestamp{}, d[0].Value)
		})
	})
}
