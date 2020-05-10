package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
)

func applyTest(t *testing.T, upsert bool, doc bson.M, fn func(fn func(bson.M, []bson.M, interface{}))) {
	t.Run("Mongo", func(t *testing.T) {
		coll := testCollection()

		fn(func(update bson.M, arrayFilters []bson.M, result interface{}) {
			var query bson.M
			if !upsert {
				res, err := coll.InsertOne(nil, doc)
				assert.NoError(t, err)
				query = bson.M{
					"_id": res.InsertedID,
				}
			}

			opts := options.Update().SetUpsert(true)

			if arrayFilters != nil {
				list := make([]interface{}, 0, len(arrayFilters))
				for _, af := range arrayFilters {
					list = append(list, af)
				}

				opts.SetArrayFilters(options.ArrayFilters{Filters: list})
			}

			n, err := coll.UpdateOne(nil, query, update, opts)
			if _, ok := result.(string); ok {
				assert.Error(t, err, update)
				assert.Nil(t, n, update)
				return
			}

			var m bson.M
			err = coll.FindOne(nil, query).Decode(&m)
			assert.NoError(t, err)

			d := bsonkit.MustConvert(m)
			bsonkit.Unset(d, "_id")

			if cb, ok := result.(func(*testing.T, bson.D)); ok {
				cb(t, *d)
				return
			}

			assert.Equal(t, result, d, update)
		})
	})

	t.Run("Lungo", func(t *testing.T) {
		fn(func(update bson.M, arrayFilters []bson.M, result interface{}) {
			d := bsonkit.MustConvert(doc)
			l := bsonkit.MustConvertList(arrayFilters)
			_, err := Apply(d, nil, bsonkit.MustConvert(update), upsert, l)
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

			assert.Equal(t, result, d, update)
		})
	})
}

func TestApply(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, []bson.M, interface{})) {
		// empty update
		fn(bson.M{}, nil, bsonkit.MustConvert(bson.M{
			"foo": "bar",
		}))

		// not an operator
		fn(bson.M{
			"foo": "bar",
		}, nil, `missing default operator`)

		// unknown operator
		fn(bson.M{
			"$foo": "bar",
		}, nil, `unknown top level operator "$foo"`)

		// missing document
		fn(bson.M{
			"$set": "bar",
		}, nil, "$set: expected document")

		// empty document
		fn(bson.M{
			"$set": bson.M{},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": "bar",
		}))

		// valid update
		fn(bson.M{
			"$set": bson.M{
				"foo": "baz",
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": "baz",
		}))
	})
}

func TestApplyPositionalOperators(t *testing.T) {
	// single positional operator
	applyTest(t, false, bson.M{
		"foo": bson.A{
			"bar",
			"baz",
			"foo",
		},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$set": bson.M{
				"foo.$[foo]": "baz",
			},
		}, []bson.M{
			{"foo": "foo"},
		}, bsonkit.MustConvert(bson.M{
			"foo": bson.A{
				"bar",
				"baz",
				"baz",
			},
		}))
	})

	// multiple adjacent positional operators
	applyTest(t, false, bson.M{
		"foo": bson.A{
			bson.A{"x", "y", "z"},
			bson.A{"v", "w"},
			bson.A{"i", "j", "k"},
		},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$set": bson.M{
				"foo.$[].$[]": "baz",
			},
		}, []bson.M{}, bsonkit.MustConvert(bson.M{
			"foo": bson.A{
				bson.A{"baz", "baz", "baz"},
				bson.A{"baz", "baz"},
				bson.A{"baz", "baz", "baz"},
			},
		}))
	})

	// multiple nested positional operators
	applyTest(t, false, bson.M{
		"foo": bson.A{
			bson.M{
				"val":  int32(10),
				"ints": bson.A{int32(-1), int32(2), int32(-3), int32(4)},
			},
			bson.M{
				"val":  int32(20),
				"ints": bson.A{int32(10), int32(-20), int32(30), int32(-40)},
			},
			bson.M{
				"val":  int32(30),
				"ints": bson.A{int32(-100), int32(200), int32(-300), int32(400)},
			},
		},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$set": bson.M{
				"foo.$[gt15].ints.$[neg]": int32(0),
			},
		}, []bson.M{
			{"gt15.val": bson.M{
				"$gt": 15,
			}},
			{"neg": bson.M{
				"$lt": 0,
			}},
		}, bsonkit.MustConvert(bson.M{
			"foo": bson.A{
				bson.M{
					"val":  int32(10),
					"ints": bson.A{int32(-1), int32(2), int32(-3), int32(4)},
				},
				bson.M{
					"val":  int32(20),
					"ints": bson.A{int32(10), int32(0), int32(30), int32(0)},
				},
				bson.M{
					"val":  int32(30),
					"ints": bson.A{int32(0), int32(200), int32(0), int32(400)},
				},
			},
		}))
	})
}

func TestApplySet(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, []bson.M, interface{})) {
		// replace value
		fn(bson.M{
			"$set": bson.M{
				"foo": "baz",
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": "baz",
		}))

		// add value
		fn(bson.M{
			"$set": bson.M{
				"quz": bson.M{
					"qux": int32(42),
				},
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": "bar",
			"quz": bson.M{
				"qux": int32(42),
			},
		}))
	})

	// changes
	changes, err := Apply(bsonkit.MustConvert(bson.M{
		"foo": "bar",
	}), nil, bsonkit.MustConvert(bson.M{
		"$set": bson.M{
			"foo": "baz",
		},
	}), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, &Changes{
		Updated: map[string]interface{}{
			"foo": "baz",
		},
		Removed: []string{},
	}, changes)
}

func TestApplySetOnInsert(t *testing.T) {
	// update
	applyTest(t, false, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, []bson.M, interface{})) {
		// replace value
		fn(bson.M{
			"$setOnInsert": bson.M{
				"foo": "baz",
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": "bar",
		}))

		// add value
		fn(bson.M{
			"$setOnInsert": bson.M{
				"quz": bson.M{
					"qux": int32(42),
				},
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": "bar",
		}))
	})

	// upsert
	applyTest(t, true, nil, func(fn func(bson.M, []bson.M, interface{})) {
		// replace value
		fn(bson.M{
			"$setOnInsert": bson.M{
				"foo": "baz",
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": "baz",
		}))
	})

	// upsert nested
	applyTest(t, true, nil, func(fn func(bson.M, []bson.M, interface{})) {
		// add value
		fn(bson.M{
			"$setOnInsert": bson.M{
				"quz": bson.M{
					"qux": int32(42),
				},
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"quz": bson.M{
				"qux": int32(42),
			},
		}))
	})

	// changes
	changes, err := Apply(bsonkit.MustConvert(bson.M{
		"foo": "bar",
	}), nil, bsonkit.MustConvert(bson.M{
		"$setOnInsert": bson.M{
			"foo": "baz",
		},
	}), true, nil)
	assert.NoError(t, err)
	assert.Equal(t, &Changes{
		Upsert: true,
		Updated: map[string]interface{}{
			"foo": "baz",
		},
		Removed: []string{},
	}, changes)
}

func TestApplyUnset(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": "baz",
		},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		// unset nested value
		fn(bson.M{
			"$unset": bson.M{
				"foo.bar": nil,
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{},
		}))

		// unset value
		fn(bson.M{
			"$unset": bson.M{
				"foo": nil,
			},
		}, nil, bsonkit.MustConvert(bson.M{}))
	})

	// changes
	changes, err := Apply(bsonkit.MustConvert(bson.M{
		"foo": bson.M{
			"bar": "baz",
		},
	}), nil, bsonkit.MustConvert(bson.M{
		"$unset": bson.M{
			"foo.bar": nil,
		},
	}), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, &Changes{
		Updated: map[string]interface{}{},
		Removed: []string{
			"foo.bar",
		},
	}, changes)
}

func TestApplyRename(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": "baz",
		},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		// rename nested value
		fn(bson.M{
			"$rename": bson.M{
				"foo.bar": "foo.baz",
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"baz": "baz",
			},
		}))

		// rename value
		fn(bson.M{
			"$rename": bson.M{
				"foo": "bar",
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"bar": bson.M{
				"bar": "baz",
			},
		}))

		// rename missing
		fn(bson.M{
			"$rename": bson.M{
				"baz": "quz",
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"bar": "baz",
			},
		}))
	})

	// ignored array values
	applyTest(t, false, bson.M{
		"foo": bson.A{
			bson.M{"bar": "baz"},
		},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$rename": bson.M{
				"foo.0.bar": "foo.0.baz",
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.A{
				bson.M{"bar": "baz"},
			},
		}))
	})

	// changes
	changes, err := Apply(bsonkit.MustConvert(bson.M{
		"foo": bson.M{
			"bar": "baz",
		},
	}), nil, bsonkit.MustConvert(bson.M{
		"$rename": bson.M{
			"foo.bar": "foo.baz",
		},
	}), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, &Changes{
		Updated: map[string]interface{}{
			"foo.baz": "baz",
		},
		Removed: []string{
			"foo.bar",
		},
	}, changes)
}

func TestApplyInc(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": int32(42),
		},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		// increment nested value
		fn(bson.M{
			"$inc": bson.M{
				"foo.bar": int64(2),
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"bar": 44,
			},
		}))

		// increment missing value
		fn(bson.M{
			"$inc": bson.M{
				"foo.baz": int32(2),
			},
		}, nil, bsonkit.MustConvert(bson.M{
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
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"bar": 43.5,
			},
		}))
	})

	// changes
	changes, err := Apply(bsonkit.MustConvert(bson.M{
		"foo": bson.M{
			"bar": 42,
		},
	}), nil, bsonkit.MustConvert(bson.M{
		"$inc": bson.M{
			"foo.bar": 2,
		},
	}), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, &Changes{
		Updated: map[string]interface{}{
			"foo.bar": int64(44),
		},
		Removed: []string{},
	}, changes)
}

func TestApplyMul(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": int32(42),
		},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		// multiply nested value
		fn(bson.M{
			"$mul": bson.M{
				"foo.bar": int64(2),
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"bar": 84,
			},
		}))

		// multiply missing value
		fn(bson.M{
			"$mul": bson.M{
				"foo.baz": int32(2),
			},
		}, nil, bsonkit.MustConvert(bson.M{
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
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"bar": 63.0,
			},
		}))
	})

	// changes
	changes, err := Apply(bsonkit.MustConvert(bson.M{
		"foo": bson.M{
			"bar": int32(42),
		},
	}), nil, bsonkit.MustConvert(bson.M{
		"$mul": bson.M{
			"foo.bar": 2,
		},
	}), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, &Changes{
		Updated: map[string]interface{}{
			"foo.bar": int64(84),
		},
		Removed: []string{},
	}, changes)
}

func TestApplyMax(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": int64(42),
		},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		// keep value
		fn(bson.M{
			"$max": bson.M{
				"foo.bar": int32(2),
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"bar": 42,
			},
		}))

		// set value
		fn(bson.M{
			"$max": bson.M{
				"foo.bar": int32(44),
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"bar": int32(44),
			},
		}))

		// add value
		fn(bson.M{
			"$max": bson.M{
				"foo.baz": int32(2),
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"bar": 42,
				"baz": int32(2),
			},
		}))
	})

	// changes
	changes, err := Apply(bsonkit.MustConvert(bson.M{
		"foo": bson.M{
			"bar": int32(42),
		},
	}), nil, bsonkit.MustConvert(bson.M{
		"$max": bson.M{
			"foo.bar": int32(44),
		},
	}), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, &Changes{
		Updated: map[string]interface{}{
			"foo.bar": int32(44),
		},
		Removed: []string{},
	}, changes)
}

func TestApplyMin(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": bson.M{
			"bar": int64(42),
		},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		// keep value
		fn(bson.M{
			"$min": bson.M{
				"foo.bar": int32(44),
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"bar": 42,
			},
		}))

		// set value
		fn(bson.M{
			"$min": bson.M{
				"foo.bar": int32(21),
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"bar": int32(21),
			},
		}))

		// add value
		fn(bson.M{
			"$min": bson.M{
				"foo.baz": int32(2),
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.M{
				"bar": 42,
				"baz": int32(2),
			},
		}))
	})

	// changes
	changes, err := Apply(bsonkit.MustConvert(bson.M{
		"foo": bson.M{
			"bar": int32(42),
		},
	}), nil, bsonkit.MustConvert(bson.M{
		"$min": bson.M{
			"foo.bar": int32(21),
		},
	}), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, &Changes{
		Updated: map[string]interface{}{
			"foo.bar": int32(21),
		},
		Removed: []string{},
	}, changes)
}

func TestApplyCurrentDate(t *testing.T) {
	applyTest(t, false, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, []bson.M, interface{})) {
		// missing document
		fn(bson.M{
			"$currentDate": bson.M{
				"foo": "baz",
			},
		}, nil, "$currentDate: expected boolean or document")

		// invalid document
		fn(bson.M{
			"$currentDate": bson.M{
				"foo": bson.M{
					"foo": "bar",
				},
			},
		}, nil, "$currentDate: expected document with a single $type field")

		// invalid type
		fn(bson.M{
			"$currentDate": bson.M{
				"foo": bson.M{
					"$type": "foo",
				},
			},
		}, nil, "$currentDate: expected $type 'date' or 'timestamp'")

		// set date
		fn(bson.M{
			"$currentDate": bson.M{
				"foo": true,
			},
		}, nil, func(t *testing.T, d bson.D) {
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
		}, nil, func(t *testing.T, d bson.D) {
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
		}, nil, func(t *testing.T, d bson.D) {
			assert.Len(t, d, 1)
			assert.Equal(t, "foo", d[0].Key)
			assert.IsType(t, primitive.Timestamp{}, d[0].Value)
		})
	})

	// changes
	changes, err := Apply(bsonkit.MustConvert(bson.M{
		"foo": "bar",
	}), nil, bsonkit.MustConvert(bson.M{
		"$currentDate": bson.M{
			"foo": true,
		},
	}), false, nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, changes.Updated["foo"])
	assert.Equal(t, &Changes{
		Updated: map[string]interface{}{
			"foo": changes.Updated["foo"],
		},
		Removed: []string{},
	}, changes)
}

func TestApplyPush(t *testing.T) {
	// create array
	applyTest(t, false, bson.M{}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$push": bson.M{
				"foo": "bar",
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.A{"bar"},
		}))
	})

	// add element
	applyTest(t, false, bson.M{
		"foo": bson.A{"bar"},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$push": bson.M{
				"foo": "baz",
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.A{"bar", "baz"},
		}))
	})

	// non-array
	applyTest(t, false, bson.M{
		"str": "bar",
		"int": int32(42),
		"nil": nil,
		"obj": bson.D{},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$push": bson.M{
				"str": "baz",
				"int": "baz",
				"nil": "baz",
				"obj": "baz",
			},
		}, nil, `value at path "int" is not an array`)
	})

	// changes
	changes, err := Apply(bsonkit.MustConvert(bson.M{
		"foo": bson.A{"bar"},
	}), nil, bsonkit.MustConvert(bson.M{
		"$push": bson.M{
			"foo": "baz",
		},
	}), true, nil)
	assert.NoError(t, err)
	assert.Equal(t, &Changes{
		Upsert: true,
		Updated: map[string]interface{}{
			"foo.1": "baz",
		},
		Removed: []string{},
	}, changes)
}

func TestApplyPop(t *testing.T) {
	// unsupported value
	applyTest(t, false, bson.M{
		"foo": bson.A{"bar", "baz"},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$pop": bson.M{
				"foo": 0,
			},
		}, nil, "$pop: expected 1 or -1")
	})

	// missing value
	applyTest(t, false, bson.M{
		"foo": bson.A{"bar", "baz"},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$pop": bson.M{
				"bar": -1,
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.A{"bar", "baz"},
		}))
	})

	// remove first element
	applyTest(t, false, bson.M{
		"foo": bson.A{"bar", "baz"},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$pop": bson.M{
				"foo": -1,
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.A{"baz"},
		}))
	})

	// remove last value
	applyTest(t, false, bson.M{
		"foo": bson.A{"bar", "baz"},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$pop": bson.M{
				"foo": 1,
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.A{"bar"},
		}))
	})

	// empty array
	applyTest(t, false, bson.M{
		"foo": bson.A{},
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$pop": bson.M{
				"foo": 1,
			},
		}, nil, bsonkit.MustConvert(bson.M{
			"foo": bson.A{},
		}))
	})

	// non-array
	applyTest(t, false, bson.M{
		"foo": "bar",
	}, func(fn func(bson.M, []bson.M, interface{})) {
		fn(bson.M{
			"$pop": bson.M{
				"foo": 1,
			},
		}, nil, `value at path "foo" is not an array`)
	})

	// changes
	changes, err := Apply(bsonkit.MustConvert(bson.M{
		"foo": bson.A{"bar", "baz"},
	}), nil, bsonkit.MustConvert(bson.M{
		"$pop": bson.M{
			"foo": 1,
		},
	}), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, &Changes{
		Upsert: false,
		Updated: map[string]interface{}{
			"foo": bson.A{"bar"},
		},
		Removed: []string{},
	}, changes)
}
