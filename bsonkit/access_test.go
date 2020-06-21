package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestGet(t *testing.T) {
	doc := MustConvert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 42,
			},
		},
	})

	// basic field
	res := Get(doc, "foo")
	assert.Equal(t, *MustConvert(bson.M{
		"bar": bson.M{
			"baz": 42,
		},
	}), res)

	// missing field
	res = Get(doc, "bar")
	assert.Equal(t, Missing, res)

	// nested field
	res = Get(doc, "foo.bar")
	assert.Equal(t, *MustConvert(bson.M{
		"baz": 42,
	}), res)

	// missing nested field
	res = Get(doc, "bar.foo")
	assert.Equal(t, Missing, res)

	// final nested field
	res = Get(doc, "foo.bar.baz")
	assert.Equal(t, int64(42), res)

	// empty path
	res = Get(doc, "")
	assert.Equal(t, Missing, res)

	// empty sub path
	res = Get(doc, "foo.")
	assert.Equal(t, Missing, res)
}

func TestGetArray(t *testing.T) {
	doc := MustConvert(bson.M{
		"foo": bson.A{
			"bar",
			bson.M{
				"baz": 42,
			},
		},
	})

	// negative index
	res := Get(doc, "foo.-1")
	assert.Equal(t, Missing, res)

	// first element
	res = Get(doc, "foo.0")
	assert.Equal(t, "bar", res)

	// second element
	res = Get(doc, "foo.1")
	assert.Equal(t, *MustConvert(bson.M{
		"baz": 42,
	}), res)

	// missing index
	res = Get(doc, "foo.5")
	assert.Equal(t, Missing, res)

	// nested field
	res = Get(doc, "foo.1.baz")
	assert.Equal(t, int64(42), res)
}

func TestAll(t *testing.T) {
	doc := MustConvert(bson.M{
		"foo": bson.A{
			"bar",
			bson.M{
				"baz": 7,
				"quz": bson.A{
					bson.M{
						"foo": "bar",
					},
					bson.M{
						"qux": 13,
					},
				},
			},
			bson.M{
				"baz": 42,
				"quz": bson.A{
					bson.M{
						"qux": 13,
					},
					bson.M{
						"qux": 26,
					},
				},
			},
		},
		"bar": "baz",
	})

	// missing field
	res, multi := All(doc, "foo.bar", false, false)
	assert.True(t, multi)
	assert.Equal(t, bson.A{Missing, Missing, Missing}, res)

	// missing field (compact)
	res, multi = All(doc, "foo.bar", true, false)
	assert.True(t, multi)
	assert.Equal(t, bson.A{}, res)

	// simple field
	res, multi = All(doc, "bar", false, false)
	assert.False(t, multi)
	assert.Equal(t, "baz", res)

	// nested field
	res, multi = All(doc, "foo.baz", false, false)
	assert.True(t, multi)
	assert.Equal(t, bson.A{Missing, int64(7), int64(42)}, res)

	// nested field (compact)
	res, multi = All(doc, "foo.baz", true, false)
	assert.True(t, multi)
	assert.Equal(t, bson.A{int64(7), int64(42)}, res)

	// multi level
	res, multi = All(doc, "foo.quz.qux", false, false)
	assert.True(t, multi)
	assert.Equal(t, bson.A{Missing, bson.A{Missing, int64(13)}, bson.A{int64(13), int64(26)}}, res)

	// multi level (compact)
	res, multi = All(doc, "foo.quz.qux", true, false)
	assert.True(t, multi)
	assert.Equal(t, bson.A{int64(13), int64(13), int64(26)}, res)
}

func TestAllMerge(t *testing.T) {
	doc := MustConvert(bson.M{
		"foo": bson.A{
			bson.M{
				"quz": bson.A{
					3,
					4,
				},
			},
			bson.M{
				"quz": bson.A{
					bson.A{1, 2},
				},
			},
			bson.M{
				"quz": 5,
			},
		},
		"bar": bson.A{
			bson.A{1, 2},
			bson.A{3, 4},
			5,
		},
	})

	// array field
	res, multi := All(doc, "bar", true, true)
	assert.False(t, multi)
	assert.Equal(t, bson.A{bson.A{int64(1), int64(2)}, bson.A{int64(3), int64(4)}, int64(5)}, res)

	// nested array field
	res, multi = All(doc, "foo.quz", true, true)
	assert.True(t, multi)
	assert.Equal(t, bson.A{int64(3), int64(4), bson.A{int64(1), int64(2)}, int64(5)}, res)
}

func TestPut(t *testing.T) {
	doc := MustConvert(bson.M{
		"foo": "bar",
	})

	// replace final value
	res, err := Put(doc, "foo", "baz", false)
	assert.NoError(t, err)
	assert.Equal(t, "bar", res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": "baz",
	}), doc)

	// append field
	res, err = Put(doc, "bar", "baz", false)
	assert.NoError(t, err)
	assert.Equal(t, Missing, res)
	assert.Equal(t, &bson.D{
		bson.E{Key: "foo", Value: "baz"},
		bson.E{Key: "bar", Value: "baz"},
	}, doc)

	// prepend field
	res, err = Put(doc, "baz", "quz", true)
	assert.NoError(t, err)
	assert.Equal(t, Missing, res)
	assert.Equal(t, &bson.D{
		bson.E{Key: "baz", Value: "quz"},
		bson.E{Key: "foo", Value: "baz"},
		bson.E{Key: "bar", Value: "baz"},
	}, doc)

	doc = MustConvert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 42,
			},
		},
	})

	// replace nested final value
	res, err = Put(doc, "foo.bar.baz", int64(7), false)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 7,
			},
		},
	}), doc)

	// append nested field
	res, err = Put(doc, "foo.bar.quz", int64(42), false)
	assert.NoError(t, err)
	assert.Equal(t, Missing, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.M{
			"bar": bson.D{
				bson.E{Key: "baz", Value: 7},
				bson.E{Key: "quz", Value: 42},
			},
		},
	}), doc)

	// prepend nested field
	res, err = Put(doc, "foo.bar.qux", int64(42), true)
	assert.NoError(t, err)
	assert.Equal(t, Missing, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.M{
			"bar": bson.D{
				bson.E{Key: "qux", Value: 42},
				bson.E{Key: "baz", Value: 7},
				bson.E{Key: "quz", Value: 42},
			},
		},
	}), doc)

	// replace tree
	res, err = Put(doc, "foo.bar", int64(42), false)
	assert.NoError(t, err)
	assert.Equal(t, bson.D{
		bson.E{Key: "qux", Value: int64(42)},
		bson.E{Key: "baz", Value: int64(7)},
		bson.E{Key: "quz", Value: int64(42)},
	}, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.M{
			"bar": 42,
		},
	}), doc)

	// invalid type error
	res, err = Put(doc, "foo.bar.baz", 42, false)
	assert.Error(t, err)
	assert.Equal(t, nil, res)
	assert.Equal(t, "cannot put value at foo.bar.baz", err.Error())
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.M{
			"bar": 42,
		},
	}), doc)

	// intermediary document creation
	doc = &bson.D{}
	res, err = Put(doc, "baz.bar.foo", int64(42), false)
	assert.NoError(t, err)
	assert.Equal(t, Missing, res)
	assert.Equal(t, MustConvert(bson.M{
		"baz": bson.M{
			"bar": bson.M{
				"foo": 42,
			},
		},
	}), doc)
}

func TestPutArray(t *testing.T) {
	doc := MustConvert(bson.M{
		"foo": bson.A{
			"bar",
		},
	})

	// negative index
	res, err := Put(doc, "foo.-1", 7, false)
	assert.Error(t, err)
	assert.Equal(t, nil, res)
	assert.Equal(t, "cannot put value at foo.-1", err.Error())
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.A{
			"bar",
		},
	}), doc)

	// first element
	res, err = Put(doc, "foo.0", int64(7), false)
	assert.NoError(t, err)
	assert.Equal(t, "bar", res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.A{
			7,
		},
	}), doc)

	// second element
	res, err = Put(doc, "foo.1", *MustConvert(bson.M{
		"baz": 42,
	}), false)
	assert.NoError(t, err)
	assert.Equal(t, Missing, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.A{
			7,
			bson.M{
				"baz": 42,
			},
		},
	}), doc)

	// missing index
	res, err = Put(doc, "foo.5", int64(7), false)
	assert.NoError(t, err)
	assert.Equal(t, Missing, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.A{
			7,
			bson.M{
				"baz": 42,
			},
			nil,
			nil,
			nil,
			7,
		},
	}), doc)

	// nested field
	res, err = Put(doc, "foo.1.baz", int64(7), false)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.A{
			7,
			bson.M{
				"baz": 7,
			},
			nil,
			nil,
			nil,
			7,
		},
	}), doc)
}

func TestUnset(t *testing.T) {
	doc := MustConvert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 42,
			},
		},
	})

	// missing field
	res := Unset(doc, "foo.bar.baz.quz")
	assert.Equal(t, Missing, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 42,
			},
		},
	}), doc)

	// leaf field
	res = Unset(doc, "foo.bar.baz")
	assert.Equal(t, int64(42), res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.M{
			"bar": bson.M{},
		},
	}), doc)

	// missing field
	res = Unset(doc, "foo.bar.baz")
	assert.Equal(t, Missing, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.M{
			"bar": bson.M{},
		},
	}), doc)

	// top level field
	res = Unset(doc, "foo")
	assert.Equal(t, bson.D{
		bson.E{Key: "bar", Value: bson.D{}},
	}, res)
	assert.Equal(t, MustConvert(bson.M{}), doc)
}

func TestUnsetArray(t *testing.T) {
	doc := MustConvert(bson.M{
		"foo": bson.A{
			"bar",
			bson.M{
				"baz": 42,
			},
		},
	})

	// negative index
	res := Unset(doc, "foo.-1")
	assert.Equal(t, Missing, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.A{
			"bar",
			bson.M{
				"baz": 42,
			},
		},
	}), doc)

	// first element
	res = Unset(doc, "foo.0")
	assert.Equal(t, "bar", res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.A{
			nil,
			bson.M{
				"baz": 42,
			},
		},
	}), doc)

	// second element
	res = Unset(doc, "foo.1")
	assert.Equal(t, bson.D{
		bson.E{Key: "baz", Value: int64(42)},
	}, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.A{
			nil,
			nil,
		},
	}), doc)

	// missing index
	res = Unset(doc, "foo.5")
	assert.Equal(t, Missing, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.A{
			nil,
			nil,
		},
	}), doc)
}

func TestIncrement(t *testing.T) {
	doc := MustConvert(bson.M{
		"foo": 42,
		"bar": "42",
	})

	// invalid incrementee
	res, err := Increment(doc, "bar", int64(2))
	assert.Error(t, err)
	assert.Equal(t, nil, res)
	assert.Equal(t, `incrementee or increment is not a number`, err.Error())

	// invalid increment
	res, err = Increment(doc, "foo", "2")
	assert.Error(t, err)
	assert.Equal(t, nil, res)
	assert.Equal(t, "incrementee or increment is not a number", err.Error())

	// increment existing field
	res, err = Increment(doc, "foo", int64(2))
	assert.NoError(t, err)
	assert.Equal(t, int64(44), res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": 44,
		"bar": "42",
	}), doc)

	// increment missing field
	res, err = Increment(doc, "quz", int64(2))
	assert.NoError(t, err)
	assert.Equal(t, int64(2), res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": 44,
		"bar": "42",
		"quz": 2,
	}), doc)
}

func TestMultiply(t *testing.T) {
	doc := MustConvert(bson.M{
		"foo": 42,
		"bar": "42",
	})

	// invalid multiplicand
	res, err := Multiply(doc, "bar", int64(2))
	assert.Error(t, err)
	assert.Equal(t, nil, res)
	assert.Equal(t, `multiplicand or multiplier is not a number`, err.Error())

	// invalid multiplier
	res, err = Multiply(doc, "foo", "2")
	assert.Error(t, err)
	assert.Equal(t, nil, res)
	assert.Equal(t, "multiplicand or multiplier is not a number", err.Error())

	// multiply existing field
	res, err = Multiply(doc, "foo", int64(2))
	assert.NoError(t, err)
	assert.Equal(t, int64(84), res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": 84,
		"bar": "42",
	}), doc)

	// multiply missing field
	res, err = Multiply(doc, "quz", int64(2))
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": 84,
		"bar": "42",
		"quz": 0,
	}), doc)
}

func TestPush(t *testing.T) {
	doc := MustConvert(bson.M{
		"bar": "42",
	})

	// create array
	res, err := Push(doc, "foo", int64(42))
	assert.NoError(t, err)
	assert.Equal(t, bson.A{int64(42)}, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.A{42},
		"bar": "42",
	}), doc)

	// add to array
	res, err = Push(doc, "foo", int64(2))
	assert.NoError(t, err)
	assert.Equal(t, bson.A{int64(42), int64(2)}, res)
	assert.Equal(t, MustConvert(bson.M{
		"foo": bson.A{42, 2},
		"bar": "42",
	}), doc)

	// non-array field
	res, err = Push(doc, "bar", int64(2))
	assert.Error(t, err)
	assert.Equal(t, `value at path "bar" is not an array`, err.Error())
}

func TestPop(t *testing.T) {
	doc := MustConvert(bson.M{
		"bar": "42",
		"foo": bson.A{7, 13, 42},
	})

	// missing array
	res, err := Pop(doc, "baz", false)
	assert.NoError(t, err)
	assert.Equal(t, Missing, res)
	assert.Equal(t, MustConvert(bson.M{
		"bar": "42",
		"foo": bson.A{7, 13, 42},
	}), doc)

	// pop first element
	res, err = Pop(doc, "foo", false)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), res)
	assert.Equal(t, MustConvert(bson.M{
		"bar": "42",
		"foo": bson.A{13, 42},
	}), doc)

	// pop last element
	res, err = Pop(doc, "foo", true)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), res)
	assert.Equal(t, MustConvert(bson.M{
		"bar": "42",
		"foo": bson.A{13},
	}), doc)

	// pop first element
	res, err = Pop(doc, "foo", false)
	assert.NoError(t, err)
	assert.Equal(t, int64(13), res)
	assert.Equal(t, MustConvert(bson.M{
		"bar": "42",
		"foo": bson.A{},
	}), doc)

	// empty array
	res, err = Pop(doc, "foo", false)
	assert.NoError(t, err)
	assert.Equal(t, Missing, res)
	assert.Equal(t, MustConvert(bson.M{
		"bar": "42",
		"foo": bson.A{},
	}), doc)

	// non-array field
	res, err = Pop(doc, "bar", false)
	assert.Error(t, err)
	assert.Equal(t, `value at path "bar" is not an array`, err.Error())
}

func BenchmarkGet(b *testing.B) {
	b.ReportAllocs()

	doc := MustConvert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 42,
			},
		},
	})

	for i := 0; i < b.N; i++ {
		Get(doc, "foo.bar.baz")
	}
}

func BenchmarkAll(b *testing.B) {
	b.ReportAllocs()

	doc := MustConvert(bson.M{
		"foo": bson.A{
			"bar",
			bson.M{
				"baz": 7,
				"quz": bson.A{
					bson.M{
						"foo": "bar",
					},
					bson.M{
						"qux": 13,
					},
				},
			},
			bson.M{
				"baz": 42,
				"quz": bson.A{
					bson.M{
						"qux": 13,
					},
					bson.M{
						"qux": 26,
					},
				},
			},
		},
		"bar": "baz",
	})

	for i := 0; i < b.N; i++ {
		All(doc, "foo.quz.qux", true, false)
	}
}

func BenchmarkPut(b *testing.B) {
	b.ReportAllocs()

	doc := MustConvert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 42,
			},
		},
	})

	for i := 0; i < b.N; i++ {
		_, _ = Put(doc, "foo.bar.baz", 43, false)
	}
}
