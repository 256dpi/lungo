package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestGet(t *testing.T) {
	doc := Convert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 42,
			},
		},
	})

	// basic field

	res := Get(doc, "foo")
	assert.Equal(t, *Convert(bson.M{
		"bar": bson.M{
			"baz": 42,
		},
	}), res)

	// missing field

	res = Get(doc, "bar")
	assert.Equal(t, Missing, res)

	// nested field

	res = Get(doc, "foo.bar")
	assert.Equal(t, *Convert(bson.M{
		"baz": 42,
	}), res)

	// missing nested field

	res = Get(doc, "bar.foo")
	assert.Equal(t, Missing, res)

	// final nested field

	res = Get(doc, "foo.bar.baz")
	assert.Equal(t, 42, res)
}

func TestPut(t *testing.T) {
	doc := Convert(bson.M{
		"foo": "bar",
	})

	// replace final value

	err := Put(doc, "foo", "baz", false)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": "baz",
	}), doc)

	// append field
	err = Put(doc, "bar", "baz", false)
	assert.NoError(t, err)
	assert.Equal(t, &bson.D{
		bson.E{Key: "foo", Value: "baz"},
		bson.E{Key: "bar", Value: "baz"},
	}, doc)

	// prepend field
	err = Put(doc, "baz", "quz", true)
	assert.NoError(t, err)
	assert.Equal(t, &bson.D{
		bson.E{Key: "baz", Value: "quz"},
		bson.E{Key: "foo", Value: "baz"},
		bson.E{Key: "bar", Value: "baz"},
	}, doc)

	doc = Convert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 42,
			},
		},
	})

	// replace nested final value

	err = Put(doc, "foo.bar.baz", 7, false)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 7,
			},
		},
	}), doc)

	// append nested field

	err = Put(doc, "foo.bar.quz", 42, false)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": bson.D{
				bson.E{Key: "baz", Value: 7},
				bson.E{Key: "quz", Value: 42},
			},
		},
	}), doc)

	// prepend nested field

	err = Put(doc, "foo.bar.qux", 42, true)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": bson.D{
				bson.E{Key: "qux", Value: 42},
				bson.E{Key: "baz", Value: 7},
				bson.E{Key: "quz", Value: 42},
			},
		},
	}), doc)

	// replace tree

	err = Put(doc, "foo.bar", 42, false)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": 42,
		},
	}), doc)

	// invalid type error

	err = Put(doc, "foo.bar.baz", 42, false)
	assert.Error(t, err)
	assert.Equal(t, "cannot put value at bar.baz", err.Error())
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": 42,
		},
	}), doc)

	// intermediary object creation

	doc = &bson.D{}
	err = Put(doc, "baz.bar.foo", 42, false)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"baz": bson.M{
			"bar": bson.M{
				"foo": 42,
			},
		},
	}), doc)
}

func TestUnset(t *testing.T) {
	doc := Convert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 42,
			},
		},
	})

	// leaf field
	err := Unset(doc, "foo.bar.baz.quz")
	assert.Error(t, err)
	assert.Equal(t, "cannot unset field in 42", err.Error())
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 42,
			},
		},
	}), doc)

	// leaf field
	err = Unset(doc, "foo.bar.baz")
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": bson.M{},
		},
	}), doc)

	// missing field
	err = Unset(doc, "foo.bar.baz")
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": bson.M{},
		},
	}), doc)

	// top level field
	err = Unset(doc, "foo")
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{}), doc)
}

func TestIncrement(t *testing.T) {
	doc := Convert(bson.M{
		"foo": int64(42),
		"bar": "42",
	})

	// invalid field
	err := Increment(doc, "bar", int64(2))
	assert.Error(t, err)
	assert.Equal(t, `incrementee "bar" is not a number`, err.Error())

	// invalid increment
	err = Increment(doc, "foo", "2")
	assert.Error(t, err)
	assert.Equal(t, "increment is not a number", err.Error())

	// increment existing field
	err = Increment(doc, "foo", int64(2))
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": int64(44),
		"bar": "42",
	}), doc)

	// increment missing field
	err = Increment(doc, "quz", int64(2))
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": int64(44),
		"bar": "42",
		"quz": int64(2),
	}), doc)
}

func TestMultiply(t *testing.T) {
	doc := Convert(bson.M{
		"foo": int64(42),
		"bar": "42",
	})

	// invalid field
	err := Multiply(doc, "bar", int64(2))
	assert.Error(t, err)
	assert.Equal(t, `multiplicand "bar" is not a number`, err.Error())

	// invalid multiplicand
	err = Multiply(doc, "foo", 2)
	assert.Error(t, err)
	assert.Equal(t, "multiplier is not a number", err.Error())

	// multiply existing field
	err = Multiply(doc, "foo", int64(2))
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": int64(84),
		"bar": "42",
	}), doc)

	// multiply missing field
	err = Multiply(doc, "quz", int64(2))
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": int64(84),
		"bar": "42",
		"quz": int64(0),
	}), doc)
}
