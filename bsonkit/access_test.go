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

func TestSet(t *testing.T) {
	doc := Convert(bson.M{
		"foo": "bar",
	})

	// replace final value

	err := Set(doc, "foo", "baz", false)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": "baz",
	}), doc)

	// append field
	err = Set(doc, "bar", "baz", false)
	assert.NoError(t, err)
	assert.Equal(t, &bson.D{
		bson.E{Key: "foo", Value: "baz"},
		bson.E{Key: "bar", Value: "baz"},
	}, doc)

	// prepend field
	err = Set(doc, "baz", "quz", true)
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

	err = Set(doc, "foo.bar.baz", 7, false)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": bson.M{
				"baz": 7,
			},
		},
	}), doc)

	// append nested field

	err = Set(doc, "foo.bar.quz", 42, false)
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

	err = Set(doc, "foo.bar.qux", 42, true)
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

	err = Set(doc, "foo.bar", 42, false)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": 42,
		},
	}), doc)

	// invalid type error

	err = Set(doc, "foo.bar.baz", 42, false)
	assert.Error(t, err)
	assert.Equal(t, "set: cannot add field to 42", err.Error())
	assert.Equal(t, Convert(bson.M{
		"foo": bson.M{
			"bar": 42,
		},
	}), doc)

	// intermediary object creation

	doc = &bson.D{}
	err = Set(doc, "baz.bar.foo", 42, false)
	assert.NoError(t, err)
	assert.Equal(t, Convert(bson.M{
		"baz": bson.M{
			"bar": bson.M{
				"foo": 42,
			},
		},
	}), doc)
}
