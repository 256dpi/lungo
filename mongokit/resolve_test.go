package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

func resolveTest(t *testing.T, path string, query, doc bsonkit.Doc, arrayFilters bsonkit.List, expectedPaths []string) {
	paths := make([]string, 0)
	err := Resolve(path, query, doc, arrayFilters, func(path string) error {
		paths = append(paths, path)
		return nil
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, expectedPaths, paths)
}

func TestResolve(t *testing.T) {
	// no operators
	resolveTest(t, "foo", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo",
	})
	resolveTest(t, "foo.bar.baz", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo.bar.baz",
	})

	// no operators but index
	resolveTest(t, "foo.0", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo.0",
	})
	resolveTest(t, "foo.2.bar.7.baz", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo.2.bar.7.baz",
	})

	// root operator
	err := Resolve("$[]", nil, &bson.D{}, nil, nil)
	assert.Error(t, err)

	// single operator
	resolveTest(t, "foo.$[]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{1, 2, 3},
	}), bsonkit.List{}, []string{
		"foo.0",
		"foo.1",
		"foo.2",
	})

	// nested operators
	resolveTest(t, "foo.$[].bar.$[]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.M{
				"bar": bson.A{1, 2},
			},
			bson.M{
				"bar": bson.A{3},
			},
		},
	}), bsonkit.List{}, []string{
		"foo.0.bar.0",
		"foo.0.bar.1",
		"foo.1.bar.0",
	})

	// adjacent operators
	resolveTest(t, "foo.$[].$[]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.A{1, 2},
			bson.A{3},
		},
	}), bsonkit.List{}, []string{
		"foo.0.0",
		"foo.0.1",
		"foo.1.0",
	})

	// trailing field
	resolveTest(t, "foo.$[].$[].bar", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.A{
				bson.M{
					"bar": 1,
				},
				bson.M{
					"bar": 2,
				},
			},
			bson.A{
				bson.M{
					"bar": 3,
				},
			},
		},
	}), bsonkit.List{}, []string{
		"foo.0.0.bar",
		"foo.0.1.bar",
		"foo.1.0.bar",
	})

	// trailing index
	resolveTest(t, "foo.$[].0", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.A{1, 2},
			bson.A{1},
		},
	}), bsonkit.List{}, []string{
		"foo.0.0",
		"foo.1.0",
	})
}

func TestResolveArrayFilters(t *testing.T) {
	// single expression
	resolveTest(t, "foo.$[af1]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			"bar",
			"baz",
			"quz",
		},
	}), bsonkit.ConvertList([]bson.M{
		{
			"af1": bson.M{
				"$ne": "quz",
			},
		},
	}), []string{
		"foo.0",
		"foo.1",
	})

	// multiple expressions
	resolveTest(t, "foo.$[af1].$[af2]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.A{-10, 20, 30, -40, 4},
			bson.A{10, -20, -30, 40},
		},
	}), bsonkit.ConvertList([]bson.M{
		{
			"af1": bson.M{
				"$size": 5,
			},
		},
		{
			"af2": bson.M{
				"$lt": 0,
			},
		},
	}), []string{
		"foo.0.0",
		"foo.0.3",
	})

	// complex expressions
	resolveTest(t, "foo.$[af1].bar.$[af2]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.M{
				"ok":  true,
				"val": 20,
				"bar": bson.A{
					"foo",
					"bar",
				},
			},
			bson.M{
				"ok":  false,
				"val": 100,
				"bar": bson.A{
					"foo",
					"bar",
				},
			},
			bson.M{
				"ok":  true,
				"val": 120,
				"bar": bson.A{
					"foo",
					"bar",
				},
			},
			bson.M{
				"ok":  false,
				"val": 20,
				"bar": bson.A{
					"foo",
					"bar",
				},
			},
		},
	}), bsonkit.ConvertList([]bson.M{
		{
			"af1.ok": true,
			"af1.val": bson.M{
				"$gt": 50,
			},
		},
		{
			"af2": "foo",
		},
	}), []string{
		"foo.2.bar.0",
	})
}
