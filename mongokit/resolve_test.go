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

func TestDividePathStaticDynamicPart(t *testing.T) {
	static, dynamic := dividePathStaticDynamicPart("foo.bar.$[baz].boo")
	assert.Equal(t, static, "foo.bar")
	assert.Equal(t, dynamic, "$[baz].boo")

	static, dynamic = dividePathStaticDynamicPart("foo.bar.boo")
	assert.Equal(t, static, "foo.bar.boo")
	assert.Equal(t, dynamic, bsonkit.PathEnd)

	static, dynamic = dividePathStaticDynamicPart("$[].foo.bar.boo")
	assert.Equal(t, static, bsonkit.PathEnd)
	assert.Equal(t, dynamic, "$[].foo.bar.boo")

	static, dynamic = dividePathStaticDynamicPart("$")
	assert.Equal(t, static, bsonkit.PathEnd)
	assert.Equal(t, dynamic, "$")
}

func TestResolveSimplePath(t *testing.T) {
	resolveTest(t, "foo", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo",
	})

	resolveTest(t, "foo.bar.baz", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo.bar.baz",
	})
}

func TestResolveArrayIndexPath(t *testing.T) {
	resolveTest(t, "foo.0", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo.0",
	})

	resolveTest(t, "foo.2.bar.7.baz", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo.2.bar.7.baz",
	})
}

func TestResolveArrayPath(t *testing.T) {
	resolveTest(t, "foo.$[]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			"bar",
			"baz",
			"fooz",
		},
	}), bsonkit.List{}, []string{
		"foo.0",
		"foo.1",
		"foo.2",
	})

	resolveTest(t, "foo.$[].bar.$[]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.M{
				"bar": bson.A{
					"foobar",
					"barfoo",
				},
			},
			bson.M{
				"bar": bson.A{
					"foobar",
				},
			},
		},
	}), bsonkit.List{}, []string{
		"foo.0.bar.0",
		"foo.0.bar.1",
		"foo.1.bar.0",
	})

	resolveTest(t, "foo.$[].$[]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.A{
				"foobar",
				"barfoo",
			},
			bson.A{
				"foobar",
			},
		},
	}), bsonkit.List{}, []string{
		"foo.0.0",
		"foo.0.1",
		"foo.1.0",
	})

	resolveTest(t, "foo.$[].$[].bar", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.A{
				bson.M{
					"bar": "foobar",
				},
				bson.M{
					"bar": "barfoo",
				},
			},
			bson.A{
				bson.M{
					"bar": "foobar",
				},
			},
		},
	}), bsonkit.List{}, []string{
		"foo.0.0.bar",
		"foo.0.1.bar",
		"foo.1.0.bar",
	})

	resolveTest(t, "foo.$[].0", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.A{
				"foobar",
				"barfoo",
			},
			bson.A{
				"foobar",
			},
		},
	}), bsonkit.List{}, []string{
		"foo.0.0",
		"foo.1.0",
	})
}

func TestResolveArrayFilters(t *testing.T) {
	resolveTest(t, "foo.$[notfooz]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			"bar",
			"baz",
			"fooz",
		},
	}), bsonkit.ConvertList([]bson.M{
		{
			"notfooz": bson.M{
				"$ne": "fooz",
			},
		},
	}), []string{
		"foo.0",
		"foo.1",
	})

	resolveTest(t, "foo.$[valid].bar.$[foobar]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.M{
				"ok":  true,
				"val": 20,
				"bar": bson.A{
					"foobar",
					"barfoo",
				},
			},
			bson.M{
				"ok":  false,
				"val": 100,
				"bar": bson.A{
					"foobar",
					"barfoo",
				},
			},
			bson.M{
				"ok":  true,
				"val": 120,
				"bar": bson.A{
					"foobar",
					"barfoo",
				},
			},
			bson.M{
				"ok":  false,
				"val": 20,
				"bar": bson.A{
					"foobar",
					"barfoo",
				},
			},
		},
	}), bsonkit.ConvertList([]bson.M{
		{
			"valid.ok": true,
			"valid.val": bson.M{
				"$gt": 50,
			},
		},
		{
			"foobar": "foobar",
		},
	}), []string{
		"foo.2.bar.0",
	})

	resolveTest(t, "foo.$[ok].$[ok2]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
		"foo": bson.A{
			bson.A{
				-10,
				20,
				30,
				-40,
				4,
			},
			bson.A{
				10,
				-20,
				-30,
				40,
			},
		},
	}), bsonkit.ConvertList([]bson.M{
		{
			"ok": bson.M{
				"$size": 5,
			},
		},
		{
			"ok2": bson.M{
				"$lt": 0,
			},
		},
	}), []string{
		"foo.0.0",
		"foo.0.3",
	})
}
