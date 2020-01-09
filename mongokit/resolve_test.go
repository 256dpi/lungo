package mongokit

import (
	"testing"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func ResolveTest(t *testing.T, path string, query, doc bsonkit.Doc, arrayFilters bsonkit.List, expectedPaths []string) {
	paths := make([]string, 0)
	err := Resolve(path, query, doc, arrayFilters, func(path string) error {
		paths = append(paths, path)
		return nil
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, expectedPaths, paths)
}

func TestDividePathStaticDynamicPart(t *testing.T) {
	static, dynamic := dividePathStaticDynamicPart("foo.bar.$[baz].boo")
	assert.Equal(t, static, "foo.bar")
	assert.Equal(t, dynamic, "$[baz].boo")

	static, dynamic = dividePathStaticDynamicPart("foo.bar.boo")
	assert.Equal(t, static, "foo.bar.boo")
	assert.Equal(t, dynamic, pathEnd)

	static, dynamic = dividePathStaticDynamicPart("$[].foo.bar.boo")
	assert.Equal(t, static, pathEnd)
	assert.Equal(t, dynamic, "$[].foo.bar.boo")

	static, dynamic = dividePathStaticDynamicPart("$")
	assert.Equal(t, static, pathEnd)
	assert.Equal(t, dynamic, "$")
}

func TestResolveSimplePath(t *testing.T) {
	ResolveTest(t, "foo", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo",
	})

	ResolveTest(t, "foo.bar.baz", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo.bar.baz",
	})
}

func TestResolveArrayIndexPath(t *testing.T) {
	ResolveTest(t, "foo.0", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo.0",
	})

	ResolveTest(t, "foo.2.bar.7.baz", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{}), bsonkit.List{}, []string{
		"foo.2.bar.7.baz",
	})
}

func TestResolveArrayPath(t *testing.T) {
	ResolveTest(t, "foo.$[]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
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

	ResolveTest(t, "foo.$[].bar.$[]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
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

	ResolveTest(t, "foo.$[].$[]", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
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

	ResolveTest(t, "foo.$[].$[].bar", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
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

	ResolveTest(t, "foo.$[].0", bsonkit.Convert(bson.M{}), bsonkit.Convert(bson.M{
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
