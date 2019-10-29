package lungo

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
)

func TestFileStore(t *testing.T) {
	_ = os.Remove("./test.bson")

	store := NewFileStore("./test.bson", 0666)

	engine, err := CreateEngine(Options{Store: store})
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	handle := Handle{"foo", "bar"}

	id1 := primitive.NewObjectID()
	id2 := primitive.NewObjectID()

	res, err := engine.Insert(handle, bsonkit.List{
		bsonkit.Convert(bson.M{
			"_id": id1,
			"foo": "bar",
		}),
		bsonkit.Convert(bson.M{
			"_id": id2,
			"bar": "baz",
		}),
	}, false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(res.Modified))

	name, err := engine.CreateIndex(handle, bsonkit.Convert(bson.M{
		"foo": int32(-1),
	}), "idx", false, nil)
	assert.NoError(t, err)
	assert.Equal(t, "idx", name)

	engine.Close()

	bytes, err := ioutil.ReadFile("./test.bson")
	assert.NoError(t, err)

	var out bson.M
	err = bson.Unmarshal(bytes, &out)
	assert.NoError(t, err)
	assert.Equal(t, bson.M{
		"namespaces": bson.M{
			"foo.bar": bson.M{
				"documents": bson.A{
					bson.M{
						"_id": id1,
						"foo": "bar",
					},
					bson.M{
						"_id": id2,
						"bar": "baz",
					},
				},
				"indexes": bson.M{
					"_id_": bson.M{
						"key": bson.M{
							"_id": int32(1),
						},
						"unique": true,
						"partial": nil,
					},
					"idx": bson.M{
						"key": bson.M{
							"foo": int32(-1),
						},
						"unique": false,
						"partial": nil,
					},
				},
			},
		},
	}, out)

	engine, err = CreateEngine(Options{Store: store})
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	res, err = engine.Find(handle, bsonkit.Convert(bson.M{}), nil, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, bsonkit.List{
		bsonkit.Convert(bson.M{
			"_id": id1,
			"foo": "bar",
		}),
		bsonkit.Convert(bson.M{
			"_id": id2,
			"bar": "baz",
		}),
	}, res.Matched)

	databases, err := engine.ListDatabases(bsonkit.Convert(bson.M{}))
	assert.NoError(t, err)
	assert.Equal(t, bson.A{
		"foo",
	}, bsonkit.Pick(databases, "name", false))

	collections, err := engine.ListCollections("foo", bsonkit.Convert(bson.M{}))
	assert.NoError(t, err)
	assert.Equal(t, bson.A{
		"bar",
	}, bsonkit.Pick(collections, "name", false))

	indexes, err := engine.ListIndexes(handle)
	assert.NoError(t, err)
	assert.Equal(t, bson.A{
		"_id_",
		"idx",
	}, bsonkit.Pick(indexes, "name", false))

	engine.Close()
}
