package lungo

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/mongokit"
)

func TestFileStore(t *testing.T) {
	_ = os.Remove("./test.bson")

	store := NewFileStore("./test.bson", 0666)

	engine, err := CreateEngine(Options{Store: store})
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	get := func(i int, name string) primitive.Timestamp {
		return bsonkit.Get(engine.Catalog().Namespaces[Oplog].Documents.List[i], name).(primitive.Timestamp)
	}

	handle := Handle{"foo", "bar"}

	id1 := primitive.NewObjectID()
	id2 := primitive.NewObjectID()

	txn, err := engine.Begin(nil, true)
	assert.NoError(t, err)

	res, err := txn.Insert(handle, bsonkit.List{
		bsonkit.MustConvert(bson.M{
			"_id": id1,
			"foo": "bar",
		}),
		bsonkit.MustConvert(bson.M{
			"_id": id2,
			"bar": "baz",
		}),
	}, false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(res.Modified))

	name, err := txn.CreateIndex(handle, "idx", mongokit.IndexConfig{
		Key: bsonkit.MustConvert(bson.M{
			"foo": int32(-1),
		}),
	})
	assert.NoError(t, err)
	assert.Equal(t, "idx", name)

	err = engine.Commit(txn)
	assert.NoError(t, err)

	engine.Close()

	bytes, err := os.ReadFile("./test.bson")
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
						"unique":  true,
						"partial": nil,
						"expiry":  int64(0),
					},
					"idx": bson.M{
						"key": bson.M{
							"foo": int32(-1),
						},
						"unique":  false,
						"partial": nil,
						"expiry":  int64(0),
					},
				},
			},
			"local.oplog": bson.M{
				"documents": bson.A{
					bson.M{
						"_id": bson.M{
							"ts": get(0, "_id.ts"),
						},
						"clusterTime": get(0, "clusterTime"),
						"documentKey": bson.M{
							"_id": id1,
						},
						"fullDocument": bson.M{
							"_id": id1,
							"foo": "bar",
						},
						"ns": bson.M{
							"db":   "foo",
							"coll": "bar",
						},
						"operationType": "insert",
					},
					bson.M{
						"_id": bson.M{
							"ts": get(1, "_id.ts"),
						},
						"clusterTime": get(1, "clusterTime"),
						"documentKey": bson.M{
							"_id": id2,
						},
						"fullDocument": bson.M{
							"_id": id2,
							"bar": "baz",
						},
						"ns": bson.M{
							"db":   "foo",
							"coll": "bar",
						},
						"operationType": "insert",
					},
				},
				"indexes": bson.M{},
			},
		},
	}, out)

	engine, err = CreateEngine(Options{Store: store})
	assert.NoError(t, err)
	assert.NotNil(t, engine)

	txn, err = engine.Begin(nil, false)
	assert.NoError(t, err)

	res, err = txn.Find(handle, bsonkit.MustConvert(bson.M{}), nil, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, bsonkit.List{
		bsonkit.MustConvert(bson.M{
			"_id": id1,
			"foo": "bar",
		}),
		bsonkit.MustConvert(bson.M{
			"_id": id2,
			"bar": "baz",
		}),
	}, res.Matched)

	databases, err := txn.ListDatabases(bsonkit.MustConvert(bson.M{}))
	bsonkit.Sort(databases, []bsonkit.Column{{Path: "name"}}, true)
	assert.NoError(t, err)
	assert.Equal(t, bson.A{
		"foo",
		"local",
	}, bsonkit.Pick(databases, "name", false))

	collections, err := txn.ListCollections(Handle{"foo"}, bsonkit.MustConvert(bson.M{}))
	assert.NoError(t, err)
	assert.Equal(t, bson.A{
		"bar",
	}, bsonkit.Pick(collections, "name", false))

	indexes, err := txn.ListIndexes(handle)
	assert.NoError(t, err)
	assert.Equal(t, bson.A{
		"_id_",
		"idx",
	}, bsonkit.Pick(indexes, "name", false))

	engine.Close()
}
