package lungo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
)

func TestTransactionOplogCleaningBySize(t *testing.T) {
	catalog := NewCatalog()

	get := func(i int, name string) primitive.Timestamp {
		return bsonkit.Get(catalog.Namespaces[Oplog].Documents.List[i], name).(primitive.Timestamp)
	}

	txn := NewTransaction(catalog)

	id1 := primitive.NewObjectID()
	_, err := txn.Insert(Handle{"foo", "bar"}, bsonkit.List{
		bsonkit.MustConvert(bson.M{
			"_id": id1,
			"foo": "bar",
		}),
	}, true)
	assert.NoError(t, err)

	txn.Clean(1, time.Hour)

	catalog = txn.Catalog()
	assert.Equal(t, bsonkit.List{
		bsonkit.MustConvert(bson.M{
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
		}),
	}, catalog.Namespaces[Oplog].Documents.List)

	txn = NewTransaction(catalog)

	_, err = txn.Update(Handle{"foo", "bar"}, bsonkit.MustConvert(bson.M{
		"_id": id1,
	}), nil, bsonkit.MustConvert(bson.M{
		"$set": bson.M{
			"foo": "baz",
		},
	}), 0, 0, false, nil)
	assert.NoError(t, err)

	txn.Clean(1, time.Hour)

	catalog = txn.Catalog()
	assert.Equal(t, bsonkit.List{
		bsonkit.MustConvert(bson.M{
			"_id": bson.M{
				"ts": get(0, "_id.ts"),
			},
			"clusterTime": get(0, "clusterTime"),
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": "baz",
			},
			"ns": bson.M{
				"db":   "foo",
				"coll": "bar",
			},
			"operationType": "update",
			"updateDescription": bson.M{
				"updatedFields": bson.M{
					"foo": "baz",
				},
				"removedFields": bson.A{},
			},
		}),
	}, catalog.Namespaces[Oplog].Documents.List)
}

func TestTransactionOplogCleaningByTime(t *testing.T) {
	catalog := NewCatalog()

	get := func(i int, name string) primitive.Timestamp {
		return bsonkit.Get(catalog.Namespaces[Oplog].Documents.List[i], name).(primitive.Timestamp)
	}

	txn := NewTransaction(catalog)

	id1 := primitive.NewObjectID()
	_, err := txn.Insert(Handle{"foo", "bar"}, bsonkit.List{
		bsonkit.MustConvert(bson.M{
			"_id": id1,
			"foo": "bar",
		}),
	}, true)
	assert.NoError(t, err)

	txn.Clean(10, time.Second)

	// wait at least 2s to ensure the clean threshold is always smaller than the
	// timestamp. if we only wait a second the ordinal number might cause the
	// $lte to not include the event
	time.Sleep(2200 * time.Millisecond)

	catalog = txn.Catalog()
	assert.Equal(t, bsonkit.List{
		bsonkit.MustConvert(bson.M{
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
		}),
	}, catalog.Namespaces[Oplog].Documents.List)

	txn = NewTransaction(catalog)

	_, err = txn.Update(Handle{"foo", "bar"}, bsonkit.MustConvert(bson.M{
		"_id": id1,
	}), nil, bsonkit.MustConvert(bson.M{
		"$set": bson.M{
			"foo": "baz",
		},
	}), 0, 0, false, nil)
	assert.NoError(t, err)

	txn.Clean(10, time.Second)

	catalog = txn.Catalog()
	assert.Equal(t, bsonkit.List{
		bsonkit.MustConvert(bson.M{
			"_id": bson.M{
				"ts": get(0, "_id.ts"),
			},
			"clusterTime": get(0, "clusterTime"),
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": "baz",
			},
			"ns": bson.M{
				"db":   "foo",
				"coll": "bar",
			},
			"operationType": "update",
			"updateDescription": bson.M{
				"updatedFields": bson.M{
					"foo": "baz",
				},
				"removedFields": bson.A{},
			},
		}),
	}, catalog.Namespaces[Oplog].Documents.List)
}
