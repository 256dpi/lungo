package lungo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
)

func TestTransactionOplogCleaning(t *testing.T) {
	bsonkit.ResetCounter()

	catalog := NewCatalog()

	txn := NewTransaction(catalog)

	id1 := primitive.NewObjectID()
	_, err := txn.Insert(Handle{"foo", "bar"}, bsonkit.List{
		bsonkit.Convert(bson.M{
			"_id": id1,
			"foo": "bar",
		}),
	}, true)
	assert.NoError(t, err)

	err = txn.Clean(1)
	assert.NoError(t, err)

	catalog = txn.Catalog()
	assert.Equal(t, bsonkit.List{
		bsonkit.Convert(bson.M{
			"_id": bson.M{
				"ts": primitive.Timestamp{I: 1},
			},
			"clusterTime": primitive.Timestamp{I: 1},
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

	_, err = txn.Update(Handle{"foo", "bar"}, bsonkit.Convert(bson.M{
		"_id": id1,
	}), nil, bsonkit.Convert(bson.M{
		"$set": bson.M{
			"foo": "baz",
		},
	}), 0, false)
	assert.NoError(t, err)

	err = txn.Clean(1)
	assert.NoError(t, err)

	catalog = txn.Catalog()
	assert.Equal(t, bsonkit.List{
		bsonkit.Convert(bson.M{
			"_id": bson.M{
				"ts": primitive.Timestamp{I: 2},
			},
			"clusterTime": primitive.Timestamp{I: 2},
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
