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
	txn := NewTransaction(NewCatalog())

	/* prepare */

	id1 := primitive.NewObjectID()
	_, err := txn.Insert(Handle{"foo", "bar"}, bsonkit.List{
		bsonkit.MustConvert(bson.M{
			"_id": id1,
			"foo": "bar",
		}),
	}, true)
	assert.NoError(t, err)

	_, err = txn.Update(Handle{"foo", "bar"}, bsonkit.MustConvert(bson.M{
		"_id": id1,
	}), nil, bsonkit.MustConvert(bson.M{
		"$set": bson.M{
			"foo": "baz",
		},
	}), 0, 0, false, nil)
	assert.NoError(t, err)

	_, err = txn.Delete(Handle{"foo", "bar"}, bsonkit.MustConvert(bson.M{
		"_id": id1,
	}), nil, 0, 0)
	assert.NoError(t, err)

	assert.Len(t, txn.Catalog().Namespaces[Oplog].Documents.List, 3)

	insert := txn.Catalog().Namespaces[Oplog].Documents.List[0]
	update := txn.Catalog().Namespaces[Oplog].Documents.List[1]
	delete := txn.Catalog().Namespaces[Oplog].Documents.List[2]

	/* clean */

	txn.Clean(3, 0, 0, time.Hour)
	assert.Equal(t, bsonkit.List{insert, update, delete}, txn.Catalog().Namespaces[Oplog].Documents.List)

	txn.Clean(2, 0, 0, time.Hour)
	assert.Equal(t, bsonkit.List{update, delete}, txn.Catalog().Namespaces[Oplog].Documents.List)

	txn.Clean(0, 1, 0, time.Hour)
	assert.Equal(t, bsonkit.List{delete}, txn.Catalog().Namespaces[Oplog].Documents.List)

	txn.Clean(0, 0, 0, time.Hour)
	assert.Empty(t, txn.Catalog().Namespaces[Oplog].Documents.List)
}

func TestTransactionOplogCleaningByTime(t *testing.T) {
	txn := NewTransaction(NewCatalog())

	/* prepare */

	id1 := primitive.NewObjectID()
	_, err := txn.Insert(Handle{"foo", "bar"}, bsonkit.List{
		bsonkit.MustConvert(bson.M{
			"_id": id1,
			"foo": "bar",
		}),
	}, true)
	assert.NoError(t, err)

	time.Sleep(time.Second)

	_, err = txn.Update(Handle{"foo", "bar"}, bsonkit.MustConvert(bson.M{
		"_id": id1,
	}), nil, bsonkit.MustConvert(bson.M{
		"$set": bson.M{
			"foo": "baz",
		},
	}), 0, 0, false, nil)
	assert.NoError(t, err)

	time.Sleep(time.Second)

	_, err = txn.Delete(Handle{"foo", "bar"}, bsonkit.MustConvert(bson.M{
		"_id": id1,
	}), nil, 0, 0)
	assert.NoError(t, err)

	time.Sleep(time.Second)

	assert.Len(t, txn.Catalog().Namespaces[Oplog].Documents.List, 3)

	insert := txn.Catalog().Namespaces[Oplog].Documents.List[0]
	update := txn.Catalog().Namespaces[Oplog].Documents.List[1]
	delete := txn.Catalog().Namespaces[Oplog].Documents.List[2]

	/* clean */

	txn.Clean(0, 100, 3*time.Second, 0)
	assert.Equal(t, bsonkit.List{insert, update, delete}, txn.Catalog().Namespaces[Oplog].Documents.List)

	txn.Clean(0, 100, 2*time.Second, 0)
	assert.Equal(t, bsonkit.List{update, delete}, txn.Catalog().Namespaces[Oplog].Documents.List)

	txn.Clean(0, 100, 0, 2*time.Second)
	assert.Equal(t, bsonkit.List{delete}, txn.Catalog().Namespaces[Oplog].Documents.List)

	txn.Clean(0, 100, 0, 0)
	assert.Empty(t, txn.Catalog().Namespaces[Oplog].Documents.List)

}
