package lungo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestIndexViewCreateMany(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		ns := c.Database().Name() + "." + c.Name()

		// invalid index
		names, err := c.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
			{
				Keys: bson.M{
					"bar": false,
				},
			},
		})
		assert.Error(t, err)
		assert.Nil(t, names)

		// compound and partial index
		names, err = c.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
			{
				Keys: bson.D{
					bson.E{Key: "bar", Value: -1},
					bson.E{Key: "baz", Value: 1},
				},
			},
			{
				Keys: bson.M{
					"foo": 1,
				},
				Options: options.Index().SetName("foo").SetUnique(true).SetPartialFilterExpression(bson.M{
					"bar": "baz",
				}),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{
			"bar_-1_baz_1",
			"foo",
		}, names)

		time.Sleep(50 * time.Millisecond)

		// list
		csr, err := c.Indexes().List(nil)
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"key": bson.M{
					"_id": int32(1),
				},
				"name": "_id_",
				"ns":   ns,
				"v":    int32(2),
			},
			{
				"key": bson.M{
					"bar": int32(-1),
					"baz": int32(1),
				},
				"name": "bar_-1_baz_1",
				"ns":   ns,
				"v":    int32(2),
			},
			{
				"key": bson.M{
					"foo": int32(1),
				},
				"name":   "foo",
				"ns":     ns,
				"unique": true,
				"partialFilterExpression": bson.M{
					"bar": "baz",
				},
				"v": int32(2),
			},
		}, readAll(csr))

		// ensure indexes
		names, err = c.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
			{
				Keys: bson.D{
					bson.E{Key: "bar", Value: -1},
					bson.E{Key: "baz", Value: 1},
				},
			},
			{
				Keys: bson.M{
					"foo": 1,
				},
				Options: options.Index().SetName("foo").SetUnique(true).SetPartialFilterExpression(bson.M{
					"bar": "baz",
				}),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{
			"bar_-1_baz_1",
			"foo",
		}, names)
	})
}

func TestIndexViewCreateOne(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		ns := c.Database().Name() + "." + c.Name()

		// invalid index
		name, err := c.Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys: bson.M{
				"bar": false,
			},
		})
		assert.Error(t, err)
		assert.Empty(t, name)

		// single unique index
		name, err = c.Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys: bson.M{
				"foo": 1,
			},
			Options: options.Index().SetName("foo").SetUnique(true),
		})
		assert.NoError(t, err)
		assert.Equal(t, "foo", name)

		time.Sleep(50 * time.Millisecond)

		// list
		csr, err := c.Indexes().List(nil)
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"key": bson.M{
					"_id": int32(1),
				},
				"name": "_id_",
				"ns":   ns,
				"v":    int32(2),
			},
			{
				"key": bson.M{
					"foo": int32(1),
				},
				"name":   "foo",
				"ns":     ns,
				"unique": true,
				"v":      int32(2),
			},
		}, readAll(csr))

		// ensure index
		name, err = c.Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys: bson.M{
				"foo": 1,
			},
			Options: options.Index().SetName("foo").SetUnique(true),
		})
		assert.NoError(t, err)
		assert.Equal(t, "foo", name)

		// duplicate index (same key)
		name, err = c.Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys: bson.M{
				"foo": 1,
			},
		})
		assert.Error(t, err)
		assert.Equal(t, "", name)
	})
}

func TestIndexViewDropAll(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		ns := c.Database().Name() + "." + c.Name()

		// unique and normal index
		names, err := c.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
			{
				Keys: bson.D{
					bson.E{Key: "bar", Value: -1},
					bson.E{Key: "baz", Value: 1},
				},
			},
			{
				Keys: bson.M{
					"foo": 1,
				},
				Options: options.Index().SetName("foo").SetUnique(true),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{
			"bar_-1_baz_1",
			"foo",
		}, names)

		time.Sleep(50 * time.Millisecond)

		// list
		csr, err := c.Indexes().List(nil)
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"key": bson.M{
					"_id": int32(1),
				},
				"name": "_id_",
				"ns":   ns,
				"v":    int32(2),
			},
			{
				"key": bson.M{
					"bar": int32(-1),
					"baz": int32(1),
				},
				"name": "bar_-1_baz_1",
				"ns":   ns,
				"v":    int32(2),
			},
			{
				"key": bson.M{
					"foo": int32(1),
				},
				"name":   "foo",
				"ns":     ns,
				"unique": true,
				"v":      int32(2),
			},
		}, readAll(csr))

		// drop
		_, err = c.Indexes().DropAll(context.Background())
		assert.NoError(t, err)

		// list
		csr, err = c.Indexes().List(nil)
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"key": bson.M{
					"_id": int32(1),
				},
				"name": "_id_",
				"ns":   ns,
				"v":    int32(2),
			},
		}, readAll(csr))
	})
}

func TestIndexViewDropOne(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		ns := c.Database().Name() + "." + c.Name()

		// unique and normal index
		name, err := c.Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys: bson.M{
				"foo": 1,
			},
			Options: options.Index().SetName("foo").SetUnique(true),
		})
		assert.NoError(t, err)
		assert.Equal(t, "foo", name)

		time.Sleep(50 * time.Millisecond)

		// list
		csr, err := c.Indexes().List(nil)
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"key": bson.M{
					"_id": int32(1),
				},
				"name": "_id_",
				"ns":   ns,
				"v":    int32(2),
			},
			{
				"key": bson.M{
					"foo": int32(1),
				},
				"name":   "foo",
				"ns":     ns,
				"unique": true,
				"v":      int32(2),
			},
		}, readAll(csr))

		// drop
		_, err = c.Indexes().DropOne(context.Background(), "foo")
		assert.NoError(t, err)

		// list
		csr, err = c.Indexes().List(nil)
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"key": bson.M{
					"_id": int32(1),
				},
				"name": "_id_",
				"ns":   ns,
				"v":    int32(2),
			},
		}, readAll(csr))
	})
}

func TestIndexViewList(t *testing.T) {
	// tested in above tests
}

func TestIndexExpiry(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		ns := c.Database().Name() + "." + c.Name()

		// invalid index
		name, err := c.Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys: bson.M{
				"foo": 1,
				"bar": -1,
			},
			Options: options.Index().SetExpireAfterSeconds(0),
		})
		assert.Error(t, err)
		assert.Empty(t, name)

		// ttl index
		name, err = c.Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys: bson.M{
				"foo": 1,
			},
			Options: options.Index().SetExpireAfterSeconds(0),
		})
		assert.NoError(t, err)
		assert.Equal(t, "foo_1", name)

		time.Sleep(50 * time.Millisecond)

		// list
		csr, err := c.Indexes().List(nil)
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"key": bson.M{
					"_id": int32(1),
				},
				"name": "_id_",
				"ns":   ns,
				"v":    int32(2),
			},
			{
				"key": bson.M{
					"foo": int32(1),
				},
				"name":               "foo_1",
				"ns":                 ns,
				"expireAfterSeconds": int32(0),
				"v":                  int32(2),
			},
		}, readAll(csr))

		// skip mongo test
		if _, ok := c.(*MongoCollection); ok {
			return
		}

		// add documents
		now := time.Now()
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()
		_, err = c.InsertMany(nil, bson.A{
			bson.M{
				"foo": now,
			},
			bson.M{
				"_id": id1,
				"foo": now.Add(time.Second),
			},
			bson.M{
				"foo": bson.A{
					now,
					now.Add(time.Second),
				},
			},
			bson.M{
				"_id": id2,
				"foo": 0,
			},
		})
		assert.NoError(t, err)

		time.Sleep(150 * time.Millisecond)

		// check expiry
		csr, err = c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": primitive.NewDateTimeFromTime(now.Add(time.Second)),
			},
			{
				"_id": id2,
				"foo": int32(0),
			},
		}, readAll(csr))

		time.Sleep(time.Second)

		// check expiry
		csr, err = c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id2,
				"foo": int32(0),
			},
		}, readAll(csr))
	})
}
