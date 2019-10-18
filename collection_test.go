package lungo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestCollectionClone(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		c2, err := c.Clone()
		assert.NoError(t, err)
		assert.NotNil(t, c2)
	})
}

func TestCollectionDatabase(t *testing.T) {
	databaseTest(t, func(t *testing.T, d IDatabase) {
		assert.Equal(t, d, d.Collection("").Database())
	})
}

func TestCollectionDrop(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		_, err := c.InsertOne(nil, bson.M{
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"foo": "bar",
			},
		}, dumpCollection(c, true))

		err = c.Drop(nil)
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{}, dumpCollection(c, true))
	})
}

func TestCollectionDeleteMany(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, []interface{}{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, dumpCollection(c, false))

		res2, err := c.DeleteMany(nil, bson.M{
			"_id": "foo",
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), res2.DeletedCount)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, dumpCollection(c, false))

		res2, err = c.DeleteMany(nil, bson.M{
			"_id": bson.M{"$in": bson.A{id1, id2}},
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(2), res2.DeletedCount)
		assert.Equal(t, []bson.M{}, dumpCollection(c, false))
	})
}

func TestCollectionDeleteOne(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		id := primitive.NewObjectID()

		res1, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res1.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		res2, err := c.DeleteOne(nil, bson.M{
			"_id": "foo",
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), res2.DeletedCount)
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		res2, err = c.DeleteOne(nil, bson.M{
			"_id": id,
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), res2.DeletedCount)
		assert.Equal(t, []bson.M{}, dumpCollection(c, false))
	})
}

func TestCollectionFind(t *testing.T) {
	/* missing database */

	clientTest(t, func(t *testing.T, client IClient) {
		c := client.Database("not-existing").Collection("not-existing")
		csr, err := c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.NotNil(t, csr)
		assert.Equal(t, []bson.M{}, readAll(csr))
	})

	/* missing collection */

	databaseTest(t, func(t *testing.T, d IDatabase) {
		csr, err := d.Collection("not-existing").Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.NotNil(t, csr)
		assert.Equal(t, []bson.M{}, readAll(csr))
	})

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res1, err := c.InsertMany(nil, []interface{}{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res1.InsertedIDs, 2)

		// find all
		csr, err := c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, readAll(csr))

		// find, limit 1
		csr, err = c.Find(nil, bson.M{}, options.Find().SetLimit(1))
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, readAll(csr))
	})
}

func TestCollectionFindOne(t *testing.T) {
	/* missing database */

	clientTest(t, func(t *testing.T, client IClient) {
		c := client.Database("not-existing").Collection("not-existing")
		res := c.FindOne(nil, bson.M{})
		assert.Error(t, res.Err())
		assert.Equal(t, mongo.ErrNoDocuments, res.Err())
	})

	/* missing collection */

	databaseTest(t, func(t *testing.T, d IDatabase) {
		res := d.Collection("not-existing").FindOne(nil, bson.M{})
		assert.Error(t, res.Err())
		assert.Equal(t, mongo.ErrNoDocuments, res.Err())
	})

	/* fine one by id */

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		_, err := c.InsertMany(nil, []interface{}{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		})
		assert.NoError(t, err)

		var doc bson.M
		err = c.FindOne(nil, bson.M{
			"_id": id1,
		}).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id1,
			"foo": "bar",
		}, doc)
	})

	/* first from multiple results */

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		_, err := c.InsertMany(nil, []interface{}{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		})
		assert.NoError(t, err)

		var doc bson.M
		err = c.FindOne(nil, bson.M{}).Decode(&doc)
		assert.NoError(t, err)
		assert.Equal(t, bson.M{
			"_id": id1,
			"foo": "bar",
		}, doc)
	})
}

func TestCollectionInsertMany(t *testing.T) {
	/* generated id */

	collectionTest(t, func(t *testing.T, c ICollection) {
		res, err := c.InsertMany(nil, []interface{}{
			bson.M{
				"foo": "bar",
			},
			bson.M{
				"bar": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"foo": "bar",
			},
			{
				"bar": "baz",
			},
		}, dumpCollection(c, true))
	})

	/* provided _id */

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := primitive.NewObjectID()
		id2 := primitive.NewObjectID()

		res, err := c.InsertMany(nil, []interface{}{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
			bson.M{
				"_id": id2,
				"bar": "baz",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, dumpCollection(c, false))

		/* duplicate key */

		res, err = c.InsertMany(nil, []interface{}{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
		})
		assert.Error(t, err)
		// assert.Len(t, res.InsertedIDs, 2)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"bar": "baz",
			},
		}, dumpCollection(c, false))
	})

	/* complex _id */

	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.M{
			"some-id": "a",
		}

		res, err := c.InsertMany(nil, []interface{}{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
		})
		assert.NoError(t, err)
		assert.Len(t, res.InsertedIDs, 1)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, dumpCollection(c, false))

		/* duplicate key */

		res, err = c.InsertMany(nil, []interface{}{
			bson.M{
				"_id": id1,
				"foo": "bar",
			},
		})
		assert.Error(t, err)
		// assert.Nil(t, res) // TODO: mongo returns all ids in any case, bug?
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})

	// TODO: Test duplicate ids in request.
}

func TestCollectionInsertOne(t *testing.T) {
	/* generated id */

	collectionTest(t, func(t *testing.T, c ICollection) {
		res, err := c.InsertOne(nil, bson.M{
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"foo": "bar",
			},
		}, dumpCollection(c, true))
	})

	/* provided _id */

	collectionTest(t, func(t *testing.T, c ICollection) {
		id := primitive.NewObjectID()

		res, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})

	/* duplicate _id key */

	collectionTest(t, func(t *testing.T, c ICollection) {
		id := primitive.NewObjectID()

		_, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)

		_, err = c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "baz",
		})
		assert.Error(t, err)

		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})
}

func TestCollectionName(t *testing.T) {
	databaseTest(t, func(t *testing.T, d IDatabase) {
		assert.Equal(t, "foo", d.Collection("foo").Name())
	})
}
