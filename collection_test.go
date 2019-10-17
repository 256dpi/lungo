package lungo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	})

	/* duplicate _id key */

	// TODO: Test duplicate ids in request.

	collectionTest(t, func(t *testing.T, c ICollection) {
		id := primitive.NewObjectID()

		_, err := c.InsertMany(nil, []interface{}{
			bson.M{
				"_id": id,
				"foo": "bar",
			},
		})
		assert.NoError(t, err)

		_, err = c.InsertMany(nil, []interface{}{
			bson.M{
				"_id": id,
				"foo": "baz",
			},
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
