package lungo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestCollectionFind(t *testing.T) {
	/* missing database */

	collectionTest(t, func(c ICollection) {
		c = c.Database().Client().Database("not-existing").Collection("not-existing")
		csr, err := c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.NotNil(t, csr)
		assert.Equal(t, []bson.M{}, readAll(csr))
	})

	/* missing collection */

	collectionTest(t, func(c ICollection) {
		csr, err := c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.NotNil(t, csr)
		assert.Equal(t, []bson.M{}, readAll(csr))
	})
}

func TestCollectionInsertOne(t *testing.T) {
	/* generated id */

	collectionTest(t, func(c ICollection) {
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

	collectionTest(t, func(c ICollection) {
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

	collectionTest(t, func(c ICollection) {
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
	})
}
